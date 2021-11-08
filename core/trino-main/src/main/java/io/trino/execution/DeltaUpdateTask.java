/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution;

// based on CreateTableTask

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.jaxrs.testing.MockUriInfo;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.client.QueryResults;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.buffer.SerializedPage;
import io.trino.execution.warnings.WarningCollector;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.metadata.Metadata;
import io.trino.metadata.NewTableLayout;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableSchema;
import io.trino.operator.ExchangeClient;
import io.trino.operator.ExchangeClientSupplier;
import io.trino.security.AccessControl;
import io.trino.server.SessionContext;
import io.trino.server.protocol.Query;
import io.trino.server.protocol.QueryInfoUrlFactory;
import io.trino.server.protocol.Slug;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.Output;
import io.trino.sql.tree.DeltaUpdate;
import io.trino.sql.tree.Expression;
import io.trino.transaction.TransactionManager;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.MISSING_TABLE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DeltaUpdateTask
        implements DataDefinitionTask<DeltaUpdate>
{
    private final DispatchManager dispatchManager;
    private final QueryManager queryManager;
    private final QueryInfoUrlFactory queryInfoUrlFactory;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final BoundedExecutor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final BlockEncodingSerde blockEncodingSerde;

    private QualifiedTablePrefix source;
    private QualifiedTablePrefix target;
    private List<QualifiedObjectName> sourceQualifiedObjectNames;
    private List<QualifiedObjectName> targetQualifiedObjectNames;
    private Session session;
    private DeltaUpdate deltaUpdate;
    private Metadata metadata;
    private SessionContext context;

    // based on QueuedStatementResource Query
    // @GuardedBy("this")
    private ConcurrentMap<QueryId, ListenableFuture<Void>> queryFutures = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private Map<QueryId, Boolean> allDone = new HashMap<>();


    private SettableFuture<Void> future = SettableFuture.create();


    public DeltaUpdateTask(DispatchManager dispatchManager,
            QueryManager queryManager,
            QueryInfoUrlFactory queryInfoUrlFactory,
            ExchangeClientSupplier exchangeClientSupplier,
            BoundedExecutor responseExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde)
    {
        // super();
        this.dispatchManager = checkNotNull(dispatchManager, "dispatchManager is null");
        this.queryManager = queryManager;
        this.queryInfoUrlFactory = queryInfoUrlFactory;
        this.exchangeClientSupplier = exchangeClientSupplier;
        //TODO: response executor will use a thread reserved for http requests, maybe use another executor
        this.responseExecutor = responseExecutor;
        this.timeoutExecutor = timeoutExecutor;
        this.blockEncodingSerde = blockEncodingSerde;

    }

    @Override
    public String getName()
    {
        return "DELTAUPDATE";
    }

    @Override
    public String explain(DeltaUpdate statement, List<Expression> parameters)
    {
        return "DELTAUPDATE " + statement.getTarget().toString() + " FROM " + statement.getSource().toString();
    }

    @Override
    public ListenableFuture<Void> execute(
            DeltaUpdate statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        // TODO move essentially everything into a new class DeltaManger
        // We are setting properties from execute which is not very nice
        // then return a future like in dispatchManager::createQuery
        checkNotNull(stateMachine, "stateMachine is null");
        deltaUpdate = (DeltaUpdate) checkNotNull(statement, "statement is null");
        session = stateMachine.getSession();
        this.metadata = metadata;

        context = dispatchManager.queryTracker.getContext(stateMachine.getQueryId());

        // Transforming the source and target to Qualified names,
        // since the table name does not need to be defined we us QualifiedTablePrefixes
        processSourceAndTarget();
        return internalExecute(accessControl, stateMachine.getSession(), parameters, stateMachine::setOutput);
    }

    @VisibleForTesting
    ListenableFuture<Void> internalExecute(AccessControl accessControl, Session session, List<Expression> parameters, Consumer<Optional<Output>> outputConsumer)
    {
        // doing what they do in visitInsert of StatementAnalyzer
        List<TableHandle> sourceTableH;
        List<TableHandle> targetTableH;

        sourceTableH = sourceQualifiedObjectNames.stream().map(qon -> metadata.getTableHandle(session, qon))
                .filter(Optional::isPresent).map(Optional::get).collect(toImmutableList());

        if (sourceTableH.size() == 0){
            throw semanticException(MISSING_TABLE, deltaUpdate, "Deltaupdate: Source table(s) do(es) not exist");
        }

        targetTableH = targetQualifiedObjectNames.stream().map(qon -> metadata.getTableHandle(session, qon))
                .filter(Optional::isPresent).map(Optional::get).collect(toImmutableList());

        if (targetTableH.size() == 0){
            throw semanticException(MISSING_TABLE, deltaUpdate, "Deltaupdate: Target table(s) do(es) not exist");
        }

        // checking if we can insert into the source table
        // doing what they do in visitInsert
        // TODO: do some checks for update and delete
        for (QualifiedObjectName qon : targetQualifiedObjectNames) {
            // will throw an error if not
            accessControl.checkCanInsertIntoTable(session.toSecurityContext(), qon);

            if (!accessControl.getRowFilters(session.toSecurityContext(), qon).isEmpty()) {
                throw semanticException(NOT_SUPPORTED, deltaUpdate, "Insert into table with a row filter is not supported");
            }
        }

        Map<String, TableHandle> targetMap = new HashMap<>();

        for (TableHandle target : targetTableH) {
            TableSchema tableSchema = metadata.getTableSchema(session, target);
            targetMap.put(tableSchema.getTable().getTableName(), target);
        }

        for (TableHandle source : sourceTableH) {
            System.out.println("LOOP");
            // based on visitInsert of StatementAnalyzer
            TableSchema sourceSchema = metadata.getTableSchema(session, source);
            String tableName = sourceSchema.getTable().getTableName();

            if (!targetMap.containsKey(tableName)) {
                throw new TrinoException(GENERIC_USER_ERROR, format("DELTAUPDATE: target does not include all tables from source"));
            }

            TableHandle targetTableHandle = targetMap.get(tableName);
            TableSchema targetSchema = metadata.getTableSchema(session, targetTableHandle);
            ;

            // check for the columns to be matching

            List<ColumnSchema> sourceColumns = sourceSchema.getColumns().stream()
                    //.filter(column -> !column.isHidden())
                    .collect(toImmutableList());

            List<ColumnSchema> targetColumns = targetSchema.getColumns().stream()
                    //.filter(column -> !column.isHidden())
                    .collect(toImmutableList());

            for (ColumnSchema column : targetColumns) {
                if (!accessControl.getColumnMasks(session.toSecurityContext(), targetSchema.getQualifiedName(), column.getName(), column.getType()).isEmpty()) {
                    throw semanticException(NOT_SUPPORTED, deltaUpdate, "Insert into table with column masks is not supported");
                }
            }

            // TODO: figure out how partition columns are chosen
            Optional<NewTableLayout> newTableLayout = metadata.getInsertLayout(session, targetTableHandle);
            newTableLayout.ifPresent(layout -> {
                if (!ImmutableSet.copyOf(targetColumns).containsAll(layout.getPartitionColumns())) {
                    throw new TrinoException(NOT_SUPPORTED, "INSERT must write all distribution columns: " + layout.getPartitionColumns());
                }
            });

            // getting the names of both target and source columns
            List<String> sourceColumnNames = sourceColumns.stream()
                    .map(ColumnSchema::getName)
                    .collect(toImmutableList());
            List<String> targetColumnNames = targetColumns.stream()
                    .map(ColumnSchema::getName)
                    .collect(toImmutableList());

            for (String sourceColumnName : sourceColumnNames) {
                if (!targetColumnNames.contains(sourceColumnName)) {
                    throw semanticException(COLUMN_NOT_FOUND, deltaUpdate, "Insert column name does not exist in target table: %s", sourceColumnName);
                }
            }

            // check that types of columns also match:
            List<Type> sourceTableTypes = sourceColumnNames.stream()
                    .map(insertColumn -> sourceSchema.getColumn(insertColumn).getType())
                    .collect(toImmutableList());

            List<Type> targetTableTypes = targetColumnNames.stream()
                    .map(insertColumn -> targetSchema.getColumn(insertColumn).getType())
                    .collect(toImmutableList());

            if (!(targetTableTypes.equals(sourceTableTypes))) { // StatementAnalyzer::originally it was typesMatchForInsert
                throw semanticException(TYPE_MISMATCH,
                        deltaUpdate,
                        "DeltaUpdate query has mismatched column types: Target: [%s], Source: [%s]",
                        Joiner.on(", ").join(targetTableTypes),
                        Joiner.on(", ").join(sourceTableTypes));
            }

            //creating the insert statements
            //example of a querry
            //INSERT INTO memory.d1.test
            //SELECT * FROM memory.d2.test;
            // TODO: Porbably not where I should add this!
            QualifiedObjectName tQON = targetSchema.getQualifiedName();
            QualifiedObjectName sQON = sourceSchema.getQualifiedName();
            //SqlParser sqlParser = new SqlParser();
            String query = String.format("INSERT INTO %s SELECT * FROM %s", tQON.toString(), sQON.toString());
            QueryId queryId = dispatchManager.createQueryId();
            // TODO: Save the slug?
            Slug slug = Slug.createNew();
            ListenableFuture<Void> queryFuture = dispatchManager.createQuery(queryId, slug, context, query);
            queryFutures.put(queryId, queryFuture);
            synchronized (this) {
                allDone.put(queryId, false);
            }

            //System.out.println("state is "+dispatchManager.getFullQueryInfo(queryId).get().getState());

            // addSuccessCallback uses the Thread that calls the the executable to run the callback, not suited for long running functions.
            // but is used everywhere in dispatch query.
            // if weird errors appear change this.
            addSuccessCallback(queryFuture, () -> dispatchManager.getQuery(queryId).addStateChangeListener(state ->
            {
                System.out.println(state);
                if (state.equals(QueryState.RUNNING)){
                    // based on code from Query.java and ExecutingStatementResource.java
                    // Flushing does not exist as a query state it only exists as a task state
                    ExchangeClient exchangeClient = exchangeClientSupplier.get(new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), DeltaUpdateTask.class.getSimpleName()));

                    queryManager.addOutputInfoListener(queryId, outputInfo -> {
                        System.out.println(outputInfo.getColumnNames());
                        for (URI outputLocation : outputInfo.getBufferLocations()) {
                            exchangeClient.addLocation(outputLocation);
                        }
                        if (outputInfo.isNoMoreBufferLocations()) {
                            exchangeClient.noMoreLocations();
                        }
                        // if ((!queryInfo.isFinalQueryInfo() && queryInfo.getState() != FAILED) || !exchangeClient.isClosed()) {
                        if (!exchangeClient.isClosed()) {
                            // TODO: to handle failure:
                            /*Futures.addCallback(future, new FutureCallback<>()
                            {@Override public void onSuccess(@Nullable Void result){}
                            @Override public void onFailure(Throwable throwable){fail(throwable);}}, directExecutor());
                             */

                            // Once the current lock holder in the exchangeClient gets unlocked all blocked ones get notified
                            // need chained blocking else we will spin
                            // they are also notified when a new page is added
                            // TODO: Here we should definately use a different executor
                            addSuccessCallback(exchangeClient.isBlocked(), () -> {
                                nextIsBlocked(exchangeClient, queryId);
                            });

                        } // else failure?
                    });
                }

                if (state.isDone()){
                    synchronized (this){
                        allDone.put(queryId, true);
                        if (allDone.values().stream().reduce(true, (a,b) -> a && b)){
                            // documentation warns of not doing this when holding a lock
                            outputConsumer.accept(Optional.empty());
                            boolean tvalue = future.set(null);
                            System.out.println("Queries finished: " + tvalue);
                        }
                    }
                }
            }));
        }


        // look into FluentFuture.from(query.waitForDispatched()) used in QueuedStatementResource
        // it waits for it to finish?

        // In TestFlushingStageState they do:
        // queryRunner.getCoordinator().getFullQueryInfo(queryId).getOutputStage().get().getState(), FLUSHING)


        //future.set(null);
        // outputConsumer.accept(Optional.empty());

        // TODO add a call back that indicates failure if one of the subqueries fails
        // TDOO: How to get back to a consistent state when one fails an the others succeed.
        return future;
    }

    private void nextIsBlocked(ExchangeClient exchangeClient, QueryId id){
         if (!exchangeClient.isClosed()) {
            //TODO: in the ExchangeOperator::getOutput, they do operatorContext.recordProcessedInput for stats?
             int i = 0;
             while(!exchangeClient.isFinished()){ // it looks like this doeds not always work, sometimes we make an extra loop when we get the page in the previous round
                 SerializedPage p = exchangeClient.pollPage();
                 // should I again call is blocked here? - In Query they don't
                 if (p != null) {
                     System.out.println("Got page: " + p.getPositionCount() + " id: " + id+ " i: "+i);
                     System.out.println("Got page: " + p);
                     i++;
                     //System.out.println("Blocked?: " + exchangeClient.isBlocked());
                 }
                 else {
                     // look at Query::waitForResults for how to make this properly.
                     // for these queries it is good enough
                     // in Query::removePagesFromExchange they break when it returns null
                     // If query is not yet finished it starts a new callback
                     //System.out.println("TODO: should check if blocked at each iteration.");
                     if(!exchangeClient.isFinished() ||!exchangeClient.isClosed()){
                         System.out.println("Page was null: " + " id: " + id+ " i: "+i);
                         addSuccessCallback(exchangeClient.isBlocked(), () -> {nextIsBlocked(exchangeClient, id);});
                         System.out.println("Ret: " + " id: " + id);
                         return;
                     }else{
                         System.out.println("???: " + " id: " + id);
                         return;
                     }
                 }
             }
        }
        System.out.println("Finished" + " id: " + id);
        // Query::closeExchangeClientIfNecessary
        // not sure if we should close here
        // TDOD: potentially call this twice, seems not to be an issue
        exchangeClient.close();
    }


    private void processSourceAndTarget()
    {
        List<String> targetParts = deltaUpdate.getTarget().getParts();
        if (targetParts.size() > 3) {
            throw new TrinoException(SYNTAX_ERROR, format("Too many dots in table name: %s", deltaUpdate.getTarget()));
        }
        String targetCatalogName;
        String targetSchemaName;
        String targetTableName = null;
        if (session.getCatalog().isPresent()) {
            targetCatalogName = session.getCatalog().get();
            if (session.getSchema().isPresent()) {
                targetSchemaName = session.getSchema().get();
                if (targetParts.size() == 3) {
                    targetTableName = targetParts.get(2);
                    if (!targetParts.get(0).equals(targetCatalogName) || !targetParts.get(1).equals(targetSchemaName)) {
                        throw new TrinoException(SYNTAX_ERROR, format("Catalog or Schema name of %s do not match session", deltaUpdate.getTarget()));
                    }
                }
                else if (targetParts.size() == 2) {
                    targetTableName = targetParts.get(1);
                    if (!targetParts.get(0).equals(targetSchemaName)) {
                        throw new TrinoException(SYNTAX_ERROR, format("Schema name of %s do not match session", deltaUpdate.getTarget()));
                    }
                }
                else {
                    targetTableName = targetParts.get(0);
                }
            }
            else {
                int i = 0;
                if (targetParts.size() == 3) {
                    i = 1;
                } //else it is the first
                targetSchemaName = targetParts.get(i);
                if (targetParts.size() == 3) {
                    targetTableName = targetParts.get(2);
                }
                else if (targetParts.size() == 2 && !targetParts.get(0).equals(targetCatalogName)) {
                    // user provided schema.table
                    targetTableName = targetParts.get(1);
                }
            }
        }
        else {
            targetCatalogName = targetParts.get(0);
            targetSchemaName = targetParts.get(1);
            if (targetParts.size() == 3) {
                targetTableName = targetParts.get(2);
            }
        }

        List<String> sourceParts = deltaUpdate.getSource().getParts();
        if (sourceParts.size() > 3 || sourceParts.size() == 1) {
            throw new TrinoException(SYNTAX_ERROR, format("Too many dots in table name: %s", deltaUpdate.getTarget()));
        }
        String sourceCatalogName;
        String sourceSchemaName;
        String sourceTableName = null;

        if (sourceParts.size() == 3) {
            sourceCatalogName = sourceParts.get(0);
            sourceSchemaName = sourceParts.get(1);
            sourceTableName = sourceParts.get(2);
        }
        else {
            sourceCatalogName = sourceParts.get(0);
            sourceSchemaName = sourceParts.get(1);
        }

        if ((sourceTableName == null && targetTableName != null) || (sourceTableName != null && targetTableName == null)) {
            throw new TrinoException(GENERIC_USER_ERROR, format("DELTAUPDATE: If either for the source or the target the" +
                    " tableName is provided then it must be provided for both"));
        }

        if (sourceTableName != null) {
            source = new QualifiedTablePrefix(sourceCatalogName, sourceSchemaName, sourceTableName);
        }
        else {
            source = new QualifiedTablePrefix(sourceCatalogName, sourceSchemaName);
        }

        if (targetTableName != null) {
            target = new QualifiedTablePrefix(targetCatalogName, targetSchemaName, targetTableName);
        }
        else {
            target = new QualifiedTablePrefix(targetCatalogName, targetSchemaName);
        }

        if (sourceTableName != null) {
            sourceQualifiedObjectNames = List.of(new QualifiedObjectName(sourceCatalogName, sourceSchemaName, sourceTableName));
            targetQualifiedObjectNames = List.of(new QualifiedObjectName(targetCatalogName, targetSchemaName, targetTableName));
        }
        else {
            sourceQualifiedObjectNames = metadata.listTables(session, new QualifiedTablePrefix(sourceCatalogName, sourceSchemaName));
            targetQualifiedObjectNames = metadata.listTables(session, new QualifiedTablePrefix(targetCatalogName, targetSchemaName));
        }
    }

    @Override
    protected void finalize()
            throws Throwable
    {
        super.finalize();
        System.out.println("object is deallocaed");
    }
}
