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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Provider;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.warnings.WarningCollector;
import io.trino.memory.VersionedMemoryPoolId;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Metadata;
import io.trino.operator.ExchangeClientSupplier;
import io.trino.operator.ForDeltaUpdate;
import io.trino.security.AccessControl;
import io.trino.server.BasicQueryInfo;
import io.trino.server.DeltaFlagRequest;
import io.trino.server.ForStatementResource;
import io.trino.server.protocol.QueryInfoUrlFactory;
import io.trino.server.protocol.Slug;
import io.trino.spi.QueryId;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.sql.planner.Plan;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Statement;
import io.trino.transaction.TransactionManager;
import org.joda.time.DateTime;
import io.airlift.http.client.HttpClient;

import javax.annotation.Nullable;
import javax.inject.Inject;


import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class DeltaUpdateExecution<T extends Statement>
        implements QueryExecution
{
    // Based on DataDefinitionExecution
    private final DataDefinitionTask<T> task;
    private final T statement;
    private final Slug slug;
    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final QueryStateMachine stateMachine;
    private final List<Expression> parameters;
    private final WarningCollector warningCollector;

    private DeltaUpdateExecution(
            DataDefinitionTask<T> task,
            T statement,
            Slug slug,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        this.task = requireNonNull(task, "task is null");
        this.statement = requireNonNull(statement, "statement is null");
        this.slug = requireNonNull(slug, "slug is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.parameters = parameters;
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    @Override
    public Slug getSlug()
    {
        return slug;
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return stateMachine.getMemoryPool();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        stateMachine.setMemoryPool(poolId);
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return DataSize.ofBytes(0);
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return DataSize.ofBytes(0);
    }

    @Override
    public DateTime getCreateTime()
    {
        return stateMachine.getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return stateMachine.getExecutionStartTime();
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        return stateMachine.getLastHeartbeat();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return stateMachine.getEndTime();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return new Duration(0, NANOSECONDS);
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return stateMachine.getFinalQueryInfo()
                .map(BasicQueryInfo::new)
                .orElseGet(() -> stateMachine.getBasicQueryInfo(Optional.empty()));
    }

    @Override
    public void start()
    {
        try {
            // transition to running
            if (!stateMachine.transitionToRunning()) {
                // query already running or finished
                return;
            }

            ListenableFuture<Void> future = task.execute(statement, transactionManager, metadata, accessControl, stateMachine, parameters, warningCollector);
            Futures.addCallback(future, new FutureCallback<>()
            {
                @Override
                public void onSuccess(@Nullable Void result)
                {
                    stateMachine.transitionToFinishing();
                }

                @Override
                public void onFailure(Throwable throwable)
                {
                    fail(throwable);
                }
            }, directExecutor());
        }
        catch (Throwable e) {
            fail(e);
            throwIfInstanceOf(e, Error.class);
        }
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        // DDL does not have an output
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return stateMachine.getStateChange(currentState);
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        stateMachine.addQueryInfoStateChangeListener(stateChangeListener);
    }

    @Override
    public void fail(Throwable cause)
    {
        stateMachine.transitionToFailed(cause);
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public void cancelQuery()
    {
        stateMachine.transitionToCanceled();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        // no-op
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public boolean shouldWaitForMinWorkers()
    {
        return false;
    }

    @Override
    public void pruneInfo()
    {
        // no-op
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return stateMachine.getFinalQueryInfo().orElseGet(() -> stateMachine.updateQueryInfo(Optional.empty()));
    }

    @Override
    public Plan getQueryPlan()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    @Override
    public Optional<Duration> getPlanningTime()
    {
        return stateMachine.getPlanningTime();
    }

    public List<Expression> getParameters()
    {
        return parameters;
    }

    public static class DeltaUpdateExecutionFactory
            implements QueryExecutionFactory<DeltaUpdateExecution<?>>
    {
        private final TransactionManager transactionManager;
        private final Metadata metadata;
        private final AccessControl accessControl;
        private final Provider<DispatchManager> dispatchManagerProvider;
        private final QueryManager queryManager;
        private final ExchangeClientSupplier exchangeClientSupplier;
        private final BoundedExecutor responseExecutor;
        private final QueryInfoUrlFactory queryInfoUrlFactory;
        private final ScheduledExecutorService timeoutExecutor;
        private final BlockEncodingSerde blockEncodingSerde;
        private final InternalNodeManager internalNodeManager;
        private final HttpClient httpClient;
        private final LocationFactory locationFactory;
        private final JsonCodec<DeltaFlagRequest> deltaFlagRequestCodec;

        @Inject
        public DeltaUpdateExecutionFactory(
                TransactionManager transactionManager,
                Metadata metadata,
                AccessControl accessControl,
                Provider<DispatchManager> dispatchManagerProvider,
                QueryManager queryManager,
                ExchangeClientSupplier exchangeClientSupplier,
                @ForStatementResource BoundedExecutor responseExecutor,
                @ForStatementResource ScheduledExecutorService timeoutExecutor,
                QueryInfoUrlFactory queryInfoUrlFactory,
                BlockEncodingSerde blockEncodingSerde,
                InternalNodeManager internalNodeManager,
                @ForDeltaUpdate HttpClient httpClient,
                LocationFactory locationFactory,
                JsonCodec<DeltaFlagRequest> deltaFlagRequestCodec)
        {
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.dispatchManagerProvider = dispatchManagerProvider;
            this.queryManager = queryManager;
            this.exchangeClientSupplier = exchangeClientSupplier;
            this.responseExecutor = responseExecutor;
            this.queryInfoUrlFactory = queryInfoUrlFactory;
            this.timeoutExecutor = timeoutExecutor;
            this.blockEncodingSerde = blockEncodingSerde;
            this.internalNodeManager = internalNodeManager;
            this.httpClient = httpClient;
            this.locationFactory = locationFactory;
            this.deltaFlagRequestCodec = deltaFlagRequestCodec;

        }

        @Override
        public DeltaUpdateExecution<?> createQueryExecution(
                PreparedQuery preparedQuery,
                QueryStateMachine stateMachine,
                Slug slug,
                WarningCollector warningCollector)
        {
            return createDataDefinitionExecution(preparedQuery.getStatement(), preparedQuery.getParameters(), stateMachine, slug, warningCollector);
        }

        private <T extends Statement> DeltaUpdateExecution<T> createDataDefinitionExecution(
                T statement,
                List<Expression> parameters,
                QueryStateMachine stateMachine,
                Slug slug,
                WarningCollector warningCollector)
        {
            @SuppressWarnings("unchecked")
            DataDefinitionTask<T> task = (DataDefinitionTask<T>) new DeltaUpdateTask(dispatchManagerProvider.get(), queryManager, queryInfoUrlFactory,
                    exchangeClientSupplier, responseExecutor, timeoutExecutor, blockEncodingSerde, internalNodeManager, httpClient, locationFactory,
                    deltaFlagRequestCodec);

            stateMachine.setUpdateType(task.getName());
            return new DeltaUpdateExecution<>(task, statement, slug, transactionManager, metadata, accessControl, stateMachine, parameters, warningCollector);
        }
    }
}
