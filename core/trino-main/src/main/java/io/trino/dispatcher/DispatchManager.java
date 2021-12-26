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
package io.trino.dispatcher;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.QueryManagerStats;
import io.trino.execution.QueryPreparer;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.QueryTracker;
import io.trino.execution.resourcegroups.ResourceGroupManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.server.BasicQueryInfo;
import io.trino.server.DeltaRequestExchanger;
import io.trino.server.SessionContext;
import io.trino.server.SessionPropertyDefaults;
import io.trino.server.SessionSupplier;
import io.trino.server.protocol.Slug;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;
import io.trino.transaction.TransactionManager;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.QUERY_TEXT_TOO_LARGE;
import static io.trino.util.StatementUtils.getQueryType;
import static io.trino.util.StatementUtils.isTransactionControlStatement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class DispatchManager
{
    private final QueryIdGenerator queryIdGenerator;
    private final QueryPreparer queryPreparer;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final DispatchQueryFactory dispatchQueryFactory;
    private final FailedDispatchQueryFactory failedDispatchQueryFactory;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final SessionSupplier sessionSupplier;
    private final SessionPropertyDefaults sessionPropertyDefaults;
    private final SessionPropertyManager sessionPropertyManager;
    private final DeltaRequestExchanger deltaRequestExchanger;

    private final int maxQueryLength;

    private final Executor dispatchExecutor;

    public final QueryTracker<DispatchQuery> queryTracker;

    private final QueryManagerStats stats = new QueryManagerStats();

    private final ScheduledExecutorService taskBatcher;

    private final ScheduledExecutorService batchExecutor;

    private final List<CreateQueryStore> batchList = new LinkedList<CreateQueryStore>();

    private final List<QueryId> batchedQueries = new ArrayList<>();

    private final List<ListenableFuture<Void>> batchFuturesList = new ArrayList<>();

    private static final Logger log = Logger.get(DispatchManager.class);

    // locks the batchList
    public final ReentrantLock batchListLock = new ReentrantLock(true);

    // held during the entire time that the deltas are applied.
    public final ReentrantLock deltaupdateLock = new ReentrantLock(true);

    //private final AtomicBoolean atomicBoolean = new AtomicBoolean();

    private final AtomicReference<QueryId> atomicReference = new AtomicReference<>();

    private CreateQueryStore deferredDeltaUpdate = null;

    // looks the entire defferedDeltaUpdate process, it executes a deltaupdate that comes in whilst the delta is applied.
    public final ReentrantLock deferredDeltaUpdateLock = new ReentrantLock(true);

    // looks the batchFuturesList
    public final ReentrantLock batchFuturesListLock = new ReentrantLock(true);

    @Inject
    public DispatchManager(
            QueryIdGenerator queryIdGenerator,
            QueryPreparer queryPreparer,
            ResourceGroupManager<?> resourceGroupManager,
            DispatchQueryFactory dispatchQueryFactory,
            FailedDispatchQueryFactory failedDispatchQueryFactory,
            TransactionManager transactionManager,
            AccessControl accessControl,
            SessionSupplier sessionSupplier,
            SessionPropertyDefaults sessionPropertyDefaults,
            SessionPropertyManager sessionPropertyManager,
            QueryManagerConfig queryManagerConfig,
            DispatchExecutor dispatchExecutor,
            DeltaRequestExchanger deltaRequestExchanger)
    {
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.dispatchQueryFactory = requireNonNull(dispatchQueryFactory, "dispatchQueryFactory is null");
        this.failedDispatchQueryFactory = requireNonNull(failedDispatchQueryFactory, "failedDispatchQueryFactory is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.sessionPropertyDefaults = requireNonNull(sessionPropertyDefaults, "sessionPropertyDefaults is null");
        this.sessionPropertyManager = sessionPropertyManager;
        this.deltaRequestExchanger = deltaRequestExchanger;

        requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.maxQueryLength = queryManagerConfig.getMaxQueryLength();

        this.dispatchExecutor = requireNonNull(dispatchExecutor, "dispatchExecutor is null").getExecutor();

        this.queryTracker = new QueryTracker<>(queryManagerConfig, dispatchExecutor.getScheduledExecutor());

        taskBatcher = newScheduledThreadPool(1, threadsNamed("task-batcher-%s"));

        batchExecutor = newScheduledThreadPool(1, threadsNamed("batcher-executor-%s"));
    }

    @PostConstruct
    public void start()
    {
        queryTracker.start();

        taskBatcher.scheduleWithFixedDelay(() -> {
            try {
                startBatch();
            }
            catch (Throwable e) {
                log.warn(e, "Error scheduling the Batch Tasks");
            }
        }, 200, 200, TimeUnit.MILLISECONDS); // waits for current to finish only then is the timer for the next started.
    }

    @PreDestroy
    public void stop()
    {
        queryTracker.stop();
        taskBatcher.shutdown();
    }

    @Managed
    @Flatten
    public QueryManagerStats getStats()
    {
        return stats;
    }

    public QueryId createQueryId()
    {
        return queryIdGenerator.createNextQueryId();
    }

    public void startBatch()
    {
        //log.info("start startBatch");
        batchFuturesListLock.lock();
        if (batchFuturesList.isEmpty()) {
            batchListLock.lock();
            // copy the global list such that createQuery can add new queries to the global list
            List<CreateQueryStore> localBatchList = new ArrayList<CreateQueryStore>(batchList);
            batchList.clear();
            batchListLock.unlock();

            if (localBatchList.isEmpty()){
                //log.info("unlock batchFuturesListLock");
                batchFuturesListLock.unlock();
                return;
            }
            log.info("startBatch not empty");
            Iterator<CreateQueryStore> it = localBatchList.iterator();
            // TODO: check if all queries are finished, keep track of all queries that were started
            // Then start the integration of the delta data
            // When finished, run this next batch
            while (it.hasNext()) {
                CreateQueryStore cqs = it.next();
                ListenableFuture<Void> listenableFuture = createQueryBatched(cqs.queryId, cqs.slug, cqs.sessionContext, cqs.query);
                batchFuturesList.add(listenableFuture);
                Futures.addCallback(listenableFuture, new FutureCallback<>()
                {
                    @Override
                    public void onSuccess(@org.checkerframework.checker.nullness.qual.Nullable Void result)
                    {
                        log.info("Batched Query has been dispatched");
                        cqs.settableFuture.set(null);
                    }

                    @Override
                    public void onFailure(Throwable throwable)
                    {
                        log.warn("createQuery future failed");
                        cqs.settableFuture.setException(throwable);
                    }
                }, directExecutor());
                batchedQueries.add(cqs.queryId);
                it.remove();
            }

            ArrayList<ListenableFuture<Void>> queriesFinished = new ArrayList<>();

            // Do not care if they finished successfully,
            // Indicates that all of them have been dispatched
            ListenableFuture<Void> allSucceeded = Futures.whenAllComplete(batchFuturesList).call(() -> {
                log.info("All queries dispatched");
                for (int i = 0; i < batchFuturesList.size(); i++) {
                    ListenableFuture<Void> dispatched = batchFuturesList.get(i);
                    try {
                        Futures.getDone(dispatched);
                        SettableFuture<Void> done = SettableFuture.create();
                        queriesFinished.add(done);
                        getQuery(batchedQueries.get(i)).addStateChangeListener(state -> {
                            if (state.isDone()) {
                                log.info("Query is done");
                                done.set(null);
                            }
                        });
                    }
                    catch (ExecutionException | CancellationException e) {
                        log.warn("Query failed to dispatch: " + batchedQueries.get(i));
                    }
                }
                return null;
            }, directExecutor());

            // Starting applying the deltaupdate
            allSucceeded.addListener(() -> {
                log.info("All queries of the batch are done");
                // do the delta update
                Futures.whenAllComplete(queriesFinished).call(() -> {
                    // held for the duration of applying the stored deltas
                    log.info("All queries of the batch are done inner");
                    deltaupdateLock.lock();
                    ListenableFuture<Void> deltaStart = deltaRequestExchanger.markDeltaUpdate();
                    deltaStart.addListener(() -> {
                        log.info("finished marking the delta");
                        ListenableFuture<Void> deltaEnd = deltaRequestExchanger.unmarkDeltaUpdate();
                        deltaEnd.addListener(() -> {
                            log.info("finished unmarking the delta");
                            batchedQueries.clear();
                            log.info("before the lock");
                            batchFuturesListLock.lock();
                            log.info("after the lock");
                            batchFuturesList.clear();
                            batchFuturesListLock.unlock();
                            // held for the duration of applying the batch
                            deltaupdateLock.unlock();

                            // no looks should be held when this function is called
                            startBlockedDeltaupdate();
                            log.info("Finished entire apply batch");
                        }, batchExecutor);
                    }, batchExecutor);
                    return null;
                }, batchExecutor);
            }, batchExecutor);
        }
        //log.info("finished startBatch");
        batchFuturesListLock.unlock();
    }

    public void startBlockedDeltaupdate(){
        log.info("startBlockedDeltaupdate");
        assert (!deltaupdateLock.isHeldByCurrentThread() && !batchListLock.isHeldByCurrentThread());
        deferredDeltaUpdateLock.lock();
        if (deferredDeltaUpdate != null) {
            assert (atomicReference.get() == null);
            atomicReference.compareAndSet(null, deferredDeltaUpdate.queryId);
            CreateQueryStore ddu = deferredDeltaUpdate;
            ListenableFuture<Void> listenableFuture = createQueryBatched(ddu.queryId, ddu.slug, ddu.sessionContext, ddu.query);
            Futures.addCallback(listenableFuture, new FutureCallback<>()
            {
                @Override
                public void onSuccess(@org.checkerframework.checker.nullness.qual.Nullable Void result)
                {
                    log.info("deferred deltaupdate is dispatched");
                    ddu.settableFuture.set(null);
                    getQuery(ddu.queryId).addStateChangeListener(state -> {
                        if (state.isDone()) {
                            assert (atomicReference.get() == ddu.queryId);
                            log.info("deferred deltaupdate is done");
                            atomicReference.compareAndSet(ddu.queryId, null);
                        }
                    });
                }

                @Override
                public void onFailure(Throwable throwable)
                {
                    log.warn("createQuery future failed for the deferred delta update");
                    ddu.settableFuture.setException(throwable);
                }
            }, directExecutor());
        }
        deferredDeltaUpdate = null;
        deferredDeltaUpdateLock.unlock();
    }

    public ListenableFuture<Void> createQuery(QueryId queryId, Slug slug, SessionContext sessionContext, String query)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(sessionContext, "sessionContext is null");
        requireNonNull(query, "query is null");
        checkArgument(!query.isEmpty(), "query must not be empty string");
        checkArgument(queryTracker.tryGetQuery(queryId).isEmpty(), "query %s already exists", queryId);
        if (query.toUpperCase().startsWith("DELTAUPDATE")) {
            log.info("GOT DELTAUPDATE");
            // Want: only one deltaupdate going on at a time
            // No deltaupdates when we apply the deltas.

            // need to hold this lock before calling isLocked
            deferredDeltaUpdateLock.lock();
            if (deltaupdateLock.isLocked()) {
                // should add a queue that holds all deltaupdate queries and only processes the top one
                // However in my implementation of the leveldb server there will alway only be one deltaupdate at a time
                if (deferredDeltaUpdate == null) {
                    log.info("DELTAUPDATE DEFERRED UPDATE");
                    SettableFuture<Void> settableFuture = SettableFuture.create();
                    CreateQueryStore store = new CreateQueryStore(queryId, slug, sessionContext, query, settableFuture);
                    deferredDeltaUpdate = store;
                    deferredDeltaUpdateLock.unlock();
                    return settableFuture;
                }else{
                    SettableFuture<Void> settableFuture = SettableFuture.create();
                    settableFuture.setException(new TrinoException(GENERIC_INTERNAL_ERROR, "Only one Deltaupdate can be buffered"));
                    return settableFuture;
                }
            } else {
                log.info("DELTAUPDATE REGULAR");
                deferredDeltaUpdateLock.unlock();
                if (atomicReference.compareAndSet(null, queryId)) {
                    deltaupdateLock.lock();
                    ListenableFuture<Void> listenableFuture = createQueryBatched(queryId, slug, sessionContext, query);
                    listenableFuture.addListener(() -> {
                        log.info("deltaupdate query dispatched");
                        getQuery(queryId).addStateChangeListener(state -> {
                            if (state.isDone()) {
                                assert (atomicReference.get() == queryId);
                                log.info("deltaupdate query is done");
                                atomicReference.compareAndSet(queryId, null);
                            }
                        });
                    }, directExecutor());
                    deltaupdateLock.unlock();
                    // TODO: add a return here
                    return listenableFuture;
                }
                else {
                    SettableFuture<Void> settableFuture = SettableFuture.create();
                    settableFuture.setException(new TrinoException(GENERIC_INTERNAL_ERROR, "Only one Deltaupdate at a time can happen"));
                    return settableFuture;
                }
            }
        } else if (query.toUpperCase().startsWith("INSERT")){
            // We assume the inserts (after setup) are entirely caused by the delta update.
            // otherwise, we would need to prevent them from running when the delta is applied.

            // Also do not care to figure out when INSERT is finished as this is handled by DELTAUPDATE
            log.info("INSERT QUERY");
            return createQueryBatched(queryId, slug, sessionContext, query);

        }
        else {
            log.info("QUERY REGULAR");
            SettableFuture<Void> settableFuture = SettableFuture.create();
            CreateQueryStore store = new CreateQueryStore(queryId, slug, sessionContext, query, settableFuture);
            batchListLock.lock();
            batchList.add(store);
            batchListLock.unlock();
            return settableFuture;
        }
    }

    public ListenableFuture<Void> createQueryBatched(QueryId queryId, Slug slug, SessionContext sessionContext, String query){
        // It is important to return a future implementation which ignores cancellation request.
        // Using NonCancellationPropagatingFuture is not enough; it does not propagate cancel to wrapped future
        // but it would still return true on call to isCancelled() after cancel() is called on it.
        DispatchQueryCreationFuture queryCreationFuture = new DispatchQueryCreationFuture();
        dispatchExecutor.execute(() -> {
            try {
                createQueryInternal(queryId, slug, sessionContext, query, resourceGroupManager);
            }
            finally {
                queryCreationFuture.set(null);
            }
        });
        return queryCreationFuture;
    }

    /**
     * Creates and registers a dispatch query with the query tracker.  This method will never fail to register a query with the query
     * tracker.  If an error occurs while creating a dispatch query, a failed dispatch will be created and registered.
     */
    private <C> void createQueryInternal(QueryId queryId, Slug slug, SessionContext sessionContext, String query, ResourceGroupManager<C> resourceGroupManager)
    {
        Session session = null;
        PreparedQuery preparedQuery = null;
        try {
            if (query.length() > maxQueryLength) {
                int queryLength = query.length();
                query = query.substring(0, maxQueryLength);
                throw new TrinoException(QUERY_TEXT_TOO_LARGE, format("Query text length (%s) exceeds the maximum length (%s)", queryLength, maxQueryLength));
            }

            // decode session
            session = sessionSupplier.createSession(queryId, sessionContext);

            // check query execute permissions
            accessControl.checkCanExecuteQuery(sessionContext.getIdentity());

            // prepare query
            preparedQuery = queryPreparer.prepareQuery(session, query);

            // select resource group
            Optional<String> queryType = getQueryType(preparedQuery.getStatement()).map(Enum::name);
            SelectionContext<C> selectionContext = resourceGroupManager.selectGroup(new SelectionCriteria(
                    sessionContext.getIdentity().getPrincipal().isPresent(),
                    sessionContext.getIdentity().getUser(),
                    sessionContext.getIdentity().getGroups(),
                    sessionContext.getSource(),
                    sessionContext.getClientTags(),
                    sessionContext.getResourceEstimates(),
                    queryType));

            // apply system default session properties (does not override user set properties)
            session = sessionPropertyDefaults.newSessionWithDefaultProperties(session, queryType, selectionContext.getResourceGroupId());

            // mark existing transaction as active
            transactionManager.activateTransaction(session, isTransactionControlStatement(preparedQuery.getStatement()), accessControl);

            DispatchQuery dispatchQuery = dispatchQueryFactory.createDispatchQuery(
                    session,
                    query,
                    preparedQuery,
                    slug,
                    selectionContext.getResourceGroupId());

            boolean queryAdded = queryCreated(dispatchQuery, sessionContext);
            if (queryAdded && !dispatchQuery.isDone()) {
                try {
                    resourceGroupManager.submit(dispatchQuery, selectionContext, dispatchExecutor);
                }
                catch (Throwable e) {
                    // dispatch query has already been registered, so just fail it directly
                    dispatchQuery.fail(e);
                }
            }
        }
        catch (Throwable throwable) {
            // creation must never fail, so register a failed query in this case
            if (session == null) {
                session = Session.builder(sessionPropertyManager)
                        .setQueryId(queryId)
                        .setIdentity(sessionContext.getIdentity())
                        .setSource(sessionContext.getSource().orElse(null))
                        .build();
            }
            Optional<String> preparedSql = Optional.ofNullable(preparedQuery).flatMap(PreparedQuery::getPrepareSql);
            DispatchQuery failedDispatchQuery = failedDispatchQueryFactory.createFailedDispatchQuery(session, query, preparedSql, Optional.empty(), throwable);
            queryCreated(failedDispatchQuery, sessionContext);
        }
    }

    private boolean queryCreated(DispatchQuery dispatchQuery, SessionContext context)
    {
        boolean queryAdded = queryTracker.addQuery(dispatchQuery, context);

        // only add state tracking if this query instance will actually be used for the execution
        if (queryAdded) {
            dispatchQuery.addStateChangeListener(newState -> {
                if (newState.isDone()) {
                    // execution MUST be added to the expiration queue or there will be a leak
                    queryTracker.expireQuery(dispatchQuery.getQueryId());
                }
            });
            stats.trackQueryStats(dispatchQuery);
        }

        return queryAdded;
    }

    public ListenableFuture<Void> waitForDispatched(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(dispatchQuery -> {
                    dispatchQuery.recordHeartbeat();
                    return dispatchQuery.getDispatchedFuture();
                })
                .orElseGet(Futures::immediateVoidFuture);
    }

    public List<BasicQueryInfo> getQueries()
    {
        return queryTracker.getAllQueries().stream()
                .map(DispatchQuery::getBasicQueryInfo)
                .collect(toImmutableList());
    }

    @Managed
    public long getQueuedQueries()
    {
        return queryTracker.getAllQueries().stream()
                .filter(query -> query.getState() == QUEUED)
                .count();
    }

    @Managed
    public long getRunningQueries()
    {
        return queryTracker.getAllQueries().stream()
                .filter(query -> query.getState() == RUNNING && !query.getBasicQueryInfo().getQueryStats().isFullyBlocked())
                .count();
    }

    public boolean isQueryRegistered(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId).isPresent();
    }

    public DispatchQuery getQuery(QueryId queryId)
    {
        return queryTracker.getQuery(queryId);
    }

    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getBasicQueryInfo();
    }

    public Optional<QueryInfo> getFullQueryInfo(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId).map(DispatchQuery::getFullQueryInfo);
    }

    public Optional<DispatchInfo> getDispatchInfo(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(dispatchQuery -> {
                    dispatchQuery.recordHeartbeat();
                    return dispatchQuery.getDispatchInfo();
                });
    }

    public void cancelQuery(QueryId queryId)
    {
        queryTracker.tryGetQuery(queryId)
                .ifPresent(DispatchQuery::cancel);
    }

    public void failQuery(QueryId queryId, Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        queryTracker.tryGetQuery(queryId)
                .ifPresent(query -> query.fail(cause));
    }

    private static class DispatchQueryCreationFuture
            extends AbstractFuture<Void>
    {
        @Override
        protected boolean set(Void value)
        {
            return super.set(value);
        }

        @Override
        protected boolean setException(Throwable throwable)
        {
            return super.setException(throwable);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            // query submission cannot be canceled
            return false;
        }
    }

    private static class CreateQueryStore
    {
        QueryId queryId;
        Slug slug;
        SessionContext sessionContext;
        String query;
        SettableFuture<Void> settableFuture;
        CreateQueryStore(QueryId queryId, Slug slug, SessionContext sessionContext, String query, SettableFuture<Void> settableFuture){
            this.queryId = queryId;
            this.slug = slug;
            this.sessionContext = sessionContext;
            this.query = query;
            this.settableFuture = settableFuture;
        }
    }
}
