package com.fnklabs.draenei.orm;

import com.codahale.metrics.Timer;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.MetricsFactory;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.*;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Future;

public abstract class CacheableDataProvider<T extends Cacheable> extends DataProvider<T> {

    /**
     * hashing function to build Entity cache id
     */
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

    /**
     * Distributed object dataGrid
     */
    private final IMap<Long, T> dataGrid;

    public CacheableDataProvider(Class<T> clazz,
                                 CassandraClient cassandraClient,
                                 HazelcastInstance hazelcastClient,
                                 MetricsFactory metricsFactory,
                                 ListeningExecutorService executorService) {

        super(clazz, cassandraClient, metricsFactory, executorService);

        /**
         * Initialize dataGrid
         */
        dataGrid = hazelcastClient.<Long, T>getMap(getMapName(clazz));
    }

    @Override
    public ListenableFuture<T> findOneAsync(Object... keys) {
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_FIND).time();

        long entityId = buildEntityId(keys);

        ListenableFuture<T> findEntityFuture = Futures.transform(getFromDataGrid(entityId), (T result) -> {

            if (result != null) {
                getMetricsFactory().getCounter(MetricsType.CACHEABLE_DATA_PROVIDER_HITS).inc();

                SettableFuture<T> resultFuture = SettableFuture.<T>create();
                resultFuture.set(result);

                return resultFuture;
            }

            // try to load entity from DB
            ListenableFuture<T> findFuture = super.findOneAsync(keys);

            return Futures.transform(findFuture, (T entity) -> {
                if (entity != null) {
                    entity.setCacheId(entityId);
                    getMap().putAsync(entityId, entity);
                }

                return entity;
            }, getExecutorService());

        }, getExecutorService());

        monitorFuture(time, findEntityFuture);

        return findEntityFuture;
    }

    @Override
    public ListenableFuture<Boolean> saveAsync(@NotNull T entity) {
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_SAVE).time();

        ListenableFuture<Boolean> saveToDataGridFuture = executeOnEntry(entity, new AddToCacheOperation<>(entity));

        monitorFuture(time, saveToDataGridFuture, result -> {
            if (result) {
                super.saveAsync(entity);
                onEntrySave(entity);
            }
        });

        return saveToDataGridFuture;
    }

    @Override
    public ListenableFuture<Boolean> removeAsync(@NotNull T entity) {

        ListenableFuture<Boolean> submit = getExecutorService().submit(() -> {
            Timer.Context timer = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_REMOVE).time();

            long key = entity.getCacheId() == null ? buildEntityId(entity) : entity.getCacheId();

//        ListenableFuture<T> removeFuture = JdkFutureAdapters.listenInPoolThread(dataGrid.removeAsync(key), getExecutorService());
            getMap().remove(key);
            onEntryRemove(entity);
            super.removeAsync(entity);

            timer.stop();

            return true;
        });

        SettableFuture<Boolean> settableFuture = SettableFuture.<Boolean>create();
        settableFuture.set(true);
        return settableFuture;
    }

    /**
     * Notification when entry was removed
     *
     * @param entity Entity that must be removed
     */
    protected void onEntryRemove(T entity) {

    }

    protected void onEntrySave(T entity) {

    }

    /**
     * Execute function on entry
     *
     * @param entry          Entry
     * @param entryProcessor User Function
     *
     * @return Future for current operation
     */
    protected ListenableFuture<Boolean> executeOnEntry(@NotNull T entry, @NotNull EntryProcessor<String, T> entryProcessor) {
        SettableFuture<Boolean> booleanSettableFuture = SettableFuture.<Boolean>create();

        Long entityId = entry.getCacheId() == null ? buildEntityId(entry) : entry.getCacheId();

        getExecutorService().submit(() -> {
            getMap().submitToKey(entityId, entryProcessor, new ExecutionCallback() {
                @Override
                public void onResponse(Object response) {
                    booleanSettableFuture.set(true);
                }

                @Override
                public void onFailure(Throwable t) {
                    booleanSettableFuture.set(false);
                }
            });

        });

        return booleanSettableFuture;
    }

    protected long buildEntityId(@NotNull T entity) {
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_CREATE_KEY).time();

        long key = buildEntityId(entity, getFunnel());

        time.stop();
        return key;
    }

    protected abstract long buildEntityId(@NotNull Object... keys);

    abstract protected Funnel<T> getFunnel();

    @NotNull
    protected String getMapName() {
        return getMap().getName();
    }

    protected IMap<Long, T> getMap() {
        return dataGrid;
    }

    @NotNull
    private ListenableFuture<T> getFromDataGrid(long entityId) {
        Timer.Context timer = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_GET_FROM_DATA_GRID).time();
        // try to find entity in DataGrid then try to load it from DB
        Future<T> future = getMap().getAsync(entityId);

        if (future instanceof ICompletableFuture) {
            SettableFuture<T> responseFuture = SettableFuture.<T>create();

            ((ICompletableFuture<T>) future).andThen(new ExecutionCallback<T>() {
                @Override
                public void onResponse(T response) {
                    timer.stop();
                    responseFuture.set(response);
                }

                @Override
                public void onFailure(Throwable t) {
                    timer.stop();
                    responseFuture.setException(t);
                }
            }, getExecutorService());
        }

        return JdkFutureAdapters.listenInPoolThread(future, getExecutorService());
    }

    @NotNull
    private String getMapName(Class<T> clazz) {
        return StringUtils.lowerCase(clazz.getName());
    }

    protected enum MetricsType implements MetricsFactory.Type {
        CACHEABLE_DATA_PROVIDER_FIND,
        CACHEABLE_DATA_PROVIDER_SAVE,
        CACHEABLE_DATA_PROVIDER_CREATE_KEY,
        CACHEABLE_DATA_PROVIDER_HITS,
        CACHEABLE_DATA_PROVIDER_REMOVE, CACHEABLE_DATA_GET_FROM_DATA_GRID;

    }

    private static <T> long buildEntityId(T entity, Funnel<T> funnel) {
        return HASH_FUNCTION.hashObject(entity, funnel).asLong();
    }
}
