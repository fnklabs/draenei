package com.fnklabs.draenei.orm;

import com.codahale.metrics.Timer;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.MetricsFactory;
import com.fnklabs.draenei.orm.exception.CanNotBuildEntryCacheKey;
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
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class CacheableDataProvider<T extends Cacheable> extends DataProvider<T> {

    public static final Logger LOGGER = LoggerFactory.getLogger(CacheableDataProvider.class);

    /**
     * Distributed object dataGrid
     */
    @NotNull
    private final IMap<Long, T> dataGrid;

    @Nullable
    private final EventListener<T> eventListener;

    public CacheableDataProvider(Class<T> clazz,
                                 CassandraClient cassandraClient,
                                 HazelcastInstance hazelcastInstance,
                                 MetricsFactory metricsFactory,
                                 @Nullable EventListener<T> eventListener) {

        super(clazz, cassandraClient, hazelcastInstance, metricsFactory);

        this.eventListener = eventListener;

        /**
         * Initialize dataGrid
         */
        dataGrid = hazelcastInstance.<Long, T>getMap(getMapName(clazz));
    }

    public CacheableDataProvider(Class<T> clazz,
                                 CassandraClient cassandraClient,
                                 HazelcastInstance hazelcastInstance,
                                 MetricsFactory metricsFactory,
                                 ListeningExecutorService executorService) {

        super(clazz, cassandraClient, hazelcastInstance, metricsFactory);

        this.eventListener = null;

        /**
         * Initialize dataGrid
         */
        dataGrid = hazelcastInstance.<Long, T>getMap(getMapName(clazz));
    }


    @Override
    public ListenableFuture<T> findOneAsync(Object... keys) {
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_FIND).time();

        long cacheKey = buildCacheKey(keys);

        ListenableFuture<T> getFromDataGridFuture = getFromDataGrid(cacheKey);

        ListenableFuture<T> findEntityFuture = Futures.transform(getFromDataGridFuture, (T result) -> {

            if (result != null) {
                getMetricsFactory().getCounter(MetricsType.CACHEABLE_DATA_PROVIDER_HITS).inc();

                return getFromDataGridFuture;
            }

            // try to load entity from DB
            ListenableFuture<T> findFuture = super.findOneAsync(keys);

            Futures.addCallback(findFuture, new FutureCallback<T>() {
                @Override
                public void onSuccess(T result) {
                    if (result != null) {
                        result.setCacheKey(cacheKey);
                        getMap().putAsync(cacheKey, result);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    LOGGER.warn("Can't put to cache", t);
                }
            });

            return findFuture;
        });

        monitorFuture(time, findEntityFuture);

        return findEntityFuture;
    }

    @Override
    public ListenableFuture<Boolean> saveAsync(@NotNull T entity) {
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_SAVE).time();

        Timer.Context putAsyncTimer = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_PUT_ASYNC).time();

        ListenableFuture<T> putToCacheFuture = executeOnEntry(entity, new PutToCacheOperation<Long, T>(entity));

        monitorFuture(putAsyncTimer, putToCacheFuture);

        ListenableFuture<Boolean> saveFuture = Futures.transform(putToCacheFuture, (T result) -> {
            return super.saveAsync(entity);
        });

        Futures.addCallback(saveFuture, new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(@Nullable Boolean result) {
                if (result != null && result && eventListener != null) {
                    eventListener.onEntrySave(entity);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.warn("Cant save entry", t);
            }
        });

        return monitorFuture(time, saveFuture, result -> result);
    }

    @Override
    public ListenableFuture<Boolean> removeAsync(@NotNull T entity) {

        Timer.Context timer = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_REMOVE).time();

        long key = entity.getCacheKey() == null ? buildCacheKey(entity) : entity.getCacheKey();

        SettableFuture<Boolean> deleteFuture = SettableFuture.<Boolean>create();

        ICompletableFuture<T> removeFromDataGridFuture = (ICompletableFuture<T>) getMap().removeAsync(key);

        removeFromDataGridFuture.andThen(new ExecutionCallback<T>() {
            @Override
            public void onResponse(T response) {
                deleteFuture.set(true);
            }

            @Override
            public void onFailure(Throwable t) {
                deleteFuture.setException(t);
            }
        });

        monitorFuture(timer, deleteFuture, result -> {
            ListenableFuture<Boolean> removeFuture = super.removeAsync(entity);

            if (eventListener != null) {
                eventListener.onEntryRemove(entity);
            }

            return removeFuture;
        });

        return deleteFuture;
    }

    /**
     * Execute function on entry
     *
     * @param entry          Entry
     * @param entryProcessor User Function
     * @param <O>            Return object type from EntryProcessor
     *
     * @return Future for current operation
     */
    protected <O> ListenableFuture<O> executeOnEntry(@NotNull T entry, @NotNull EntryProcessor<Long, T> entryProcessor) {
        Long entityId = entry.getCacheKey() == null ? buildCacheKey(entry) : entry.getCacheKey();

        ICompletableFuture<O> completableFuture = (ICompletableFuture<O>) getMap().submitToKey(entityId, entryProcessor);

        SettableFuture<O> responseFuture = SettableFuture.<O>create();

        completableFuture.andThen(new ExecutionCallback<O>() {
            @Override
            public void onResponse(O response) {
                responseFuture.set(response);
            }

            @Override
            public void onFailure(Throwable t) {
                responseFuture.setException(t);
            }
        });

        return responseFuture;
    }

    @NotNull
    protected String getMapName() {
        return getMap().getName();
    }

    protected IMap<Long, T> getMap() {
        return dataGrid;
    }



    @NotNull
    private String getMapName(Class<T> clazz) {
        return StringUtils.lowerCase(clazz.getName());
    }

    @NotNull
    private ListenableFuture<T> getFromDataGrid(long cacheKey) {
        Timer.Context timer = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_GET_FROM_DATA_GRID).time();
        // try to find entity in DataGrid then try to load it from DB
        ICompletableFuture<T> future = (ICompletableFuture<T>) getMap().getAsync(cacheKey);

        SettableFuture<T> responseFuture = SettableFuture.<T>create();

        future.andThen(new ExecutionCallback<T>() {
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
        });

        return responseFuture;

    }

    protected enum MetricsType implements MetricsFactory.Type {
        CACHEABLE_DATA_PROVIDER_FIND,
        CACHEABLE_DATA_PROVIDER_SAVE,
        CACHEABLE_DATA_PROVIDER_CREATE_KEY,
        CACHEABLE_DATA_PROVIDER_HITS,
        CACHEABLE_DATA_PROVIDER_REMOVE, CACHEABLE_DATA_GET_FROM_DATA_GRID, CACHEABLE_DATA_PROVIDER_PUT_ASYNC;

    }
}
