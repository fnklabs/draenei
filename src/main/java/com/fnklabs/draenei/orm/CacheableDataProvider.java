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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public abstract class CacheableDataProvider<T extends Cacheable> extends DataProvider<T> {

    /**
     * hashing function to build Entity cache id
     */
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

    /**
     * Distributed object dataGrid
     */
    private final IMap<Long, T> dataGrid;

    private final EventListener<T> eventListener;

    public CacheableDataProvider(Class<T> clazz,
                                 CassandraClient cassandraClient,
                                 HazelcastInstance hazelcastClient,
                                 MetricsFactory metricsFactory,
                                 EventListener<T> eventListener,
                                 ListeningExecutorService executorService) {

        super(clazz, cassandraClient, metricsFactory, executorService);

        this.eventListener = eventListener;

        /**
         * Initialize dataGrid
         */
        dataGrid = hazelcastClient.<Long, T>getMap(getMapName(clazz));
    }

    public CacheableDataProvider(Class<T> clazz,
                                 CassandraClient cassandraClient,
                                 HazelcastInstance hazelcastClient,
                                 MetricsFactory metricsFactory,
                                 ListeningExecutorService executorService) {

        super(clazz, cassandraClient, metricsFactory, executorService);

        this.eventListener = new EventListener<T>() {
            @Override
            public void onEntrySave(T entry) {

            }

            @Override
            public void onEntryRemove(T entry) {

            }
        };

        /**
         * Initialize dataGrid
         */
        dataGrid = hazelcastClient.<Long, T>getMap(getMapName(clazz));
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
            }, getExecutorService());

            return findFuture;
        }, getExecutorService());

        monitorFuture(time, findEntityFuture);

        return findEntityFuture;
    }

    @Override
    public ListenableFuture<Boolean> saveAsync(@NotNull T entity) {
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_SAVE).time();

        Long cacheKey = entity.getCacheKey() == null ? buildCacheKey(entity) : entity.getCacheKey();

        ICompletableFuture<T> future = (ICompletableFuture<T>) getMap().putAsync(cacheKey, entity);

        SettableFuture<Boolean> saveFuture = SettableFuture.<Boolean>create();

        future.andThen(new ExecutionCallback<T>() {
            @Override
            public void onResponse(T response) {
                saveFuture.set(true);
            }

            @Override
            public void onFailure(Throwable t) {
                saveFuture.setException(t);
            }
        }, getExecutorService());

        monitorFuture(time, saveFuture, result -> {
            ListenableFuture<Boolean> saveAsyncFuture = super.saveAsync(entity);

            getExecutorService().submit(() -> eventListener.onEntrySave(entity));

            return saveAsyncFuture;
        });

        return saveFuture;
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

            getExecutorService().submit(() -> eventListener.onEntryRemove(entity));

            return removeFuture;
        });

        return deleteFuture;
    }

    /**
     * Execute function on entry
     *
     * @param entry          Entry
     * @param entryProcessor User Function
     *
     * @return Future for current operation
     */
    protected ListenableFuture<Boolean> executeOnEntry(@NotNull T entry, @NotNull EntryProcessor<Long, T> entryProcessor) {
        Long entityId = entry.getCacheKey() == null ? buildCacheKey(entry) : entry.getCacheKey();

        ICompletableFuture<Boolean> completableFuture = (ICompletableFuture<Boolean>) getMap().submitToKey(entityId, entryProcessor);

        SettableFuture<Boolean> responseFuture = SettableFuture.<Boolean>create();

        completableFuture.andThen(new ExecutionCallback<Boolean>() {
            @Override
            public void onResponse(Boolean response) {
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

    protected final long buildCacheKey(@NotNull T entity) {
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_CREATE_KEY).time();

        int primaryKeysSize = getEntityMetadata().getPrimaryKeysSize();

        List<Object> keys = new ArrayList<>();

        for (int i = 0; i < primaryKeysSize; i++) {
            Optional<PrimaryKeyMetadata> primaryKey = getEntityMetadata().getPrimaryKey(i);

            if (primaryKey.isPresent()) {
                PrimaryKeyMetadata primaryKeyMetadata = primaryKey.get();

                Method readMethod = primaryKeyMetadata.getReadMethod();

                try {
                    Object value = readMethod.invoke(entity);
                    keys.add(value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    LOGGER.warn(String.format("Can't invoke read method: %s#%s", entity.getClass(), readMethod.getName()), e);
                }
            }
        }

        long cacheKey = buildCacheKey(keys);

        time.stop();

        return cacheKey;
    }

    /**
     * Build cache key
     *
     * @param keys Entity keys
     *
     * @return Cache key
     */
    protected final long buildCacheKey(Object... keys) {
        ArrayList<Object> keyList = new ArrayList<>();

        Collections.addAll(keyList, keys);

        return buildCacheKey(keyList);
    }

    private long buildCacheKey(List<Object> keys) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);

            for (Object key : keys) {
                objectOutputStream.writeObject(key);
            }

            return HASH_FUNCTION.hashBytes(out.toByteArray()).asLong();

        } catch (IOException e) {
            LOGGER.warn("Can't ");

            throw new CanNotBuildEntryCacheKey(getEntityClass(), e);
        }
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
        }, getExecutorService());

        return responseFuture;

    }

    protected enum MetricsType implements MetricsFactory.Type {
        CACHEABLE_DATA_PROVIDER_FIND,
        CACHEABLE_DATA_PROVIDER_SAVE,
        CACHEABLE_DATA_PROVIDER_CREATE_KEY,
        CACHEABLE_DATA_PROVIDER_HITS,
        CACHEABLE_DATA_PROVIDER_REMOVE, CACHEABLE_DATA_GET_FROM_DATA_GRID;

    }
}
