package com.fnklabs.draenei.orm;

import com.codahale.metrics.Timer;
import com.fnklabs.draenei.MetricsFactory;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;

public class CacheableDataProvider<Entry extends Serializable> extends DataProvider<Entry> {

    public static final Logger LOGGER = LoggerFactory.getLogger(CacheableDataProvider.class);

    private final IgniteCache<Long, Entry> cache;

    public CacheableDataProvider(@NotNull Class<Entry> clazz,
                                 @NotNull CassandraClientFactory cassandraClient,
                                 @NotNull Ignite ignite,
                                 @NotNull ExecutorService executorService,
                                 @NotNull MetricsFactory metricsFactory) {

        super(clazz, cassandraClient, executorService, metricsFactory);

        cache = ignite.getOrCreateCache(getCacheConfiguration());
    }

    public CacheableDataProvider(@NotNull Class<Entry> clazz, @NotNull CassandraClientFactory cassandraClient, @NotNull Ignite ignite, @NotNull MetricsFactory metricsFactory) {
        super(clazz, cassandraClient, metricsFactory);

        cache = ignite.getOrCreateCache(getCacheConfiguration());
    }

    @Override
    public ListenableFuture<Entry> findOneAsync(Object... keys) {
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_FIND).time();

        long cacheKey = buildHashCode(keys);

        Entry entry = cache.get(cacheKey);

        if (entry != null) {
            getMetricsFactory().getCounter(MetricsType.CACHEABLE_DATA_PROVIDER_HITS).inc();

            return Futures.immediateFuture(entry);
        }

        // try to load entity from DB
        ListenableFuture<Entry> findFuture = super.findOneAsync(keys);

        Futures.addCallback(findFuture, new FutureCallback<Entry>() {
            @Override
            public void onSuccess(Entry result) {
                if (result != null) {
                    cache.put(cacheKey, result);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.warn("Can't get entry from DB", t);
            }
        });

        monitorFuture(time, findFuture);

        return findFuture;
    }

    /**
     * Execute on entry cache
     *
     * @param entry          Entry on which will be executed entry processor
     * @param entryProcessor Entry processor that must be executed
     * @param <ReturnValue>  ClassType
     *
     * @return Return value from entry processor
     */
    public <ReturnValue> ReturnValue execute(@NotNull Entry entry, @NotNull CacheEntryProcessor<Long, Entry, ReturnValue> entryProcessor) {
        return cache.invoke(buildHashCode(entry), entryProcessor);
    }

    @Override
    public ListenableFuture<Boolean> saveAsync(@NotNull Entry entity) {
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_PUT_TO_CACHE).time();

        long cacheKey = buildHashCode(entity);

        return Futures.transform(super.saveAsync(entity), (Boolean result) -> {
            time.stop();
            if (result) {
                cache.put(cacheKey, entity);
            }

            return result;
        });
    }

    public ListenableFuture<Boolean> removeAsync(@NotNull Entry entity) {

        Timer.Context timer = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_REMOVE_FROM_CACHE).time();

        long key = buildHashCode(entity);

        return Futures.transform(super.removeAsync(entity), (Boolean result) -> {
            timer.stop();

            return result && cache.remove(key);
        });
    }

    /**
     * Return ignite cache configuration
     *
     * @return CacheConfiguration instance
     */
    @NotNull
    public CacheConfiguration<Long, Entry> getCacheConfiguration() {
        return CacheUtils.getDefaultCacheConfiguration(getEntityClass());

    }

    protected IgniteCache<Long, Entry> getCache() {
        return cache;
    }

    @NotNull
    protected String getMapName() {
        return cache.getName();
    }

    @NotNull
    private String getMapName(Class<Entry> clazz) {
        return CacheUtils.getCacheName(clazz);
    }

    protected enum MetricsType implements MetricsFactory.Type {
        CACHEABLE_DATA_PROVIDER_FIND,
        CACHEABLE_DATA_PROVIDER_PUT_TO_CACHE,
        CACHEABLE_DATA_PROVIDER_CREATE_KEY,
        CACHEABLE_DATA_PROVIDER_HITS,
        CACHEABLE_DATA_PROVIDER_REMOVE_FROM_CACHE,
        CACHEABLE_DATA_GET_FROM_DATA_GRID,
        CACHEABLE_DATA_PROVIDER_PUT_ASYNC;

    }
}
