package com.fnklabs.draenei.orm;

import com.codahale.metrics.Timer;
import com.fnklabs.draenei.MetricsFactory;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class CacheableDataProvider<Entry extends Serializable> extends DataProvider<Entry> {

    public static final Logger LOGGER = LoggerFactory.getLogger(CacheableDataProvider.class);

    private final IgniteCache<Long, Entry> cache;

    public CacheableDataProvider(@NotNull Class<Entry> clazz,
                                 @NotNull CassandraClientFactory cassandraClient,
                                 @NotNull Ignite ignite,
                                 @NotNull MetricsFactory metricsFactory) {

        super(clazz, cassandraClient, metricsFactory);

        cache = ignite.getOrCreateCache(getCacheConfiguration());
    }

    @Override
    public ListenableFuture<Entry> findOneAsync(Object... keys) {
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_FIND).time();

        long cacheKey = buildCacheKey(keys);

        Entry entry = cache.withAsync().get(cacheKey);

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

    @Override
    public ListenableFuture<Boolean> saveAsync(@NotNull Entry entity) {
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_PUT_TO_CACHE).time();

        long cacheKey = buildCacheKey(entity);

        cache.put(cacheKey, entity);

        time.stop();

        return super.saveAsync(entity);
    }

    @Override
    public ListenableFuture<Boolean> removeAsync(@NotNull Entry entity) {

        Timer.Context timer = getMetricsFactory().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_REMOVE_FROM_CACHE).time();

        long key = buildCacheKey(entity);

        boolean remove = cache.remove(key);

        timer.stop();

        if (!remove) {
            return Futures.immediateFuture(false);
        }

        return super.removeAsync(entity);
    }

    /**
     * Return ignite cache configuration
     *
     * @return CacheConfiguration instance
     */
    @NotNull
    protected CacheConfiguration<Long, Entry> getCacheConfiguration() {
        CacheConfiguration<Long, Entry> cacheCfg = new CacheConfiguration<>(getMapName(getEntityClass()));
        cacheCfg.setBackups(1);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setOffHeapMaxMemory(0);
        cacheCfg.setReadThrough(false);
        cacheCfg.setWriteThrough(false);
        cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(10000));
        return cacheCfg;
    }

    @NotNull
    protected String getMapName() {
        return cache.getName();
    }

    @NotNull
    private String getMapName(Class<Entry> clazz) {
        return StringUtils.lowerCase(clazz.getName());
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
