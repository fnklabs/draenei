package com.fnklabs.draenei.orm;


import com.fnklabs.metrics.Timer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * DataProvider that working through cache layer
 * <p>
 * Current implementation doesn't guarantee full data consistency because it write data into persistent storage asynchronously in background and old record can
 * rewrite new record in  storage.
 * <p>
 * Ignite must be configured to process cache eventType: {@code org.apache.ignite.configuration.IgniteConfiguration#setIncludeEventTypes(org.apache.ignite.events.EventType.EVTS_CACHE)}
 *
 * @param <Entry> Entry class type
 */
public class CacheableDataProvider<Entry extends Serializable> extends DataProvider<Entry> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheableDataProvider.class);

    private final IgniteCache<List, Entry> cache;

    public CacheableDataProvider(Class<Entry> clazz,
                                 CassandraClientFactory cassandraClientFactory,
                                 Ignite ignite) {
        super(clazz, cassandraClientFactory);

        cache = ignite.getOrCreateCache(getCacheConfiguration());
    }

    @Override
    public ListenableFuture<Entry> findOneAsync(Object... keys) {
        return Futures.immediateFuture(findOne(keys));
    }

    @Override
    public Entry findOne(Object... keys) {
        try (Timer time = getMetrics().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_FIND.name())) {

            List cacheKey = Arrays.asList(keys);

            return cache.get(cacheKey);
        } catch (Exception e) {
            LOGGER.warn("Can't find entity", e);

            return null;
        }
    }

    @Override
    public List<Entry> find(Object... keys) {
        return super.find(keys);
    }

    /**
     * Execute entry processor on entry cache
     *
     * @param entry          Entry on which will be executed entry processor
     * @param entryProcessor Entry processor that must be executed
     * @param <ReturnValue>  ClassType
     *
     * @return Return value from entry processor
     */
    public <ReturnValue> ReturnValue executeOnEntry(Entry entry, CacheEntryProcessor<List, Entry, ReturnValue> entryProcessor) {
        List key = getPrimaryKeys(entry);

        return cache.invoke(key, entryProcessor);
    }

    /**
     * Put entity to cache, save to persistence storage operation will be executed in background
     *
     * @param entity Target entity
     *
     * @return Future for put to cache operation
     */
    @Override
    public ListenableFuture<Boolean> saveAsync(Entry entity) {
        Timer time = getMetrics().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_PUT_TO_CACHE.name());

        List cacheKey = getPrimaryKeys(entity);

        cache.put(cacheKey, entity);

        time.stop();

        return Futures.immediateFuture(true);
    }

    /**
     * Remove entity from cache, remove from persistence storage operation will be executed in background
     *
     * @param entity Target entity
     *
     * @return Future for remove from cache operation
     */
    public ListenableFuture<Boolean> removeAsync(Entry entity) {

        Timer timer = getMetrics().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_REMOVE_FROM_CACHE.name());

        List key = getPrimaryKeys(entity);

        cache.remove(key);

        timer.stop();

        return Futures.immediateFuture(true);
    }

    /**
     * Return ignite cache configuration
     *
     * @return CacheConfiguration instance
     */
    private CacheConfiguration<List, Entry> getCacheConfiguration() {
        CacheDaoFactory<Entry> factory = new CacheDaoFactory<>(cassandraClientFactory, getEntityClass());

        CacheConfiguration<List, Entry> defaultCacheConfiguration = CacheUtils.getDefaultCacheConfiguration(getEntityClass());
        defaultCacheConfiguration.setWriteThrough(true);
        defaultCacheConfiguration.setWriteBehindEnabled(true);
        defaultCacheConfiguration.setReadThrough(true);
        defaultCacheConfiguration.setCacheWriterFactory(factory);
        defaultCacheConfiguration.setCacheLoaderFactory(factory);

        return defaultCacheConfiguration;
    }

    private enum MetricsType {
        CACHEABLE_DATA_PROVIDER_FIND,
        CACHEABLE_DATA_PROVIDER_PUT_TO_CACHE,
        CACHEABLE_DATA_PROVIDER_HITS,
        CACHEABLE_DATA_PROVIDER_REMOVE_FROM_CACHE;
    }

}
