package com.fnklabs.draenei.orm;


import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.metrics.Timer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * DataProvider that working through cache layer
 * <p>
 * Current implementation doesn't guarantee full data consistency because it write data into persistent storage asynchronously in background and old record can rewrite new record
 * in  storage.
 * <p>
 * Ignite must be configured to process cache eventType: {@code org.apache.ignite.configuration.IgniteConfiguration#setIncludeEventTypes(org.apache.ignite.events.EventType.EVTS_CACHE)}
 *
 * @param <Entry> Entry class type
 */
public class CacheableDataProvider<Entry extends Serializable> extends DataProvider<Entry> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheableDataProvider.class);

    private final IgniteCache<Long, Entry> cache;

    public CacheableDataProvider(@NotNull Class<Entry> clazz,
                                 @NotNull CassandraClient cassandraClient,
                                 @NotNull Ignite ignite,
                                 @NotNull ExecutorService executorService) {
        super(clazz, cassandraClient, executorService);

        cache = ignite.getOrCreateCache(getCacheConfiguration());

        initializeEventListener(ignite);
    }

    @Override
    public ListenableFuture<Entry> findOneAsync(Object... keys) {
        Timer time = getMetrics().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_FIND.name());

        long cacheKey = buildHashCode(keys);

        Entry entry = cache.get(cacheKey);

        if (entry != null) {
            getMetrics().getCounter(MetricsType.CACHEABLE_DATA_PROVIDER_HITS.name()).inc();

            return Futures.immediateFuture(entry);
        }

        // try to load entity from DB
        ListenableFuture<Entry> findFuture = super.findOneAsync(keys);

        Futures.addCallback(findFuture, new FutureCallback<Entry>() {
            @Override
            public void onSuccess(Entry result) {
                if (result != null) {
                    cache.putIfAbsent(cacheKey, result);
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
    public Entry findOne(Object... keys) {
        Timer time = getMetrics().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_FIND.name());

        long cacheKey = buildHashCode(keys);

        Entry entry = cache.get(cacheKey);

        if (entry != null) {
            getMetrics().getCounter(MetricsType.CACHEABLE_DATA_PROVIDER_HITS.name()).inc();

            time.stop();

        } else {
            entry = super.findOne(keys);

            if (entry != null) {
                cache.put(cacheKey, entry);
            }
        }

        time.stop();

        return entry;
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
    public <ReturnValue> ReturnValue executeOnEntry(@NotNull Entry entry, @NotNull CacheEntryProcessor<Long, Entry, ReturnValue> entryProcessor) {
        long key = buildHashCode(entry);

        if (!cache.containsKey(key)) {
            List<Object> primaryKeys = getPrimaryKeys(entry);

            List<Entry> entries = super.fetch(primaryKeys);

            Optional<Entry> first = entries.stream().findFirst();

            if (first.isPresent()) {
                cache.putIfAbsent(key, first.get());
            }
        }

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
    public ListenableFuture<Boolean> saveAsync(@NotNull Entry entity) {
        Timer time = getMetrics().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_PUT_TO_CACHE.name());

        long cacheKey = buildHashCode(entity);

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
    public ListenableFuture<Boolean> removeAsync(@NotNull Entry entity) {

        Timer timer = getMetrics().getTimer(MetricsType.CACHEABLE_DATA_PROVIDER_REMOVE_FROM_CACHE.name());

        long key = buildHashCode(entity);

        cache.remove(key);

        timer.stop();

        return Futures.immediateFuture(true);
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

    @NotNull
    private String getMapName() {
        return cache.getName();
    }

    private void initializeEventListener(@NotNull Ignite ignite) {
        ignite.events()
              .localListen(
                      new LocalCacheEventListener(),
                      EventType.EVT_CACHE_OBJECT_EXPIRED,
                      EventType.EVT_CACHE_OBJECT_PUT,
                      EventType.EVT_CACHE_OBJECT_REMOVED
              );
    }

    private enum MetricsType {
        CACHEABLE_DATA_PROVIDER_FIND,
        CACHEABLE_DATA_PROVIDER_PUT_TO_CACHE,
        CACHEABLE_DATA_PROVIDER_HITS,
        CACHEABLE_DATA_PROVIDER_REMOVE_FROM_CACHE;

    }

    /**
     * LocalCache listener perform save and remove operation into persistence storage when retrieve event from cache
     */
    private class LocalCacheEventListener implements IgnitePredicate<Event> {

        @Override
        public boolean apply(Event event) {
            if (event instanceof CacheEvent) {
                CacheEvent cacheEvent = (CacheEvent) event;

                if (StringUtils.equals(getMapName(), cacheEvent.cacheName())) {

                    switch (cacheEvent.type()) {
                        case EventType.EVT_CACHE_OBJECT_EXPIRED:
                            CacheableDataProvider.super.saveAsync((Entry) cacheEvent.newValue());
                            break;
                        case EventType.EVT_CACHE_OBJECT_PUT:
                            CacheableDataProvider.super.saveAsync((Entry) cacheEvent.newValue());
                            break;
                        case EventType.EVT_CACHE_OBJECT_REMOVED:
                            CacheableDataProvider.super.removeAsync((Entry) cacheEvent.oldValue());
                            break;
                    }
                }
            }
            return true;
        }
    }
}
