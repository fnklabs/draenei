package com.fnklabs.draenei.analytics;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.TokenRange;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.base.Verify;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AnalyticsUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsUtils.class);

    /**
     * Execute compute operation with MR paradigm
     *
     * @param analyticsContext Analytics context instance for retrieving distributed executor service and cassandra factory
     * @param <ValueIn>        Entity class type
     *
     * @return Reduce result
     */
    @NotNull
    public static <Entity, Key, ValueIn, MapOutput, ReducerResult> ReducerResult compute(@NotNull AnalyticsContext analyticsContext,
                                                                                         @NotNull RangeScanJobFactory<Entity, Key, ValueIn> rangeScanJobFactory,
                                                                                         @NotNull MapReduceTaskFactory<Key, ValueIn, MapOutput, ReducerResult> mapReduceTaskFactory) {
        String jobName = rangeScanJobFactory.getClass()
                                            .toString()
                                            .toLowerCase();

        String cacheName = String.format("job-%s-%s", jobName, UUID.randomUUID());

        CacheConfiguration<Key, ValueIn> cacheConfiguration = AnalyticsUtils.getCacheConfiguration(cacheName);

        return compute(analyticsContext, rangeScanJobFactory, mapReduceTaskFactory, cacheConfiguration);
    }

    /**
     * Execute compute operation with MR paradigm
     *
     * @param analyticsContext Analytics context instance for retrieving distributed executor service and cassandra factory
     * @param <ValueIn>        Entity class type
     *
     * @return Reduce result
     */
    @NotNull
    public static <Entity, Key, ValueIn, MapOutput, ReducerResult> ReducerResult compute(@NotNull AnalyticsContext analyticsContext,
                                                                                         @NotNull RangeScanJobFactory<Entity, Key, ValueIn> rangeScanJobFactory,
                                                                                         @NotNull MapReduceTaskFactory<Key, ValueIn, MapOutput, ReducerResult> mapReduceTaskFactory,
                                                                                         @NotNull CacheConfiguration<Key, ValueIn> cacheConfiguration) {

        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.compute");

        try (IgniteCache<Key, ValueIn> cache = analyticsContext.getIgnite().getOrCreateCache(cacheConfiguration)) {
            Verify.verifyNotNull(cache);

            ClusterGroup clusterGroup = analyticsContext.getIgnite()
                                                        .cluster()
                                                        .forServers();

            rangeScanJobFactory.setCacheConfiguration(cacheConfiguration);

            Integer loadedEntries = scanStorage(analyticsContext, rangeScanJobFactory);

            LOGGER.debug("Loaded entries: {} Cache size: {}MB", loadedEntries, (float)cache.metrics().getOffHeapAllocatedSize() / 1024 / 1024);

            mapReduceTaskFactory.setCacheConfiguration(cacheConfiguration);

            ReducerResult result = analyticsContext.getIgnite()
                                                   .compute(clusterGroup)
                                                   .execute(mapReduceTaskFactory, null);


            LOGGER.debug("Complete compute operation in {}", timer);

            return result;
        } catch (IgniteException e) {
            LOGGER.warn("Can't complete compute operation", e);
            throw e;
        } finally {
            timer.stop();
        }
    }

    /**
     * Get default cache configuration for specified entity class
     *
     * @return Cache Configuration for specified entity class
     */
    public static <Key, Entry> CacheConfiguration<Key, Entry> getCacheConfiguration(String cacheName) {
        CacheConfiguration<Key, Entry> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setBackups(0);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setReadThrough(false);
        cacheCfg.setWriteThrough(false);
        cacheCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(10000));
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setOffHeapMaxMemory(4L * 1024L * 1024L * 1024L);
        return cacheCfg;
    }

    static Map<Host, Set<TokenRange>> splitRangeScanTask(@NotNull String keyspace, @NotNull CassandraClient cassandraClient) {
        Map<Host, Set<TokenRange>> result = new HashMap<>();

        cassandraClient.getTokenRangeOwners(keyspace)
                       .forEach((key, hosts) -> {
                           Optional<Host> lessLoadedNode = hosts.stream()
                                                                .sorted((o1, o2) -> Integer.compare(
                                                                        result.getOrDefault(o1, Collections.emptySet())
                                                                              .size(),
                                                                        result.getOrDefault(o2, Collections.emptySet())
                                                                              .size()
                                                                ))
                                                                .findFirst();

                           Host defaultValue = hosts.stream().findFirst().orElse(null);

                           Host host = lessLoadedNode.orElse(defaultValue);

                           result.compute(host, (node, tokens) -> {
                               if (tokens == null) {
                                   tokens = new HashSet<>();
                               }

                               tokens.add(key);

                               return tokens;
                           });

                       });

        LOGGER.debug("Split result: {}", result);

        return result;
    }

    /**
     * Scan storage
     * <p>
     * Will generate token range and execute tasks to load data from cassandra by token range
     *
     * @param analyticsContext Analytics context instance for retrieving distributed executor service and cassandra factory
     * @param <Entity>         Entity class type
     *
     * @return Total processed entities
     */
    @NotNull
    private static <Entity, Key, Value> Integer scanStorage(@NotNull AnalyticsContext analyticsContext, @NotNull RangeScanJobFactory<Entity, Key, Value> rangeScanJobFactory) {
        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.scan_storage");


        ClusterGroup clusterGroup = analyticsContext.getIgnite()
                                                    .cluster()
                                                    .forServers();

        Integer loadedDocuments = analyticsContext.getIgnite()
                                                  .compute(clusterGroup)
                                                  .execute(rangeScanJobFactory, null);

        timer.stop();

        LOGGER.warn("Complete to scan storage data in {}. Processed items: {} ", timer, loadedDocuments);

        return loadedDocuments;
    }


}
