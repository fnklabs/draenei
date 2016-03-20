package com.fnklabs.draenei.analytics;

import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.collect.Range;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class AnalyticsUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsUtils.class);


    /**
     * Scan storage
     * <p>
     * Will generate token range and execute tasks to load data from cassandra by token range
     *
     * @param analyticsContext Analytics context instance for retrieving distributed executor service and cassandra factory
     * @param <ValueIn>        Entity class type
     *
     * @return Total processed entities
     */
    @NotNull
    public static <ValueIn extends Serializable> Integer scanStorage(@NotNull AnalyticsContext analyticsContext, @NotNull DataProviderRangeScanFactory<ValueIn> scanFactory) {
        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.scan_storage");

        CassandraClient cassandraClient = analyticsContext.getCassandraClientFactory().create();
        Collection<Range<Long>> ranges = CassandraUtils.splitRing(cassandraClient);

        List<DataProviderRangeScan<ValueIn>> calls = ranges.stream()
                                                           .map(range -> scanFactory.create(range.lowerEndpoint(), range.upperEndpoint()))
                                                           .collect(Collectors.toList());

        ClusterGroup clusterGroup = analyticsContext.getIgnite()
                                                    .cluster()
                                                    .forServers();

        Integer loadedDocuments = analyticsContext.getIgnite()
                                                  .compute(clusterGroup)
                                                  .call(calls, new ScanStorageReducer());

        timer.stop();

        LOGGER.warn("Complete to scan storage data in {}. Processed items: {} ", timer, loadedDocuments);

        return loadedDocuments;
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
    public static <Key extends Serializable, ValueIn, MapOutput extends Serializable, ReducerResult extends Serializable> ReducerResult compute(@NotNull AnalyticsContext analyticsContext,
                                                                                                                                                @NotNull MapReduceTask<Key, ValueIn, MapOutput, ReducerResult> mapReduceTask) {

        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.compute");

        ClusterGroup clusterGroup = analyticsContext.getIgnite()
                                                    .cluster()
                                                    .forServers();


        ReducerResult execute = analyticsContext.getIgnite()
                                                .compute(clusterGroup)
                                                .execute(mapReduceTask, null);

        timer.stop();

        LOGGER.debug("Complete compute operation in {}", timer);

        return execute;
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
        cacheCfg.setOffHeapMaxMemory(5L * 1024L * 1024L * 1024L);
        return cacheCfg;
    }
}
