package com.fnklabs.draenei.analytics;

import com.datastax.driver.core.TokenRange;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.net.HostAndPort;
import org.apache.ignite.Ignite;
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

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Analytics context provide basic analytics functionality
 */
public class AnalyticsContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsContext.class);

    /**
     * Cassandra client factory
     */
    @NotNull
    private final CassandraClient cassandraClientFactory;

    /**
     * Ignite instance
     */
    @NotNull
    private final Ignite ignite;

    /**
     * Construct analytics context
     *
     * @param cassandraClient Cassandra client instance
     * @param ignite          Ignite instance
     */
    public AnalyticsContext(@NotNull CassandraClient cassandraClient, @NotNull Ignite ignite) {
        this.cassandraClientFactory = cassandraClient;
        this.ignite = ignite;
    }

    /**
     * Get default cache configuration for specified entity class
     *
     * @return Cache Configuration for specified entity class
     */
    public static <Key, Entry> CacheConfiguration<Key, Entry> getCacheConfiguration(String cacheName) {
        CacheConfiguration<Key, Entry> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setBackups(1);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setReadThrough(false);
        cacheCfg.setWriteThrough(false);
        cacheCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(1000));
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setOffHeapMaxMemory(1L * 1024L * 1024L * 1024L);
        return cacheCfg;
    }

    /**
     * Split range scan task for provided keyspace by cassandra hosts from which data can be read
     *
     * @param keyspace        Keyspace
     * @param cassandraClient Cassandra client instance
     *
     * @return Map of cassandra hosts and owned token ranges
     */
    static Map<HostAndPort, Set<TokenRange>> splitRangeScanTask(@NotNull String keyspace, @NotNull CassandraClient cassandraClient) {
        Map<HostAndPort, Set<TokenRange>> result = cassandraClient.getTokenRangesByHost(keyspace)
                                                                  .entrySet()
                                                                  .stream()
                                                                  .collect(Collectors.toMap(
                                                                          Map.Entry::getKey,
                                                                          Map.Entry::getValue
                                                                  ));

        LOGGER.debug("Split result: {}", result);

        return result;
    }

    /**
     * Execute compute operation with MR paradigm
     *
     * @param <RangeOutputValue> Entity class type
     *
     * @return Reduce result
     */
    @NotNull
    public <Entity,
            RangeScanOutputKey,
            RangeOutputValue,
            RangeScanCombinerValue,
            MapOutputKey,
            MapOutputValue,
            MapCombinerValue,
            ReducerOutputKey,
            ReducerOutputValue,
            ReducerCombinerValue> IgniteCache<ReducerOutputKey, ReducerCombinerValue> compute(@NotNull RangeScanJobFactory<Entity, RangeScanOutputKey, RangeOutputValue, RangeScanCombinerValue> rangeScanJobFactory,
                                                                                              @NotNull MapFactory<RangeScanOutputKey, RangeScanCombinerValue, MapOutputKey, MapOutputValue, MapCombinerValue> mapFactory,
                                                                                              @NotNull ReducerFactory<MapOutputKey, MapCombinerValue, ReducerOutputKey, ReducerOutputValue, ReducerCombinerValue> reducerFactory) {

        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.compute");


        CacheConfiguration<RangeScanOutputKey, RangeScanCombinerValue> scanResultConfig = scanStorage(rangeScanJobFactory);
        CacheConfiguration<MapOutputKey, MapCombinerValue> mapDataResultConfig = getCacheConfiguration(getJobName(mapFactory));
        CacheConfiguration<ReducerOutputKey, ReducerCombinerValue> reducerResultConfig = getCacheConfiguration(getJobName(reducerFactory));

        try {
            map(scanResultConfig, mapDataResultConfig, mapFactory);

            getIgnite().getOrCreateCache(scanResultConfig).close(); // close data cache

            reduce(mapDataResultConfig, reducerResultConfig, reducerFactory);

            getIgnite().getOrCreateCache(mapDataResultConfig).close(); // close map result

            LOGGER.debug("Complete compute operation in {}", timer);

            return getIgnite().getOrCreateCache(reducerResultConfig);
        } catch (IgniteException e) {
            LOGGER.warn("Can't complete compute operation", e);
            throw e;
        } finally {
            ignite.getOrCreateCache(scanResultConfig).close();
            ignite.getOrCreateCache(mapDataResultConfig).close();

            timer.stop();
        }
    }

    /**
     * Execute compute operation with MR paradigm
     *
     * @param <RangeOutputValue> Entity class type
     *
     * @return Reduce result
     */
    @NotNull
    public <Entity,
            RangeScanOutputKey,
            RangeOutputValue,
            RangeScanCombinerValue,
            MapOutputKey,
            MapOutputValue,
            MapCombinerValue> IgniteCache<MapOutputKey, MapCombinerValue> compute(
            @NotNull RangeScanJobFactory<Entity, RangeScanOutputKey, RangeOutputValue, RangeScanCombinerValue> rangeScanJobFactory,
            @NotNull MapFactory<RangeScanOutputKey, RangeScanCombinerValue, MapOutputKey, MapOutputValue, MapCombinerValue> mapFactory
    ) {

        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.compute");


        try {
            CacheConfiguration<RangeScanOutputKey, RangeScanCombinerValue> scanResultConfig = scanStorage(rangeScanJobFactory);
            CacheConfiguration<MapOutputKey, MapCombinerValue> mapDataResultConfig = getCacheConfiguration(getJobName(mapFactory));

            map(scanResultConfig, mapDataResultConfig, mapFactory);

            getIgnite().getOrCreateCache(scanResultConfig).close(); // close data cache

            LOGGER.debug("Complete compute operation in {}", timer);

            return getIgnite().getOrCreateCache(mapDataResultConfig);
        } catch (IgniteException e) {
            LOGGER.warn("Can't complete compute operation", e);
            throw e;
        } finally {
            timer.stop();
        }
    }

    @NotNull
    public Ignite getIgnite() {
        return ignite;
    }

    public <InputKey, InputValue, OutputKey, OutputValue, CombinerValue> Long map(
            @NotNull CacheConfiguration<InputKey, InputValue> inputDataConfig,
            @NotNull CacheConfiguration<OutputKey, CombinerValue> outputDataConfig,
            @NotNull MapFactory<InputKey, InputValue, OutputKey, OutputValue, CombinerValue> mapFactory
    ) {
        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.map");

        try {
            Long mappedEntries = getIgnite().compute(getServers())
                                            .execute(mapFactory, new TransformationContext<>(inputDataConfig, outputDataConfig));


            LOGGER.debug("Mapped entries {} in {}", mappedEntries, timer);

            return mappedEntries;

        } finally {
            timer.stop();
        }
    }


    public <InputKey, InputValue, OutputKey, OutputValue, CombinerValue> IgniteCache<OutputKey, CombinerValue> map(
            @NotNull CacheConfiguration<InputKey, InputValue> inputDataConfig,
            @NotNull MapFactory<InputKey, InputValue, OutputKey, OutputValue, CombinerValue> mapFactory
    ) {

        CacheConfiguration<OutputKey, CombinerValue> cacheConfiguration = getCacheConfiguration(getJobName(mapFactory.getClass()));

        map(inputDataConfig, cacheConfiguration, mapFactory);

        return getIgnite().getOrCreateCache(cacheConfiguration);
    }

    /**
     * Scan storage
     * <p>
     * Will generate token range and execute tasks to load data from cassandra by token range
     *
     * @param <Entity> Entity class type
     *
     * @return Total processed entities
     */
    @NotNull
    public <Entity, Key, Value, CombinerValue> CacheConfiguration<Key, CombinerValue> scanStorage(@NotNull RangeScanJobFactory<Entity, Key, Value, CombinerValue> rangeScanJobFactory) {
        CacheConfiguration<Key, CombinerValue> cacheConfiguration = getCacheConfiguration(getJobName(rangeScanJobFactory));

        return scanStorage(rangeScanJobFactory, cacheConfiguration);
    }

    /**
     * Scan storage
     * <p>
     * Will generate token range and execute tasks to load data from cassandra by token range
     *
     * @param <Entity> Entity class type
     *
     * @return Total processed entities
     */
    @NotNull
    public <Entity, Key, Value, CombinerValue> CacheConfiguration<Key, CombinerValue> scanStorage(@NotNull RangeScanJobFactory<Entity, Key, Value, CombinerValue> rangeScanJobFactory,
                                                                                                  @NotNull CacheConfiguration<Key, CombinerValue> cacheConfiguration) {
        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.scan_storage");

        ClusterGroup clusterGroup = getServers();

        Integer loadedDocuments = getIgnite().compute(clusterGroup)
                                             .execute(rangeScanJobFactory, cacheConfiguration);

        timer.stop();

        LOGGER.warn("Complete to scan storage data in {}. Processed items: {} ", timer, loadedDocuments);

        return cacheConfiguration;
    }

    @NotNull
    private CassandraClient getCassandraClient() {
        return cassandraClientFactory;
    }

    private <InputKey, InputValue, OutputKey, OutputValue, CombinerValue> void reduce(
            @NotNull CacheConfiguration<InputKey, InputValue> inputDataConfig,
            @NotNull CacheConfiguration<OutputKey, CombinerValue> outputDataConfig,
            @NotNull ReducerFactory<InputKey, InputValue, OutputKey, OutputValue, CombinerValue> reducerFactory
    ) {
        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.reduce");

        try {
            Long mappedEntries = getIgnite().compute(getServers())
                                            .execute(reducerFactory, new TransformationContext<>(inputDataConfig, outputDataConfig));


            LOGGER.debug("Reduced entries {} in {}", mappedEntries, timer);
        } finally {
            timer.stop();
        }
    }

    private ClusterGroup getServers() {
        return getIgnite().cluster()
                          .forServers();
    }

    @NotNull
    private static String getJobName(@NotNull Object rangeScanJobFactory) {
        String className = rangeScanJobFactory.getClass()
                                              .toString()
                                              .toLowerCase();
        return String.format("job-%s-%s", className, UUID.randomUUID());
    }
}
