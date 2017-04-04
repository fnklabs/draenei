package com.fnklabs.draenei.analytics;

import com.datastax.driver.core.TokenRange;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.base.Verify;
import com.google.common.net.HostAndPort;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


/**
 * Analytics context provide basic analytics functionality
 */
public class AnalyticsContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsContext.class);

    /**
     * Cassandra client factory
     */

    private final CassandraClient cassandraClientFactory;

    /**
     * Ignite instance
     */

    private final Ignite ignite;

    /**
     * Construct analytics context
     *
     * @param cassandraClient Cassandra client instance
     * @param ignite          Ignite instance
     */
    public AnalyticsContext(CassandraClient cassandraClient, Ignite ignite) {
        this.cassandraClientFactory = cassandraClient;
        this.ignite = ignite;
    }

    /**
     * Split range scan task for provided keyspace by cassandra hosts from which data can be read
     *
     * @param keyspace        Keyspace
     * @param cassandraClient Cassandra client instance
     *
     * @return Map of cassandra hosts and owned token ranges
     */
    public static Map<TokenRange, ClusterNode> splitRangeScanTask(String keyspace, CassandraClient cassandraClient, List<ClusterNode> subgrid) {
        Map<HostAndPort, Integer> dataOwnerStatistic = new HashMap<>();

        Map<TokenRange, ClusterNode> result = cassandraClient.getTokensOwner(keyspace)
                                                             .entrySet()
                                                             .stream()
                                                             .collect(
                                                                     Collectors.toMap(
                                                                             Map.Entry::getKey,
                                                                             entry -> {

                                                                                 ClusterNode clusterNode = getLessLoadedNode(subgrid, dataOwnerStatistic, entry.getValue());

                                                                                 LOGGER.debug(
                                                                                         "Nearest node for {} is {}",
                                                                                         entry,
                                                                                         clusterNode.addresses()
                                                                                 );

                                                                                 return clusterNode;
                                                                             }
                                                                     )
                                                             );
        LOGGER.debug("Split result: {} Stat", result, dataOwnerStatistic);


        return result;
    }

    protected static ClusterNode getLessLoadedNode(List<ClusterNode> subgrid,
                                                   Map<HostAndPort, Integer> dataOwnerStatistic,
                                                   Set<HostAndPort> dataOwner) {
        Verify.verify(!subgrid.isEmpty(), "Subgrid can't be empty");
        Verify.verify(!dataOwner.isEmpty(), "Cassandra nodes collection can't be empty");

        Optional<HostAndPort> lessLoadedDataNode = dataOwner.stream()
                                                            .sorted((left, right) -> {
                                                                return Integer.compare(dataOwnerStatistic.getOrDefault(left, 0), dataOwnerStatistic.getOrDefault(right, 0));
                                                            })
                                                            .limit(1)
                                                            .findFirst();

        //  Update statistic
        lessLoadedDataNode.ifPresent(host -> {
            dataOwnerStatistic.compute(host, (key, value) -> value == null ? 1 : value + 1);
        });

        Optional<ClusterNode> clusterNode = lessLoadedDataNode.flatMap(host -> {
            return subgrid.stream()
                          .filter(node -> node.addresses().contains(host.getHostText()))
                          .findFirst();
        });

        return clusterNode.orElse(subgrid.stream().findAny().get());
    }

    private static String getJobName(ComputeTaskAdapter rangeScanJobFactory) {
        String className = rangeScanJobFactory.getClass()
                                              .getName()
                                              .toLowerCase();

        return String.format("job-%s-%s", className, UUID.randomUUID());
    }

    /**
     * Get default cache configuration for specified entity class
     *
     * @return Cache Configuration for specified entity class
     */
    public <Key, Entry> CacheConfiguration<Key, Entry> getCacheConfiguration(String cacheName) {
        CacheConfiguration<Key, Entry> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setBackups(0);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setReadThrough(false);
        cacheCfg.setWriteThrough(false);
        cacheCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new FifoEvictionPolicy(1000));
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setOffHeapMaxMemory(0);
        return cacheCfg;
    }

    /**
     * Execute compute operation with MR paradigm
     *
     * @param <RangeOutputValue> Entity class type
     *
     * @return Reduce result
     */

    public <Entity,
            RangeScanOutputKey,
            RangeOutputValue,
            RangeScanCombinerValue,
            MapOutputKey,
            MapOutputValue,
            MapCombinerValue,
            ReducerOutputKey,
            ReducerOutputValue,
            ReducerCombinerValue> CacheConfiguration<ReducerOutputKey, ReducerCombinerValue> compute(RangeScanJobFactory<Entity, RangeScanOutputKey, RangeOutputValue, RangeScanCombinerValue> rangeScanJobFactory,
                                                                                                     MapFactory<RangeScanOutputKey, RangeScanCombinerValue, MapOutputKey, MapOutputValue, MapCombinerValue> mapFactory,
                                                                                                     ReducerFactory<MapOutputKey, MapCombinerValue, ReducerOutputKey, ReducerOutputValue, ReducerCombinerValue> reducerFactory) {

        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.compute");


        CacheConfiguration<RangeScanOutputKey, RangeScanCombinerValue> scanResultConfig = scanStorage(rangeScanJobFactory);

        CacheConfiguration<MapOutputKey, MapCombinerValue> mapDataResultConfig = getCacheConfiguration(getJobName(mapFactory));
        CacheConfiguration<ReducerOutputKey, ReducerCombinerValue> reducerResultConfig = getCacheConfiguration(getJobName(reducerFactory));

        try {
            map(mapFactory, mapDataResultConfig, scanResultConfig);

            getIgnite().getOrCreateCache(scanResultConfig).clear();
            getIgnite().getOrCreateCache(scanResultConfig).destroy(); // close data cache

            reduce(reducerFactory, reducerResultConfig, mapDataResultConfig);

            getIgnite().getOrCreateCache(mapDataResultConfig).clear();
            getIgnite().getOrCreateCache(mapDataResultConfig).destroy(); // close map result

            LOGGER.debug("Complete compute operation in {}", timer);


        } catch (Exception e) {
            LOGGER.warn("Can't complete compute operation", e);


            LOGGER.warn("Destroy output cache {}, {}, {}", reducerResultConfig.getName());

            getIgnite().getOrCreateCache(reducerResultConfig).clear();
            getIgnite().getOrCreateCache(reducerResultConfig).destroy();

        } finally {
            ignite.getOrCreateCache(scanResultConfig).clear();
            ignite.getOrCreateCache(scanResultConfig).destroy();
            ignite.getOrCreateCache(mapDataResultConfig).clear();
            ignite.getOrCreateCache(mapDataResultConfig).destroy();

            timer.stop();
        }

        return reducerResultConfig;
    }

    /**
     * Execute compute operation with MR paradigm
     *
     * @param <RangeOutputValue> Entity class type
     *
     * @return Reduce result
     */

    public <Entity,
            RangeScanOutputKey,
            RangeOutputValue,
            RangeScanCombinerValue,
            MapOutputKey,
            MapOutputValue,
            MapCombinerValue> CacheConfiguration<MapOutputKey, MapCombinerValue> compute(
            RangeScanJobFactory<Entity, RangeScanOutputKey, RangeOutputValue, RangeScanCombinerValue> rangeScanJobFactory,
            MapFactory<RangeScanOutputKey, RangeScanCombinerValue, MapOutputKey, MapOutputValue, MapCombinerValue> mapFactory
    ) {

        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.compute");


        CacheConfiguration<MapOutputKey, MapCombinerValue> mapDataResultConfig = getCacheConfiguration(getJobName(mapFactory));

        try {
            CacheConfiguration<RangeScanOutputKey, RangeScanCombinerValue> scanResultConfig = scanStorage(rangeScanJobFactory);

            map(mapFactory, mapDataResultConfig, scanResultConfig);

            getIgnite().getOrCreateCache(scanResultConfig).clear();
            getIgnite().getOrCreateCache(scanResultConfig).destroy(); // close data cache

            LOGGER.debug("Complete compute operation in {}", timer);

            return mapDataResultConfig;
        } catch (Exception e) {
            LOGGER.warn("Can't complete compute operation", e);

            LOGGER.warn("Destroy output cache {}", mapDataResultConfig.getName(), e);

            ignite.getOrCreateCache(mapDataResultConfig).clear();
            ignite.getOrCreateCache(mapDataResultConfig).destroy();

            return mapDataResultConfig;
        } finally {
            timer.stop();
        }
    }

    public <InputKey, InputValue, OutputKey, OutputValue, CombinerValue> CacheConfiguration<OutputKey, CombinerValue> map(
            MapFactory<InputKey, InputValue, OutputKey, OutputValue, CombinerValue> mapFactory,
            CacheConfiguration<InputKey, InputValue> inputDataConfig
    ) {

        CacheConfiguration<OutputKey, CombinerValue> cacheConfiguration = getCacheConfiguration(getJobName(mapFactory));

        map(mapFactory, cacheConfiguration, inputDataConfig);

        return cacheConfiguration;
    }

    public <InputKey, InputValue, OutputKey, OutputValue, CombinerValue> Long map(
            MapFactory<InputKey, InputValue, OutputKey, OutputValue, CombinerValue> mapFactory,
            CacheConfiguration<OutputKey, CombinerValue> outputDataConfig,
            CacheConfiguration<InputKey, InputValue> inputDataConfig
    ) {
        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.map");

        try {
            Long mappedEntries = getIgnite().compute(getServers())
                                            .execute(mapFactory, new TransformationContext<>(inputDataConfig, outputDataConfig));


            LOGGER.debug("Mapped entries {} in {}", mappedEntries, timer);

            return mappedEntries;

        } catch (Exception e) {
            LOGGER.warn("Destroy output cache {}", outputDataConfig.getName(), e);

            getIgnite().getOrCreateCache(outputDataConfig).clear();
            getIgnite().getOrCreateCache(outputDataConfig).destroy();

            return 0L;
        } finally {
            timer.stop();
        }
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

    public <Entity, Key, Value, CombinerValue> CacheConfiguration<Key, CombinerValue> scanStorage(RangeScanJobFactory<Entity, Key, Value, CombinerValue> rangeScanJobFactory) {
        CacheConfiguration<Key, CombinerValue> cacheConfiguration = getCacheConfiguration(getJobName(rangeScanJobFactory));

        scanStorage(rangeScanJobFactory, cacheConfiguration);

        return cacheConfiguration;
    }

    /**
     * Scan storage
     * <p>
     * Will generate token range and execute tasks to load data from cassandra by token range
     *
     * @param <Entity> Entity class type
     */
    public <Entity, Key, Value, CombinerValue> void scanStorage(
            RangeScanJobFactory<Entity, Key, Value, CombinerValue> rangeScanJobFactory,
            CacheConfiguration<Key, CombinerValue> cacheConfiguration
    ) {

        try {
            Timer timer = MetricsFactory.getMetrics().getTimer("analytics.scan_storage");

            ClusterGroup clusterGroup = getServers();

            Integer loadedDocuments = getIgnite().compute(clusterGroup)
                                                 .execute(rangeScanJobFactory, cacheConfiguration);

            timer.stop();

            LOGGER.warn("Complete to scan storage data in {}. Processed items: {} ", timer, loadedDocuments);

        } catch (Exception e) {
            LOGGER.warn("Destroy output cache {}", cacheConfiguration.getName(), e);

            ignite.getOrCreateCache(cacheConfiguration).clear();
            ignite.getOrCreateCache(cacheConfiguration).destroy();
        }
    }

    public <InputKey, InputValue, OutputKey, OutputValue, CombinerValue> void reduce(
            ReducerFactory<InputKey, InputValue, OutputKey, OutputValue, CombinerValue> reducerFactory,
            CacheConfiguration<OutputKey, CombinerValue> outputDataConfig,
            CacheConfiguration<InputKey, InputValue> inputDataConfig
    ) {
        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.reduce");

        try {
            Long mappedEntries = getIgnite().compute(getServers())
                                            .execute(reducerFactory, new TransformationContext<>(inputDataConfig, outputDataConfig));


            LOGGER.debug("Reduced entries {} in {}", mappedEntries, timer);
        } catch (Exception e) {
            LOGGER.warn("Destroy output cache {}", outputDataConfig.getName(), e);

            getIgnite().getOrCreateCache(outputDataConfig).clear();
            getIgnite().getOrCreateCache(outputDataConfig).destroy();
        } finally {
            timer.stop();
        }
    }

    public Ignite getIgnite() {
        return ignite;
    }

    private CassandraClient getCassandraClient() {
        return cassandraClientFactory;
    }

    private ClusterGroup getServers() {
        return getIgnite().cluster()
                          .forServers();
    }
}
