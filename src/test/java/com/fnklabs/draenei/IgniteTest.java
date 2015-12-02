package com.fnklabs.draenei;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.*;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.UUID;
import java.util.function.Consumer;

public class IgniteTest implements Serializable {
    @Test
    public void testExecution() throws Exception {
        LoggerFactory.getLogger(getClass()).debug("Is client: {}", Ignition.isClientMode());


        Ignite ignite = Ignition.start(getIgniteConfiguration());
        Ignite secondIgnite = Ignition.start(getIgniteConfiguration());

        ignite.compute(ignite.cluster()).broadcast(new IgniteTask());

        IgniteCompute compute = ignite.compute(ignite.cluster().forServers());

        compute.broadcast(new IgniteTask());

    }

    @Test
    public void testCache() throws UnknownHostException, InterruptedException {
        LoggerFactory.getLogger(getClass()).debug("Is client: {}", Ignition.isClientMode());


        Ignite firstIgnite = Ignition.start(getIgniteConfiguration());
        Ignite secondIgnite = Ignition.start(getIgniteConfiguration());


        IgniteCache<String, Long> firstCache = firstIgnite.getOrCreateCache(getCacheConfiguration());
        IgniteCache<String, Long> secondCache = secondIgnite.getOrCreateCache(getCacheConfiguration());


        firstCache.put("aa", 1L);

        secondCache.put("a", 1L);
        firstCache.put("a", 2L);
        secondCache.put("b", 2L);
        secondCache.put("c", 4L);

        Assert.assertEquals(2, secondCache.get("b").longValue());
        Assert.assertEquals(2, secondCache.get("a").longValue());
        Assert.assertNull(secondCache.get("k"));


        QueryCursor<Cache.Entry<String, Long>> query = secondCache.query(new ScanQuery<String, Long>(new IgniteBiPredicate<String, Long>() {
            @Override
            public boolean apply(String s, Long aLong) {
                return true;
            }
        }));

        query.forEach(new Consumer<Cache.Entry<String, Long>>() {
            @Override
            public void accept(Cache.Entry<String, Long> stringLongEntry) {
                LoggerFactory.getLogger(getClass()).debug("Result {}:{}", stringLongEntry.getKey(), stringLongEntry.getValue());
            }
        });

    }

    @Test
    public void testQuery() throws Exception {
        Ignite ignite = Ignition.start(getIgniteConfiguration());
        IgniteCache<Integer, String> cache = ignite.getOrCreateCache("mycache");

        // Create new continuous query.
        ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

        // Optional initial query to select all keys greater than 10.
        qry.setInitialQuery(new ScanQuery<Integer, String>((k, v) -> k > 10));

        // Callback that is called locally when update notifications are received.
        qry.setLocalListener((evts) ->
                evts.forEach(e -> System.out.println("key=" + e.getKey() + ", val=" + e.getValue())));

        // This filter will be evaluated remotely on all nodes.
        // Entry that pass this filter will be sent to the caller.
        qry.setRemoteFilter(e -> e.getKey() > 10);

        // Execute query.
        try (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {
            // Iterate through existing data stored in cache.
            for (Cache.Entry<Integer, String> e : cur)
                System.out.println("key=" + e.getKey() + ", val=" + e.getValue());

            // Add a few more keys and watch a few more query notifications.
            for (int i = 5; i < 15; i++)
                cache.put(i, Integer.toString(i));
        }


    }

    @NotNull
    private CacheConfiguration<String, Long> getCacheConfiguration() {
        CacheConfiguration<String, Long> cacheCfg = new CacheConfiguration<>("test");
        cacheCfg.setBackups(1);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setOffHeapMaxMemory(0);
        cacheCfg.setCacheStoreFactory(new CacheStoreFactory());
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(1));

        cacheCfg.addCacheEntryListenerConfiguration(new TestCacheEntryListenerConfiguration());

        return cacheCfg;
    }

    @NotNull
    private IgniteConfiguration getIgniteConfiguration() throws UnknownHostException {
        HashMap<String, Object> userProperties = new HashMap<>();
        userProperties.put("ROLE", "worker");

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setGridName(InetAddress.getLocalHost().getHostName() + " - " + UUID.randomUUID().toString());
        cfg.setClientMode(false);
        cfg.setGridLogger(new Slf4jLogger(LoggerFactory.getLogger(Slf4jLogger.class)));
        cfg.setUserAttributes(userProperties);
        cfg.setMarshaller(new OptimizedMarshaller());

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        commSpi.setSlowClientQueueLimit(1000);

        cfg.setCommunicationSpi(commSpi);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setClientReconnectDisabled(false);
        discoSpi.setForceServerMode(false);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();

        discoSpi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    private static class TestCacheEntryListenerConfiguration implements CacheEntryListenerConfiguration<String, Long> {
        @Override
        public Factory<CacheEntryListener<? super String, ? super Long>> getCacheEntryListenerFactory() {
            return new CacheEntryListenerFactory();
        }

        @Override
        public boolean isOldValueRequired() {
            return true;
        }

        @Override
        public Factory<CacheEntryEventFilter<? super String, ? super Long>> getCacheEntryEventFilterFactory() {
            return null;
        }

        @Override
        public boolean isSynchronous() {
            return false;
        }
    }

    private static class CacheEntryListenerFactory implements Factory<CacheEntryListener<? super String, ? super Long>> {
        @Override
        public CacheEntryListener<String, Long> create() {
            return new TestCacheEntryListener();
        }
    }

    private static class TestCacheEntryListener implements CacheEntryCreatedListener<String, Long>, CacheEntryUpdatedListener<String, Long>, CacheEntryRemovedListener<String, Long>, CacheEntryExpiredListener<String, Long> {


        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends Long>> cacheEntryEvents) throws CacheEntryListenerException {
            LoggerFactory.getLogger(getClass()).debug("onCreated: {}", cacheEntryEvents);
        }

        @Override
        public void onExpired(Iterable<CacheEntryEvent<? extends String, ? extends Long>> cacheEntryEvents) throws CacheEntryListenerException {
            LoggerFactory.getLogger(getClass()).debug("onExpired: {}", cacheEntryEvents);
        }

        @Override
        public void onRemoved(Iterable<CacheEntryEvent<? extends String, ? extends Long>> cacheEntryEvents) throws CacheEntryListenerException {
            LoggerFactory.getLogger(getClass()).debug("onRemoved: {}", cacheEntryEvents);
        }

        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends String, ? extends Long>> cacheEntryEvents) throws CacheEntryListenerException {
            LoggerFactory.getLogger(getClass()).debug("onUpdated: {}", cacheEntryEvents);
        }
    }


}
