package com.fnklabs.draenei;

import com.fnklabs.draenei.orm.annotations.Enumerated;
import com.google.common.base.MoreObjects;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.*;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Ignore
public class IgniteTest implements Serializable {
    private static AtomicInteger igniteNumber = new AtomicInteger(1);

    @NotNull
    public static IgniteConfiguration getIgniteConfiguration() throws UnknownHostException {
        HashMap<String, Object> userProperties = new HashMap<>();
        userProperties.put("ROLE", "worker");

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIncludeEventTypes(org.apache.ignite.events.EventType.EVTS_CACHE);
        cfg.setGridName("IgniteNode[" + igniteNumber.getAndIncrement() + "]");
        cfg.setClientMode(false);
        cfg.setGridLogger(new Slf4jLogger(LoggerFactory.getLogger(Slf4jLogger.class)));
        cfg.setUserAttributes(userProperties);
        cfg.setMarshaller(new OptimizedMarshaller(false));
        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        cfg.setEventStorageSpi(new MemoryEventStorageSpi());

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        commSpi.setSlowClientQueueLimit(1000);

        cfg.setCommunicationSpi(commSpi);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setClientReconnectDisabled(false);
        discoSpi.setForceServerMode(false);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(new CacheConfiguration());

        return cfg;
    }

    @Test
    public void cacheDestroy() throws Exception {
        CacheConfiguration<UUID, UUID> cacheCfg = new CacheConfiguration<>(UUID.randomUUID().toString());
        cacheCfg.setBackups(0);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new FifoEvictionPolicy(1));
        cacheCfg.setOffHeapMaxMemory(1600000); // ~1 000 000 UUIDs
        cacheCfg.setSwapEnabled(false);

        Ignite first = Ignition.start(getIgniteConfiguration());

        first.getOrCreateCache(cacheCfg).destroy();
    }

    @Test
    public void offheapCache() throws Exception {


        Ignite first = Ignition.start(getIgniteConfiguration());
        Ignite second = Ignition.start(getIgniteConfiguration());

        for (int i = 0; i < 10; i++) {

            CacheConfiguration<UUID, UUID> cacheCfg = new CacheConfiguration<>(UUID.randomUUID().toString());
            cacheCfg.setBackups(0);
            cacheCfg.setCacheMode(CacheMode.PARTITIONED);
            cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
            cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
            cacheCfg.setEvictionPolicy(new FifoEvictionPolicy(1));
            cacheCfg.setOffHeapMaxMemory(-1); // ~1 000 000 UUIDs
            cacheCfg.setSwapEnabled(true);

            for (int j = 0; j < 500000; j++) {
                if (j % 2 == 0) {
                    first.getOrCreateCache(cacheCfg).put(UUID.randomUUID(), UUID.randomUUID());
                } else {
                    second.getOrCreateCache(cacheCfg).put(UUID.randomUUID(), UUID.randomUUID());
                }
            }

            first.getOrCreateCache(cacheCfg).destroy();

            LoggerFactory.getLogger(getClass()).debug("Destroy cache: {}", cacheCfg.getName());
        }

    }

    @Test
    public void igfsMR() throws Exception {
        Ignite start = Ignition.start(getIgniteConfiguration());

    }

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

        firstIgnite.events()
                   .localListen(
                           new IgnitePredicate<Event>() {
                               @Override
                               public boolean apply(Event event) {
                                   LoggerFactory.getLogger(getClass()).debug("Event: {}", event);
                                   return true;
                               }
                           },
                           EventType.EVT_CACHE_ENTRY_CREATED,
                           EventType.EVT_CACHE_ENTRY_DESTROYED,
                           EventType.EVT_CACHE_ENTRY_EVICTED,
                           EventType.EVT_CACHE_OBJECT_PUT,
                           EventType.EVT_CACHE_OBJECT_REMOVED,
                           EventType.EVT_CACHE_OBJECT_EXPIRED
                   );
        secondIgnite.events()
                    .localListen(
                            new IgnitePredicate<Event>() {
                                @Override
                                public boolean apply(Event event) {
                                    LoggerFactory.getLogger(getClass()).debug("Event: {}", event);
                                    return true;
                                }
                            },
                            EventType.EVT_CACHE_ENTRY_CREATED,
                            EventType.EVT_CACHE_ENTRY_DESTROYED,
                            EventType.EVT_CACHE_ENTRY_EVICTED,
                            EventType.EVT_CACHE_OBJECT_PUT,
                            EventType.EVT_CACHE_OBJECT_REMOVED,
                            EventType.EVT_CACHE_OBJECT_EXPIRED
                    );

        IgniteCache<String, Long> firstCache = firstIgnite.getOrCreateCache(getCacheConfiguration());
        IgniteCache<String, Long> secondCache = secondIgnite.getOrCreateCache(getCacheConfiguration());

        for (int i = 0; i < 1; i++) {
            firstCache.put("aa", 1L);

            secondCache.put("a", 1L);
            firstCache.put("a", 2L);
            secondCache.put("b", 2L);
            secondCache.put("c", 4L);
            secondCache.put("ddddd", 4L);
            secondCache.put("text", 4L);

            firstCache.put("1234", 0l);
            firstCache.put("!1234", 0l);
        }

        Assert.assertEquals(2, secondCache.get("b").longValue());
        Assert.assertEquals(2, secondCache.get("a").longValue());
        Assert.assertNull(secondCache.get("k"));


//        QueryCursor<Cache.Entry<String, Long>> query = secondCache.query(new ScanQuery<String, Long>(new IgniteBiPredicate<String, Long>() {
//            @Override
//            public boolean apply(String s, Long aLong) {
//                return true;
//            }
//        }));
//
//        query.forEach(new Consumer<Cache.Entry<String, Long>>() {
//            @Override
//            public void accept(Cache.Entry<String, Long> stringLongEntry) {
//                LoggerFactory.getLogger(getClass()).debug("Result {}:{}", stringLongEntry.getKey(), stringLongEntry.getValue());
//            }
//        });


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

//    @Test
//    public void testTextSearch() throws Exception {
//        CacheConfiguration<UUID, TestObject> cacheCfg = new CacheConfiguration<>("test");
//        cacheCfg.setBackups(1);
//        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
//        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
//        cacheCfg.setOffHeapMaxMemory(0);
//        cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
//        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(1));
//        cacheCfg.setIndexedTypes(String.class, Strings.class);
//
////        Collection<CacheTypeMetadata> types = new ArrayList<>();
//
////        CacheTypeMetadata type = new CacheTypeMetadata();
////        type.setValueType(TestObject.class.getName());
////        Map<String, Class<?>> qryFlds = type.getQueryFields();
////        qryFlds.put("id", UUID.class);
////        qryFlds.put("title", String.class);
//////        qryFlds.put("genre", Set.class);
////
////        Collection<String> txtFlds = type.getTextFields();
////        txtFlds.add("title");
//////        txtFlds.add("genre");
////
////        types.add(type);
////
////        cacheCfg.setTypeMetadata(types);
//
//        Ignite ignite = Ignition.start(getIgniteConfiguration());
//        IgniteCache<UUID, TestObject> cache = ignite.<UUID, TestObject>getOrCreateCache(cacheCfg);
//
//        cache.put(UUID.randomUUID(), new TestObject("Тестовый текст тестовый ааа", Sets.newHashSet("комедия", "детектив")));
//        cache.put(UUID.randomUUID(), new TestObject("Тестовый текст текст", Sets.newHashSet("детектив", "фантастика")));
//        cache.put(UUID.randomUUID(), new TestObject("Тестовый текст текст", Sets.newHashSet("детектив", "фантастика")));
//        cache.put(UUID.randomUUID(), new TestObject("текст текст текст", Sets.newHashSet("детектив", "фантастика")));
//
//        QueryCursor<Cache.Entry<UUID, TestObject>> queryResult = cache.query(new TextQuery<UUID, TestObject>(TestObject.class, "тест"));
//
//        queryResult.getAll().forEach(item -> {
//            LoggerFactory.getLogger(getClass()).debug("Result: {}", item.getValue());
//        });
//
//    }

    @NotNull
    private CacheConfiguration<String, Long> getCacheConfiguration() {
        CacheConfiguration<String, Long> cacheCfg = new CacheConfiguration<>("test");
        cacheCfg.setBackups(1);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setOffHeapMaxMemory(0);
//        cacheCfg.setCacheStoreFactory(new CacheStoreFactory());
//        cacheCfg.setReadThrough(true);
//        cacheCfg.setWriteThrough(true);
        cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(1));


        cacheCfg.addCacheEntryListenerConfiguration(new TestCacheEntryListenerConfiguration());

        return cacheCfg;
    }

    enum TestEnum {

    }

    static class TestObject extends TestClass implements Serializable {
        private UUID id = UUID.randomUUID();

        @QueryTextField
        @Enumerated(enumType = TestEnum.class)
        private String title;

        @QueryTextField
        private Set<String> genre;

        public TestObject(String title, Set<String> genre) {
            this.title = title;
            this.genre = genre;
        }

        public UUID getId() {
            return id;
        }

        public void setId(UUID id) {
            this.id = id;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public Set<String> getGenre() {
            return genre;
        }

        public void setGenre(Set<String> genre) {
            this.genre = genre;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                              .add("title", getTitle())
                              .add("genre", getGenre())
                              .toString();
        }
    }

    private static class TestClass {
        private UUID id;

        public UUID getId() {
            return id;
        }

        public void setId(UUID id) {
            this.id = id;
        }
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
