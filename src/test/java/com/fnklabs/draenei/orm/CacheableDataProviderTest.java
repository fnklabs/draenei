package com.fnklabs.draenei.orm;

import com.datastax.driver.core.HostDistance;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.MetricsFactoryImpl;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


@Ignore
public class CacheableDataProviderTest {
    private final MetricsFactoryImpl metricsFactoryImplFactory = new MetricsFactoryImpl();
    private CassandraClient cassandraClient;
    private Ignite ignite;

    @Before
    public void setUp() throws Exception {
        cassandraClient = new CassandraClient("", "", "test", "10.211.55.19", metricsFactoryImplFactory, HostDistance.LOCAL);
        ignite = Ignition.start(getIgniteConfiguration());
    }

    @After
    public void tearDown() throws Exception {
        cassandraClient.execute("truncate test");

        cassandraClient.close();
        cassandraClient = null;

        ignite.close();
    }

    @Test
    public void testFindOneAsync() throws Exception {
        CacheableDataProvider<TestEntity> cacheableDataProvider = new CacheableDataProvider<>(TestEntity.class, new CassandraClientFactory() {
            @Override
            public CassandraClient create() {
                return cassandraClient;
            }
        }, ignite, metricsFactoryImplFactory);

        ListenableFuture<TestEntity> oneAsync = cacheableDataProvider.findOneAsync(UUID.randomUUID());

        TestEntity testEntity = oneAsync.get(5, TimeUnit.SECONDS);

        Assert.assertNull(testEntity);

        testEntity = new TestEntity();
        testEntity.setId(UUID.randomUUID());

        Boolean result = cacheableDataProvider.saveAsync(testEntity).get(5, TimeUnit.SECONDS);

        Assert.assertTrue(result);

        TestEntity testEntity2 = cacheableDataProvider.findOneAsync(testEntity.getId()).get(5, TimeUnit.SECONDS);

        Assert.assertNotNull(testEntity);
        Assert.assertEquals(testEntity, testEntity2);
    }


    @Test
    public void testSaveAsync() throws Exception {
        CacheableDataProvider<TestEntity> cacheableDataProvider = new CacheableDataProvider<>(TestEntity.class, new CassandraClientFactory() {
            @Override
            public CassandraClient create() {
                return cassandraClient;
            }
        }, ignite, metricsFactoryImplFactory);


        TestEntity testEntity = new TestEntity();
        testEntity.setId(UUID.randomUUID());

        Boolean result = cacheableDataProvider.saveAsync(testEntity).get(5, TimeUnit.SECONDS);

        Assert.assertTrue(result);

        TestEntity testEntity2 = cacheableDataProvider.findOneAsync(testEntity.getId()).get(5, TimeUnit.SECONDS);

        Assert.assertNotNull(testEntity);
        Assert.assertEquals(testEntity, testEntity2);

        Thread.sleep(1000);
    }

    @Test
    public void testRemoveAsync() throws Exception {
        CacheableDataProvider<TestEntity> cacheableDataProvider = new CacheableDataProvider<>(TestEntity.class, new CassandraClientFactory() {
            @Override
            public CassandraClient create() {
                return cassandraClient;
            }
        }, ignite, metricsFactoryImplFactory);


        TestEntity testEntity = new TestEntity();
        testEntity.setId(UUID.randomUUID());

        Boolean result = cacheableDataProvider.saveAsync(testEntity).get(5, TimeUnit.SECONDS);

        Assert.assertTrue(result);

        TestEntity testEntity2 = cacheableDataProvider.findOneAsync(testEntity.getId()).get(5, TimeUnit.SECONDS);

        Assert.assertNotNull(testEntity);
        Assert.assertEquals(testEntity, testEntity2);

        result = cacheableDataProvider.removeAsync(testEntity).get(5, TimeUnit.SECONDS);

        Assert.assertTrue(result);

        testEntity = cacheableDataProvider.findOneAsync(testEntity.getId()).get(5, TimeUnit.SECONDS);

        Assert.assertNull(testEntity);
    }

    @Test
    public void testGetMapName() throws Exception {

    }

    @NotNull
    private IgniteConfiguration getIgniteConfiguration() throws UnknownHostException {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIncludeEventTypes(org.apache.ignite.events.EventType.EVTS_CACHE);
        cfg.setGridName(InetAddress.getLocalHost().getHostName() + " - 1");
        cfg.setClientMode(false);
        cfg.setGridLogger(new Slf4jLogger(LoggerFactory.getLogger(Slf4jLogger.class)));
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
}