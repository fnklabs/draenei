package com.fnklabs.draenei;

import com.fnklabs.draenei.analytics.AnalyticsContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;

public class IgniteLocalEntriesTest {


    @Test
    public void testLocalEntries() throws Exception {
        IgniteConfiguration igniteConfiguration = IgniteTest.getIgniteConfiguration();


        Ignite ignite = Ignition.start(igniteConfiguration);

        CacheConfiguration<Integer, UUID> cacheCfg = new AnalyticsContext(Mockito.mock(CassandraClient.class), Mockito.mock(Ignite.class)).getCacheConfiguration("test");
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(10));
        cacheCfg.setOffHeapMaxMemory(-1);
        cacheCfg.setSwapEnabled(true);

        IgniteCache<Integer, UUID> cache = ignite.getOrCreateCache(cacheCfg);


        for (int i = 0; i < 1000000; i++) {
            cache.put(i, UUID.randomUUID());
        }


        List<Long> executionTimes = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            long startTime = System.currentTimeMillis();

            Iterable<Cache.Entry<Integer, UUID>> iterable = cache.localEntries(CachePeekMode.PRIMARY);

            StreamSupport.stream(iterable.spliterator(), true)

//            iterable
                         .forEach(entry -> {
                         });

            long executionTime = System.currentTimeMillis() - startTime;

            executionTimes.add(executionTime);
        }


        LoggerFactory.getLogger(getClass()).debug("Avg time: {}", executionTimes.stream().mapToLong(a -> a).average());

        ignite.close();

    }
}
