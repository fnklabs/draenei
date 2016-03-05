package com.fnklabs.draenei.orm;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * Cache utils
 */
public class CacheUtils {
    private static final long OFF_HEAP_MAX_MEM = 5 * 1024L * 1024L * 1024L;

    /**
     * Return cache name for specified entity class
     *
     * @param clazz Entity class
     *
     * @return Cache name
     */
    public static String getCacheName(@NotNull Class clazz) {
        return StringUtils.lowerCase(clazz.getName());
    }

    public static <Entry> CacheConfiguration<Long, Entry> getCacheConfiguration(@NotNull DataProvider<Entry> dataProvider) {
        if (dataProvider instanceof CacheableDataProvider) {
            return ((CacheableDataProvider) dataProvider).getCacheConfiguration();
        }

        return CacheUtils.getDefaultCacheConfiguration(dataProvider.getEntityClass());
    }

    /**
     * Get default cache configuration for specified entity class
     *
     * @param entityClass Entity class
     * @param <Entry>     Entity class typ
     *
     * @return Cache Configuration for specified entity class
     */
    public static <Entry> CacheConfiguration<Long, Entry> getDefaultCacheConfiguration(Class<Entry> entityClass) {
        CacheConfiguration<Long, Entry> cacheCfg = new CacheConfiguration<>(getCacheName(entityClass));
        cacheCfg.setBackups(1);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setOffHeapMaxMemory(0);
        cacheCfg.setReadThrough(false);
        cacheCfg.setWriteThrough(false);
        cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(10000));
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setOffHeapMaxMemory(OFF_HEAP_MAX_MEM);

        return cacheCfg;
    }

    /**
     * Get default cache configuration for specified entity class
     *
     * @return Cache Configuration for specified entity class
     */
    public static <Key, Entry> CacheConfiguration<Key, Entry> getDefaultCacheConfiguration(String cacheName) {
        CacheConfiguration<Key, Entry> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setBackups(1);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setOffHeapMaxMemory(0);
        cacheCfg.setReadThrough(false);
        cacheCfg.setWriteThrough(false);
        cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(10000));
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setOffHeapMaxMemory(OFF_HEAP_MAX_MEM);
        return cacheCfg;
    }
}
