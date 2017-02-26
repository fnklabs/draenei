package com.fnklabs.draenei.orm;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Cache utils
 */
class CacheUtils {

    /**
     * Return cache name for specified entity class
     *
     * @param clazz Entity class
     *
     * @return Cache name
     */
    private static String getCacheName(@NotNull Class clazz) {
        return StringUtils.lowerCase(clazz.getName());
    }

    /**
     * Get default cache configuration for specified entity class
     *
     * @return Cache Configuration for specified entity class
     */
    private static <Key, Entry> CacheConfiguration<Key, Entry> getDefaultCacheConfiguration(String cacheName) {
        CacheConfiguration<Key, Entry> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setBackups(1);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setReadThrough(false);
        cacheCfg.setWriteThrough(false);
        cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cacheCfg.setEvictionPolicy(new LruEvictionPolicy<>(5000));
        cacheCfg.setSwapEnabled(false);
        cacheCfg.setOffHeapMaxMemory(-1);
        return cacheCfg;
    }

    /**
     * Get default cache configuration for specified entity class
     *
     * @param entityClass Entity class
     * @param <Entry>     Entity class typ
     *
     * @return Cache Configuration for specified entity class
     */
    static <Entry> CacheConfiguration<List, Entry> getDefaultCacheConfiguration(Class<Entry> entityClass) {
        return getDefaultCacheConfiguration(getCacheName(entityClass));
    }
}
