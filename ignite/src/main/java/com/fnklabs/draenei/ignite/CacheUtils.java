package com.fnklabs.draenei.ignite;

import com.fnklabs.draenei.CassandraClientFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Cache utils
 */
public class CacheUtils {

    /**
     * Return cache name for specified entity class
     *
     * @param clazz Entity class
     *
     * @return Cache name
     */
    private static String getCacheName(Class clazz) {
        return StringUtils.lowerCase(clazz.getName());
    }

    /**
     * Get default cache configuration for specified entity class
     *
     * @return Cache Configuration for specified entity class
     */
    public static <Key, Entry> CacheConfiguration<Key, Entry> cacheConfiguration(String cacheName) {
        CacheConfiguration<Key, Entry> cacheConfiguration = new CacheConfiguration<>(cacheName);
        cacheConfiguration.setBackups(1);
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);
        cacheConfiguration.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheConfiguration.setReadThrough(false);
        cacheConfiguration.setWriteThrough(false);
        cacheConfiguration.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cacheConfiguration.setEvictionPolicy(new LruEvictionPolicy<>(100_000));
        cacheConfiguration.setSwapEnabled(false);
        cacheConfiguration.setOffHeapMaxMemory(-1);

        return cacheConfiguration;
    }

    public static <K extends Key, V> CacheConfiguration<K, V> persistent(CassandraClientFactory cassandraClientFactory, Class<V> entryClass) {
        CacheConfiguration<K, V> cacheConfiguration = cacheConfiguration(entryClass);
        cacheConfiguration.setWriteThrough(true);
        cacheConfiguration.setReadThrough(true);
        cacheConfiguration.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        cacheConfiguration.setCacheStoreFactory(new CacheDaoFactory<>(cassandraClientFactory, entryClass));
        cacheConfiguration.setEvictionPolicy(new LruEvictionPolicy(100_000));

        return cacheConfiguration;
    }

    /**
     * Get default cache configuration for specified entity class
     *
     * @param entityClass Entity class
     * @param <Entry>     Entity class typ
     *
     * @return Cache Configuration for specified entity class
     */
    private static <Key, Entry> CacheConfiguration<Key, Entry> cacheConfiguration(Class<Entry> entityClass) {
        return cacheConfiguration(getCacheName(entityClass));
    }
}
