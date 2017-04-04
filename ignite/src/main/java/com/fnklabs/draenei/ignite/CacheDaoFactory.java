package com.fnklabs.draenei.ignite;

import com.fnklabs.draenei.CassandraClientFactory;
import com.fnklabs.draenei.orm.DataProvider;
import org.apache.ignite.cache.store.CacheStore;

import javax.cache.configuration.Factory;

/**
 * Singleton implementation of {@link Factory} that return ignite {@link CacheStore} factory that return one instance of
 * {@link CacheStore} per JVM always return one instance
 *
 * @param <K> Cache Key
 * @param <V> Cache Value
 */
public class CacheDaoFactory<K extends Key, V> implements Factory<CacheStore<K, V>> {
    /**
     * Entity class
     */
    private final Class<V> entryClass;

    /**
     * Cassandra client factory
     */
    private final CassandraClientFactory cassandraClientFactory;

    /**
     * Cache store instance that initialized by factory
     */
    private transient DraeneiCacheStore<K, V> draeneiCacheStore;

    CacheDaoFactory(CassandraClientFactory cassandraClientFactory, Class<V> entryClass) {
        this.cassandraClientFactory = cassandraClientFactory;
        this.entryClass = entryClass;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized CacheStore<K, V> create() {
        if (draeneiCacheStore == null) {
            draeneiCacheStore = new DraeneiCacheStore<>(new DataProvider<>(entryClass, cassandraClientFactory));
        }

        return draeneiCacheStore;
    }
}
