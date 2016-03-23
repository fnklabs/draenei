package com.fnklabs.draenei.analytics;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;

import javax.cache.Cache;

public abstract class CacheMapper<Key, Value, Output> extends ComputeJobAdapter {
    private final CacheConfiguration<Key, Value> cacheConfiguration;

    @IgniteInstanceResource
    private Ignite ignite;


    protected CacheMapper(CacheConfiguration<Key, Value> cacheConfiguration) {
        this.cacheConfiguration = cacheConfiguration;
    }


    @Override
    public abstract Output execute() throws IgniteException;

    protected Iterable<Cache.Entry<Key, Value>> getLocalEntries() {
        IgniteCache<Key, Value> cache = ignite.getOrCreateCache(cacheConfiguration);

        return cache.localEntries(CachePeekMode.PRIMARY);
    }

    protected Ignite getIgnite() {
        return ignite;
    }


}
