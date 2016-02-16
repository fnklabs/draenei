package com.fnklabs.draenei.orm.analytics;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.List;

class ReadLocalDataFromCache<Key, Value, Output> extends ComputeJobAdapter {
    @IgniteInstanceResource
    private Ignite ignite;

    private final CacheConfiguration<Key, Value> cacheConfiguration;

    private final MapFunction<Key, Value, Output> mapFunction;

    ReadLocalDataFromCache(CacheConfiguration<Key, Value> cacheConfiguration, MapFunction<Key, Value, Output> mapFunction) {
        this.cacheConfiguration = cacheConfiguration;
        this.mapFunction = mapFunction;
    }

    @Override
    public List<Output> execute() throws IgniteException {
        IgniteCache<Key, Value> cache = ignite.getOrCreateCache(cacheConfiguration);

        Iterable<Cache.Entry<Key, Value>> entries = cache.localEntries(CachePeekMode.PRIMARY);

        List<Output> values = new ArrayList<>();

        entries.forEach(entry -> {
            Value value = entry.getValue();
            Output map = mapFunction.map(entry.getKey(), value);

            values.add(map);
        });

        return values;
    }
}
