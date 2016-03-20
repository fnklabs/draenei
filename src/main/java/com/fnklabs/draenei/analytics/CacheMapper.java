package com.fnklabs.draenei.analytics;

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

public abstract class CacheMapper<Key, Value, Output> extends ComputeJobAdapter {
    private final CacheConfiguration<Key, Value> cacheConfiguration;

    @IgniteInstanceResource
    private Ignite ignite;


    protected CacheMapper(CacheConfiguration<Key, Value> cacheConfiguration) {
        this.cacheConfiguration = cacheConfiguration;
    }

    @Override
    public final List<Output> execute() throws IgniteException {
        IgniteCache<Key, Value> cache = ignite.getOrCreateCache(cacheConfiguration);

        Iterable<Cache.Entry<Key, Value>> entries = cache.localEntries(CachePeekMode.PRIMARY);

        List<Output> values = new ArrayList<>();

        entries.forEach(entry -> {
            Value value = entry.getValue();
            Output output = map(entry.getKey(), value);

            if (output != null) {
                values.add(output);
            }
        });

        return values;
    }

    protected Ignite getIgnite() {
        return ignite;
    }

    protected abstract Output map(Key key, Value value);
}
