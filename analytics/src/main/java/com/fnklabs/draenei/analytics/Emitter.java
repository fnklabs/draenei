package com.fnklabs.draenei.analytics;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;

import java.util.function.BiFunction;

public class Emitter<Key, Value, ReturnValue> implements BiFunction<Key, Value, ReturnValue> {
    private final IgniteCache<Key, ReturnValue> dataSource;

    private final CacheEntryProcessor<Key, ReturnValue, ReturnValue> dataCombiner;

    Emitter(IgniteCache<Key, ReturnValue> dataSource, CacheEntryProcessor<Key, ReturnValue, ReturnValue> dataCombiner) {
        this.dataSource = dataSource;
        this.dataCombiner = dataCombiner;
    }

    @Override
    public ReturnValue apply(Key key, Value value) {
        return dataSource.invoke(key, dataCombiner, value);
    }
}
