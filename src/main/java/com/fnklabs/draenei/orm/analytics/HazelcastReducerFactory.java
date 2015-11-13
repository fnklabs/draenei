package com.fnklabs.draenei.orm.analytics;


import com.hazelcast.mapreduce.ReducerFactory;
import org.jetbrains.annotations.NotNull;

class HazelcastReducerFactory<Key, Value, ValueOut> implements ReducerFactory<Key, Value, ValueOut> {
    private final com.fnklabs.draenei.orm.analytics.Reducer<Key, Value, ValueOut> reducer;

    HazelcastReducerFactory(@NotNull com.fnklabs.draenei.orm.analytics.Reducer<Key, Value, ValueOut> reducer) {
        this.reducer = reducer;
    }

    @Override
    public com.hazelcast.mapreduce.Reducer<Value, ValueOut> newReducer(Key key) {
        return new HazelcastReducerWrapper<>(key, reducer);
    }
}

