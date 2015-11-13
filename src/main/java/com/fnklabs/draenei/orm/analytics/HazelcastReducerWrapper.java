package com.fnklabs.draenei.orm.analytics;

import java.util.concurrent.CopyOnWriteArrayList;

class HazelcastReducerWrapper<Key, Value, ValueOut> extends com.hazelcast.mapreduce.Reducer<Value, ValueOut> {
    private final com.fnklabs.draenei.orm.analytics.Reducer<Key, Value, ValueOut> reducer;
    private final Key key;
    private transient final CopyOnWriteArrayList<Value> values = new CopyOnWriteArrayList<>();

    HazelcastReducerWrapper(Key key, com.fnklabs.draenei.orm.analytics.Reducer<Key, Value, ValueOut> reducer) {
        this.reducer = reducer;
        this.key = key;
    }

    @Override
    public void reduce(Value value) {
        values.add(value);
    }

    @Override
    public ValueOut finalizeReduce() {
        return reducer.reduce(key, values);
    }
}