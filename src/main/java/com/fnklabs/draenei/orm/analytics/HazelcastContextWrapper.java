package com.fnklabs.draenei.orm.analytics;

import org.jetbrains.annotations.NotNull;

class HazelcastContextWrapper<Key, Value> implements com.fnklabs.draenei.orm.analytics.Context<Key, Value> {
    private final com.hazelcast.mapreduce.Context<Key, Value> context;

    HazelcastContextWrapper(com.hazelcast.mapreduce.Context<Key, Value> context) {
        this.context = context;
    }

    @Override
    public void emit(@NotNull Key key, @NotNull Value value) {
        context.emit(key, value);
    }
}
