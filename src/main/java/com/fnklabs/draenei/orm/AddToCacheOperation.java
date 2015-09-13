package com.fnklabs.draenei.orm;

import com.hazelcast.map.AbstractEntryProcessor;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

class AddToCacheOperation<Key, Value extends Cacheable> extends AbstractEntryProcessor<Key, Value> {
    @NotNull
    private Value value;

    public AddToCacheOperation(@NotNull Value value) {
        this.value = value;
    }

    @Override
    public Boolean process(Map.Entry<Key, Value> entry) {
        entry.setValue(value);

        return true;
    }
}
