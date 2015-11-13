package com.fnklabs.draenei.orm;

import com.hazelcast.map.AbstractEntryProcessor;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

class PutToCacheOperation<Key, Value extends Cacheable> extends AbstractEntryProcessor<Key, Value> {

    @NotNull
    private final Value value;

    public PutToCacheOperation(@NotNull Value value) {
        this.value = value;
    }

    /**
     * Put to entry to cache and return old value
     *
     * @param entry Map entry
     *
     * @return Previous value from entry
     */
    @Override
    public Value process(Map.Entry<Key, Value> entry) {
        return entry.setValue(value);
    }
}
