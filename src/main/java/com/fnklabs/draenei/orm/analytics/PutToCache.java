package com.fnklabs.draenei.orm.analytics;

import org.apache.ignite.cache.CacheEntryProcessor;
import org.jetbrains.annotations.NotNull;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

class PutToCache<Entry> implements CacheEntryProcessor<Long, Entry, Entry> {
    @NotNull
    private final Entry newValue;

    public PutToCache(@NotNull Entry newValue) {
        this.newValue = newValue;
    }

    @Override
    public Entry process(MutableEntry<Long, Entry> entry, Object... arguments) throws EntryProcessorException {
        Entry oldValue = entry.getValue();

        entry.setValue(newValue);

        return oldValue;
    }
}
