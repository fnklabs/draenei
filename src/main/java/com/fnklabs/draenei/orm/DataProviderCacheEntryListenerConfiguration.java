package com.fnklabs.draenei.orm;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;

class DataProviderCacheEntryListenerConfiguration<Entry> implements CacheEntryListenerConfiguration<Long, Entry> {
    @Override
    public Factory<CacheEntryListener<? super Long, ? super Entry>> getCacheEntryListenerFactory() {
        return new CacheEntryListenerFactory<Entry>();
    }

    @Override
    public boolean isOldValueRequired() {
        return true;
    }

    @Override
    public Factory<CacheEntryEventFilter<? super Long, ? super Entry>> getCacheEntryEventFilterFactory() {
        return null;
    }

    @Override
    public boolean isSynchronous() {
        return false;
    }
}
