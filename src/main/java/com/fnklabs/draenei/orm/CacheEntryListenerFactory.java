package com.fnklabs.draenei.orm;

import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryListener;

public class CacheEntryListenerFactory<Entry> implements Factory<CacheEntryListener<? super Long, ? super Entry>> {
    @Override
    public CacheEntryListener<? super Long, ? super Entry> create() {
        return new DataProviderCacheEntryListener<>();
    }
}
