package com.fnklabs.draenei.orm;

import javax.cache.event.*;

public class DataProviderCacheEntryListener<Entry> implements CacheEntryCreatedListener<Long, Entry>, CacheEntryUpdatedListener<Long, Entry>, CacheEntryRemovedListener<Long, Entry>, CacheEntryExpiredListener<Long, Entry> {
    @Override
    public void onCreated(Iterable<CacheEntryEvent<? extends Long, ? extends Entry>> cacheEntryEvents) throws CacheEntryListenerException {

    }

    @Override
    public void onExpired(Iterable<CacheEntryEvent<? extends Long, ? extends Entry>> cacheEntryEvents) throws CacheEntryListenerException {

    }

    @Override
    public void onRemoved(Iterable<CacheEntryEvent<? extends Long, ? extends Entry>> cacheEntryEvents) throws CacheEntryListenerException {

    }

    @Override
    public void onUpdated(Iterable<CacheEntryEvent<? extends Long, ? extends Entry>> cacheEntryEvents) throws CacheEntryListenerException {

    }
}
