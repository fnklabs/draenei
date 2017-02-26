package com.fnklabs.draenei.orm;

import javax.cache.configuration.Factory;

class CacheDaoFactory<Entry> implements Factory<CacheDao<Entry>> {
    private final Class<Entry> entryClass;
    private final CassandraClientFactory cassandraClientFactory;

    private transient CacheDao<Entry> cacheDao;

    CacheDaoFactory(CassandraClientFactory cassandraClientFactory, Class<Entry> entryClass) {
        this.cassandraClientFactory = cassandraClientFactory;
        this.entryClass = entryClass;
    }

    @Override
    public synchronized CacheDao<Entry> create() {
        if (cacheDao == null) {
            cacheDao = new CacheDao<>(new DataProvider<>(entryClass, cassandraClientFactory));
        }

        return cacheDao;
    }
}
