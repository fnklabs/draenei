package com.fnklabs.draenei.orm;

import org.slf4j.LoggerFactory;

import javax.cache.configuration.Factory;
import javax.cache.integration.CacheWriter;

class CacheWriterFactory<Entry> implements Factory<CacheWriter<Long, Entry>> {

    private final CassandraClientFactory cassandraClientFactory;
    private final Class<Entry> entityClass;

    CacheWriterFactory(CassandraClientFactory cassandraClientFactory, Class<Entry> entityClass) {
        this.cassandraClientFactory = cassandraClientFactory;
        this.entityClass = entityClass;
    }

    @Override
    public synchronized CacheWriter<Long, Entry> create() {
        LoggerFactory.getLogger(getClass()).debug("Create Cache writer");

        return new DataProviderCacheWriter<>(cassandraClientFactory, entityClass);
    }
}
