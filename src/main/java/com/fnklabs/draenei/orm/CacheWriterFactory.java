package com.fnklabs.draenei.orm;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheWriter;

class CacheWriterFactory<Entry> extends FactoryBuilder.SingletonFactory<CacheWriter<Long, Entry>> {

    public CacheWriterFactory(CassandraClientFactory cassandraClientFactory, Class<Entry> entityClass) {
        super(new DataProviderCacheWriter<>(cassandraClientFactory, entityClass));
    }
}
