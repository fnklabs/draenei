package com.fnklabs.draenei.orm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import java.util.Collection;

class DataProviderCacheWriter<Entry> implements CacheWriter<Long, Entry> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataProvider.class);
    private final CassandraClientFactory cassandraClientFactory;
    private final Class<Entry> entityClass;
    private final DataProvider<Entry> dataProvider;

    DataProviderCacheWriter(CassandraClientFactory cassandraClientFactory, Class<Entry> entityClass) {
        this.cassandraClientFactory = cassandraClientFactory;
        this.entityClass = entityClass;
        dataProvider = DataProvider.getDataProvider(entityClass, cassandraClientFactory);
    }

    @Override
    public void write(Cache.Entry<? extends Long, ? extends Entry> entry) throws CacheWriterException {
        try {
            Boolean save = dataProvider.save(entry.getValue());

            if (!save) {
                throw new CacheWriterException();
            }
        } catch (Exception e) {
            LOGGER.warn("Cant save data", e);
            throw new CacheWriterException(e);
        }

    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends Long, ? extends Entry>> entries) throws CacheWriterException {
        entries.forEach(entry -> write(entry));
    }

    @Override
    public void delete(Object key) throws CacheWriterException {
        LOGGER.warn("Current method is not implemented");
    }

    @Override
    public void deleteAll(Collection<?> keys) throws CacheWriterException {
        LOGGER.warn("Current method is not implemented");
    }
}
