package com.fnklabs.draenei.orm;

import javax.cache.Cache;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class CacheDao<Entry> implements CacheWriter<List, Entry>, CacheLoader<List, Entry> {
    private final DataProvider<Entry> dataProvider;

    CacheDao(DataProvider<Entry> dataProvider) {
        this.dataProvider = dataProvider;
    }

    @Override
    public Entry load(List key) throws CacheLoaderException {
        return dataProvider.findOne(key.toArray());
    }

    @Override
    public Map<List, Entry> loadAll(Iterable<? extends List> keys) throws CacheLoaderException {
        return StreamSupport.stream(keys.spliterator(), false)
                            .collect(Collectors.toMap(
                                    key -> key,
                                    this::load,
                                    (a, b) -> a
                            ));
    }

    @Override
    public void write(Cache.Entry<? extends List, ? extends Entry> entry) throws CacheWriterException {
        dataProvider.save(entry.getValue());
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends List, ? extends Entry>> entries) throws CacheWriterException {
        entries.forEach(this::write);
    }

    @Override
    public void delete(Object key) throws CacheWriterException {
        dataProvider.remove(((List) key).toArray());
    }

    @Override
    public void deleteAll(Collection<?> keys) throws CacheWriterException {
        keys.forEach(this::delete);
    }
}
