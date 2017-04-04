package com.fnklabs.draenei.ignite;

import com.fnklabs.draenei.orm.DataProvider;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class DraeneiCacheStore<K extends Key, V> implements CacheStore<K, V>, Serializable {
    private final DataProvider<V> dataProvider;

    DraeneiCacheStore(DataProvider<V> dataProvider) {
        this.dataProvider = dataProvider;
    }

    @Override
    public V load(K key) throws CacheLoaderException {
        return dataProvider.findOne(key.toArray());
    }

    @Override
    public Map<K, V> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
        return StreamSupport.stream(keys.spliterator(), false)
                            .collect(Collectors.toMap(
                                    key -> key,
                                    this::load,
                                    (a, b) -> a
                            ));
    }

    @Override
    public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        dataProvider.save(entry.getValue());
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) throws CacheWriterException {
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

    @Override
    public void loadCache(IgniteBiInClosure<K, V> clo, @Nullable Object... args) throws CacheLoaderException {}

    @Override
    public void sessionEnd(boolean commit) throws CacheWriterException {}
}
