package com.fnklabs.draenei;

import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

public class TestCacheStore implements CacheStore<String, Long>, Serializable {
    private final String name;

    public TestCacheStore(String name) {
        this.name = name;
    }

    @Override
    public void loadCache(IgniteBiInClosure<String, Long> clo, @Nullable Object... args) throws CacheLoaderException {

    }

    @Override
    public void sessionEnd(boolean commit) throws CacheWriterException {

    }

    @Override
    public Long load(String key) throws CacheLoaderException {
        LoggerFactory.getLogger(getClass()).debug("[{}] Read key: {}", name, key);
        return null;
    }

    @Override
    public Map<String, Long> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
        return null;
    }

    @Override
    public void write(Cache.Entry<? extends String, ? extends Long> entry) throws CacheWriterException {
        LoggerFactory.getLogger(getClass()).debug("[{}] Write key: {}/{}", name, entry.getKey(), entry.getValue());
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends String, ? extends Long>> entries) throws CacheWriterException {

    }

    @Override
    public void delete(Object key) throws CacheWriterException {
        LoggerFactory.getLogger(getClass()).debug("[{}] Delete key: {}", name, key);
    }

    @Override
    public void deleteAll(Collection<?> keys) throws CacheWriterException {

    }
}
