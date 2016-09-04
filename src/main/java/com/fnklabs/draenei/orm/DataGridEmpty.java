package com.fnklabs.draenei.orm;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public final class DataGridEmpty<Key, Value> implements DataGrid<Key, Value> {
    public static <Key, Value> DataGrid<Key, Value> create() {
        return new DataGridEmpty<>();
    }

    private DataGridEmpty() {
    }

    @Override
    public Value get(Key key) {
        return null;
    }

    @Override
    public Map<Key, Value> getAll(Set<? extends Key> keys) {
        return null;
    }

    @Override
    public boolean containsKey(Key key) {
        return false;
    }

    @Override
    public void loadAll(Set<? extends Key> keys, boolean replaceExistingValues, CompletionListener completionListener) {

    }

    @Override
    public void put(Key key, Value value) {

    }

    @Override
    public Value getAndPut(Key key, Value value) {
        return null;
    }

    @Override
    public void putAll(Map<? extends Key, ? extends Value> map) {

    }

    @Override
    public boolean putIfAbsent(Key key, Value value) {
        return false;
    }

    @Override
    public boolean remove(Key key) {
        return false;
    }

    @Override
    public boolean remove(Key key, Value oldValue) {
        return false;
    }

    @Override
    public Value getAndRemove(Key key) {
        return null;
    }

    @Override
    public boolean replace(Key key, Value oldValue, Value newValue) {
        return false;
    }

    @Override
    public boolean replace(Key key, Value value) {
        return false;
    }

    @Override
    public Value getAndReplace(Key key, Value value) {
        return null;
    }

    @Override
    public void removeAll(Set<? extends Key> keys) {

    }

    @Override
    public void removeAll() {

    }

    @Override
    public void clear() {

    }

    @Override
    public <T> T invoke(Key key, EntryProcessor<Key, Value, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        return null;
    }

    @Override
    public <T> Map<Key, EntryProcessorResult<T>> invokeAll(Set<? extends Key> keys, EntryProcessor<Key, Value, T> entryProcessor, Object... arguments) {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public CacheManager getCacheManager() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return null;
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<Key, Value> cacheEntryListenerConfiguration) {

    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<Key, Value> cacheEntryListenerConfiguration) {

    }

    @Override
    public Iterator<Entry<Key, Value>> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public <C extends Configuration<Key, Value>> C getConfiguration(Class<C> clazz) {
        return null;
    }
}
