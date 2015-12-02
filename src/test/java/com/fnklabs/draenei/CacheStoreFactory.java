package com.fnklabs.draenei;

import org.slf4j.LoggerFactory;

import javax.cache.configuration.Factory;

public class CacheStoreFactory implements Factory<TestCacheStore> {
    @Override
    public TestCacheStore create() {
        LoggerFactory.getLogger(getClass()).debug("Create new cache store instance");
        return new TestCacheStore("test");
    }
}
