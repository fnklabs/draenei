package com.fnklabs.draenei.orm.analytics;

import com.fnklabs.draenei.MetricsFactoryImpl;
import com.fnklabs.draenei.orm.CassandraClientFactory;
import com.fnklabs.draenei.orm.DataProvider;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/**
 * Load data from DataProvider
 */
class LoadDataTask<T extends Serializable> implements Callable<Integer>, Serializable {
    private final long startToken;
    private final long endToken;
    private final Class<T> entityClass;
    private final CassandraClientFactory cassandraClientFactory;
    private final CacheConfiguration<Long, T> cacheConfiguration;

    @IgniteInstanceResource
    private transient Ignite ignite;

    public LoadDataTask(long startToken,
                        long endToken,
                        Class<T> entityClass,
                        CassandraClientFactory cassandraClientFactory,
                        CacheConfiguration<Long, T> cacheConfiguration) {
        this.startToken = startToken;
        this.endToken = endToken;
        this.entityClass = entityClass;
        this.cassandraClientFactory = cassandraClientFactory;
        this.cacheConfiguration = cacheConfiguration;
    }

    @Override
    public Integer call() throws Exception {
        IgniteCache<Long, T> map = getCache();

        DataProvider<T> dataProvider = DataProvider.getDataProvider(entityClass, cassandraClientFactory, new MetricsFactoryImpl());

        LoadIntoDataGridConsumer<T> consumer = new LoadIntoDataGridConsumer<>(map, dataProvider);

        return dataProvider.load(startToken, endToken, consumer);
    }


    private IgniteCache<Long, T> getCache() {
        return ignite.<Long, T>getOrCreateCache(cacheConfiguration);
    }

    /**
     * Consumer that load data into hazelcast
     *
     * @param <T> Consumed data class type
     */
    protected static class LoadIntoDataGridConsumer<T> implements Consumer<T>, Serializable {
        @NotNull
        private transient final IgniteCache<Long, T> map;

        @NotNull
        private transient final DataProvider<T> dataProvider;

        LoadIntoDataGridConsumer(@NotNull IgniteCache<Long, T> map, @NotNull DataProvider<T> dataProvider) {
            this.map = map;
            this.dataProvider = dataProvider;
        }

        @Override
        public void accept(@NotNull T t) {
            long key = dataProvider.buildHashCode(t);
            map.put(key, t);
        }
    }
}
