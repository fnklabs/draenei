package com.fnklabs.draenei.orm.analytics;

import com.fnklabs.draenei.orm.CacheableDataProvider;
import com.fnklabs.draenei.orm.CassandraClientFactory;
import com.fnklabs.draenei.orm.DataProvider;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeTaskNoResultCache;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * Callable task for seeking over data
 *
 * @param <T> Entity data type
 */
@ComputeTaskNoResultCache
class BootDataTask<T extends Serializable> implements IgniteCallable<Integer> {
    private final long startToken;
    private final long endToken;
    private final Class<T> entityClass;
    private final CassandraClientFactory cassandraClientFactory;


    @IgniteInstanceResource
    private transient Ignite ignite;


    public BootDataTask(long startToken, long endToken, Class<T> entityClass, CassandraClientFactory cassandraClientFactory) {
        this.startToken = startToken;
        this.endToken = endToken;
        this.entityClass = entityClass;
        this.cassandraClientFactory = cassandraClientFactory;
    }

    @Override
    public Integer call() throws Exception {

        CacheableDataProvider<T> cacheableDataProvider = CacheableDataProvider.getCacheableDataProvider(entityClass, cassandraClientFactory, ignite);

        DataProvider<T> dataProvider = DataProvider.getDataProvider(entityClass, cassandraClientFactory);

        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.load_data");


        int load = dataProvider.load(startToken, endToken, new Consumer<T>() {
            @Override
            public void accept(T t) {
                cacheableDataProvider.executeOnEntry(t, new PutToCache<>(t));
            }
        });
        timer.stop();

        LoggerFactory.getLogger(BootDataTask.class).debug("Complete to load data {} [{},{}] in {}", load, startToken, endToken, timer);
        return load;
    }
}
