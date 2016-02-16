package com.fnklabs.draenei.orm.analytics;

import com.fnklabs.draenei.orm.CassandraClientFactory;
import com.fnklabs.draenei.orm.DataProvider;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * Callable task for seeking over data
 *
 * @param <T>              Entity data type
 * @param <ResultConsumer> Entity result consumer
 */
class SeekOverDataTask<T extends Serializable, ResultConsumer extends Consumer<T> & Serializable> implements IgniteCallable<Integer> {
    private final long startToken;
    private final long endToken;
    private final Class<T> entityClass;
    private final CassandraClientFactory cassandraClientFactory;
    private final ResultConsumer resultConsumer;


    @IgniteInstanceResource
    private transient Ignite ignite;

    public SeekOverDataTask(long startToken, long endToken, Class<T> entityClass, CassandraClientFactory cassandraClientFactory, ResultConsumer resultConsumer) {
        this.startToken = startToken;
        this.endToken = endToken;
        this.entityClass = entityClass;
        this.cassandraClientFactory = cassandraClientFactory;
        this.resultConsumer = resultConsumer;
    }

    @Override
    public Integer call() throws Exception {
        DataProvider<T> dataProvider = DataProvider.getDataProvider(entityClass, cassandraClientFactory);

        if (resultConsumer instanceof IgniteInstanceAware) {
            ((IgniteInstanceAware) resultConsumer).setIgnite(ignite);
        }

        return dataProvider.load(startToken, endToken, resultConsumer);
    }
}
