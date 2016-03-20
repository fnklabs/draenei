package com.fnklabs.draenei.analytics;

import com.fnklabs.draenei.orm.DataProvider;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import org.apache.ignite.Ignite;
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
public abstract class DataProviderRangeScan<T extends Serializable> implements IgniteCallable<Integer> {
    private final long startToken;
    private final long endToken;

    @IgniteInstanceResource
    private transient Ignite ignite;

    public DataProviderRangeScan(long startToken, long endToken) {
        this.startToken = startToken;
        this.endToken = endToken;
    }

    @Override
    public final Integer call() throws Exception {
        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.load_data");

        int load = getDataProvider().load(startToken, endToken, getResultConsumer());
        timer.stop();

        LoggerFactory.getLogger(DataProviderRangeScan.class).debug("Complete to load data `{}` [{},{}] in {}", load, startToken, endToken, timer);
        return load;
    }

    protected Ignite getIgnite() {
        return ignite;
    }

    protected abstract DataProvider<T> getDataProvider();

    protected Consumer<T> getResultConsumer() {
        return new LoggerConsumer<>();
    }

    private static class LoggerConsumer<T> implements Consumer<T> {

        @Override
        public void accept(T t) {
            LoggerFactory.getLogger(LoggerConsumer.class).debug("Consumed data: {}", t);
        }
    }
}
