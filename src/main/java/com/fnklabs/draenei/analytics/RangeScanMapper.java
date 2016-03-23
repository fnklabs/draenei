package com.fnklabs.draenei.analytics;

import com.fnklabs.draenei.orm.DataProvider;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * Callable task for seeking over data
 *
 * @param <T> Entity data type
 */
public abstract class RangeScanMapper<T extends Serializable> implements ComputeJob {
    private final long start;
    private final long end;

    @IgniteInstanceResource
    private transient Ignite ignite;

    public RangeScanMapper(long start, long end) {
        this.start = start;
        this.end = end;
    }


    @Override
    public Integer execute() throws IgniteException {
        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.load_data");

        int load = getDataProvider().load(start, end, getResultConsumer());

        timer.stop();

        LoggerFactory.getLogger(RangeScanMapper.class).debug("Complete to load data `{}` ({},{}] in {}", load, start, end, timer);

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

    @Override
    public void cancel() {

    }
}
