package com.fnklabs.draenei.analytics;

import com.fnklabs.draenei.orm.DataProvider;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeExecutionRejectedException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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

        List<T> result = new ArrayList<>();

        try {
            getDataProvider().load(start, end, result::add);

            timer.stop();

            Timer userCallBackTimer = MetricsFactory.getMetrics().getTimer("analytics.load_data.user_callback");

            result.parallelStream()
                  .forEach(getResultConsumer());

            userCallBackTimer.stop();


            getLogger().debug("Complete to load data `{}` ({},{}] in {} and perform user_callback in {}", result.size(), start, end, timer, userCallBackTimer);
        } catch (RuntimeException e) {
            getLogger().warn(String.format("Cant load data in range (%d,%d]", start, end), e);

            throw new ComputeExecutionRejectedException(e);
        }

        return result.size();
    }

    @Override
    public void cancel() {

    }

    protected Logger getLogger() {
        return LoggerFactory.getLogger(getClass());
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
