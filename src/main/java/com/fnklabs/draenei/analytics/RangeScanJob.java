package com.fnklabs.draenei.analytics;

import com.fnklabs.draenei.orm.DataProvider;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.base.Verify;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeExecutionRejectedException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Callable task for seeking over data
 *
 * @param <T> Entity data type
 */
public abstract class RangeScanJob<T extends Serializable> implements ComputeJob {
    private final long start;
    private final long end;

    @IgniteInstanceResource
    private transient Ignite ignite;

    public RangeScanJob(long start, long end) {
        this.start = start;
        this.end = end;
    }


    @Override
    public Integer execute() throws IgniteException {
        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.load_data");


        try {
            List<T> result = new ArrayList<>();

            getDataProvider().load(start, end, result::add);

            timer.stop();

            Timer userCallBackTimer = MetricsFactory.getMetrics().getTimer("analytics.load_data.user_callback");

            int reduceResult = reduce(result);

            userCallBackTimer.stop();


            getLogger().debug("Complete to load data `{}` ({},{}] in {} and perform user_callback in {}", reduceResult, start, end, timer, userCallBackTimer);

            return reduceResult;
        } catch (RuntimeException e) {
            getLogger().warn(String.format("Cant load data in range (%d,%d]", start, end), e);

            throw new ComputeExecutionRejectedException(e);
        }
    }

    @Override
    public void cancel() {
    }

    protected Logger getLogger() {
        return LoggerFactory.getLogger(getClass());
    }

    @NotNull
    protected Ignite getIgnite() {
        Verify.verifyNotNull(ignite);

        return ignite;
    }

    protected abstract DataProvider<T> getDataProvider();

    protected int reduce(List<T> items) {
        return items.size();
    }
}
