package com.fnklabs.draenei.analytics;

import com.fnklabs.draenei.orm.DataProvider;
import com.fnklabs.metrics.Metrics;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.base.Verify;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.compute.ComputeExecutionRejectedException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Callable task for seeking over data
 *
 * @param <Entity>              Entity data type
 * @param <OutputKey>           Cache entry key
 * @param <OutputValue>         Cache entry value
 * @param <CombinerOutputValue> Combiner value
 */
public abstract class RangeScanJob<Entity, OutputKey, OutputValue, CombinerOutputValue> implements ComputeJob {
    private final long start;
    private final long end;

    private final CacheConfiguration<OutputKey, CombinerOutputValue> outputData;

    @IgniteInstanceResource
    private transient Ignite ignite;

    public RangeScanJob(long start, long end, CacheConfiguration<OutputKey, CombinerOutputValue> outputData) {
        this.start = start;
        this.end = end;
        this.outputData = outputData;
    }

    @Override
    public Integer execute() throws IgniteException {
        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.load_data");

        AtomicInteger entries = new AtomicInteger();

        try {
            IgniteCache<OutputKey, CombinerOutputValue> cache = ignite.getOrCreateCache(outputData);

            Metrics metrics = MetricsFactory.getMetrics();

            Emitter<OutputKey, OutputValue, CombinerOutputValue> emitter = new Emitter<>(cache, getDataCombiner());

            getDataProvider().load(start, end, entity -> {
                Timer userCallBackTimer = metrics.getTimer("analytics.load_data.user_callback");

                entries.incrementAndGet();

                map(entity, emitter);

                userCallBackTimer.stop();
            });

            return entries.intValue();
        } catch (RuntimeException e) {
            getLogger().warn(String.format("Cant load data in range (%d,%d]", start, end), e);

            throw new ComputeExecutionRejectedException(e);
        } finally {
            getLogger().debug("Loaded entries: {}", entries);
            timer.stop();
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

    protected abstract DataProvider<Entity> getDataProvider();

    protected abstract void map(Entity entity, Emitter<OutputKey, OutputValue, CombinerOutputValue> emitter);

    protected CacheEntryProcessor<OutputKey, CombinerOutputValue, CombinerOutputValue> getDataCombiner() {
        return new PutToCacheCombiner<OutputKey, CombinerOutputValue>();
    }
}
