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

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * Callable task for seeking over data
 *
 * @param <T> Entity data type
 * @param <K> Cache entry key
 * @param <V> Cache entry value
 */
public abstract class RangeScanJob<T, K, V> implements ComputeJob {
    private final long start;
    private final long end;
    private final CacheConfiguration<K, V> cacheConfiguration;

    @IgniteInstanceResource
    private transient Ignite ignite;

    public RangeScanJob(long start, long end, CacheConfiguration<K, V> cacheConfiguration) {
        this.start = start;
        this.end = end;
        this.cacheConfiguration = cacheConfiguration;
    }


    @Override
    public Integer execute() throws IgniteException {
        Timer timer = MetricsFactory.getMetrics().getTimer("analytics.load_data");

        AtomicInteger entries = new AtomicInteger();

        try {
            IgniteCache<K, V> cache = ignite.getOrCreateCache(cacheConfiguration);

            Metrics metrics = MetricsFactory.getMetrics();

            getDataProvider().load(start, end, entity -> {
                Timer userCallBackTimer = metrics.getTimer("analytics.load_data.user_callback");
                entries.incrementAndGet();


                map(entity, (k, entryProcessor) -> {
                    cache.invoke(k, entryProcessor, entity);
                });

                userCallBackTimer.stop();

                getLogger().debug("Fetch row `{}` ({},{}] in {} and perform user_callback in {}", entries, start, end, timer, userCallBackTimer);
            });


            return entries.intValue();
        } catch (RuntimeException e) {
            getLogger().warn(String.format("Cant load data in range (%d,%d]", start, end), e);

            throw new ComputeExecutionRejectedException(e);
        } finally {
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

    protected abstract DataProvider<T> getDataProvider();

    protected abstract void map(T entity, BiConsumer<K, CacheEntryProcessor<K, V, V>> consumer);

    public static class PutProcessor<Key, Value> implements CacheEntryProcessor<Key, Value, Value> {
        @Override
        public Value process(MutableEntry<Key, Value> entry, Object... arguments) throws EntryProcessorException {

            Value value = entry.getValue();

            entry.setValue((Value) arguments[0]);

            return value;
        }
    }

}
