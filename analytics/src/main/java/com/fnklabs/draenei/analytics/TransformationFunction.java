package com.fnklabs.draenei.analytics;

import com.fnklabs.metrics.Metrics;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.concurrent.atomic.AtomicLong;

abstract class TransformationFunction<InputKey, InputValue, OutputKey, OutputValue, CombinerOutputValue> extends ComputeJobAdapter {
    private static final MathContext MATH_CONTEXT = new MathContext(2, RoundingMode.HALF_EVEN);

    private final CacheConfiguration<InputKey, InputValue> inputData;
    private final CacheConfiguration<OutputKey, CombinerOutputValue> outputData;

    @IgniteInstanceResource
    private Ignite ignite;

    TransformationFunction(CacheConfiguration<InputKey, InputValue> inputData, CacheConfiguration<OutputKey, CombinerOutputValue> outputData) {
        this.inputData = inputData;
        this.outputData = outputData;
    }

    @Override
    public final Long execute() throws IgniteException {
        Emitter<OutputKey, OutputValue, CombinerOutputValue> emitter = new Emitter<>(getOutputData(), getDataCombiner());

        AtomicLong processedEntriesCounter = new AtomicLong();

        BigDecimal localCacheSize = new BigDecimal(getEntriesCount());

        Metrics metrics = MetricsFactory.getMetrics();

        getLocalEntries().forEach(entry -> {
            Timer timer = metrics.getTimer("map_task.entry.transformation_function");

            transform(entry.getKey(), entry.getValue(), emitter);

            long processedEntries = processedEntriesCounter.incrementAndGet();

            timer.stop();

            BigDecimal progress = BigDecimal.valueOf(processedEntries * 100).divide(localCacheSize, MATH_CONTEXT);

            if (processedEntries % 10000 == 0) {
                getLogger().debug("Processed entry in {}", timer);
                getLogger().debug("Processed entries: {}/{} ({}%)", processedEntries, localCacheSize, progress);
            }
        });


        return processedEntriesCounter.longValue();
    }

    protected abstract void transform(InputKey key, InputValue value, Emitter<OutputKey, OutputValue, CombinerOutputValue> emitter);

    protected CacheEntryProcessor<OutputKey, CombinerOutputValue, CombinerOutputValue> getDataCombiner() {
        return new PutToCacheCombiner<>();
    }

    protected Logger getLogger() {
        return LoggerFactory.getLogger(getClass());
    }

    private Iterable<Cache.Entry<InputKey, InputValue>> getLocalEntries() {
        IgniteCache<InputKey, InputValue> cache = getInputData();

        long offHeapAllocatedSize = cache.metrics().getOffHeapAllocatedSize();

        getLogger().debug("Allocated size: {}MB", offHeapAllocatedSize / 1024 / 1024);

        return cache.localEntries(CachePeekMode.PRIMARY);
    }

    private IgniteCache<InputKey, InputValue> getInputData() {
        return getIgnite().getOrCreateCache(inputData);
    }

    private IgniteCache<OutputKey, CombinerOutputValue> getOutputData() {
        return getIgnite().getOrCreateCache(outputData);
    }

    /**
     * Get local data entries count
     *
     * @return count of entries
     */
    private int getEntriesCount() {
        IgniteCache<InputKey, InputValue> cache = getInputData();

        return cache.localSize(CachePeekMode.PRIMARY);
    }

    private Ignite getIgnite() {
        return ignite;
    }


}
