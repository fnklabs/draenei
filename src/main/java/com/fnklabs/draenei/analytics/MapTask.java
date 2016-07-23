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
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.concurrent.atomic.AtomicLong;

public abstract class MapTask<InputKey, InputValue, OutputKey, OutputValue, CombinerOutputValue> extends TransformationFunction<InputKey, InputValue, OutputKey, OutputValue, CombinerOutputValue> {

    protected MapTask(
            @NotNull CacheConfiguration<InputKey, InputValue> inputData,
            @NotNull CacheConfiguration<OutputKey, CombinerOutputValue> outputData
    ) {
        super(inputData, outputData);
    }

    protected abstract void map(InputKey key, InputValue value, Emitter<OutputKey, OutputValue, CombinerOutputValue> emitter);

    @Override
    protected void transform(InputKey key, InputValue value, Emitter<OutputKey, OutputValue, CombinerOutputValue> emitter) {
        map(key, value, emitter);
    }
}
