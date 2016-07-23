package com.fnklabs.draenei.analytics;

import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * @param <InputKey>
 * @param <InputValue>
 * @param <OutputKey>
 * @param <OutputValue>
 */
public abstract class MapFactory<InputKey, InputValue, OutputKey, OutputValue, CombinerOutput> extends TransformationFactory<InputKey, InputValue, OutputKey, OutputValue, CombinerOutput> {

    @NotNull
    @Override
    protected abstract MapTask<InputKey, InputValue, OutputKey, OutputValue, CombinerOutput> createTransformationFunction(
            @NotNull CacheConfiguration<InputKey, InputValue> inputData,
            @NotNull CacheConfiguration<OutputKey, CombinerOutput> outputData
    );
}
