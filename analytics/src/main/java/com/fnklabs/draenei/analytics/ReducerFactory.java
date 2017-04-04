package com.fnklabs.draenei.analytics;

import org.apache.ignite.configuration.CacheConfiguration;

/**
 * @param <InputKey>
 * @param <InputValue>
 * @param <OutputKey>
 * @param <OutputValue>
 */
public abstract class ReducerFactory<InputKey, InputValue, OutputKey, OutputValue, CombinerOutput> extends TransformationFactory<InputKey, InputValue, OutputKey, OutputValue, CombinerOutput> {


    @Override
    protected abstract ReducerTask<InputKey, InputValue, OutputKey, OutputValue, CombinerOutput> createTransformationFunction(
            CacheConfiguration<InputKey, InputValue> inputData,
            CacheConfiguration<OutputKey, CombinerOutput> outputData
    );
}
