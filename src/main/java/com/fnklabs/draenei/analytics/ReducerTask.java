package com.fnklabs.draenei.analytics;

import org.apache.ignite.configuration.CacheConfiguration;

public abstract class ReducerTask<InputKey, InputValue, OutputKey, OutputValue, CombinerOutputValue> extends TransformationFunction<InputKey, InputValue, OutputKey, OutputValue, CombinerOutputValue> {

    protected ReducerTask(CacheConfiguration<InputKey, InputValue> inputData, CacheConfiguration<OutputKey, CombinerOutputValue> outputData) {
        super(inputData, outputData);
    }

    protected abstract void reducer(InputKey key, InputValue value, Emitter<OutputKey, OutputValue, CombinerOutputValue> emitter);

    @Override
    protected void transform(InputKey key, InputValue value, Emitter<OutputKey, OutputValue, CombinerOutputValue> emitter) {
        reducer(key, value, emitter);
    }
}
