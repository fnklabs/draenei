package com.fnklabs.draenei.analytics;

import org.apache.ignite.configuration.CacheConfiguration;

public abstract class MapTask<InputKey, InputValue, OutputKey, OutputValue, CombinerOutputValue> extends TransformationFunction<InputKey, InputValue, OutputKey, OutputValue, CombinerOutputValue> {

    protected MapTask(
            CacheConfiguration<InputKey, InputValue> inputData,
            CacheConfiguration<OutputKey, CombinerOutputValue> outputData
    ) {
        super(inputData, outputData);
    }

    protected abstract void map(InputKey key, InputValue value, Emitter<OutputKey, OutputValue, CombinerOutputValue> emitter);

    @Override
    protected void transform(InputKey key, InputValue value, Emitter<OutputKey, OutputValue, CombinerOutputValue> emitter) {
        map(key, value, emitter);
    }
}
