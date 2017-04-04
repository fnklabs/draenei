package com.fnklabs.draenei.analytics;

import org.apache.ignite.configuration.CacheConfiguration;

class TransformationContext<InputKey, InputValue, OutputKey, OutputValue> {

    private final CacheConfiguration<InputKey, InputValue> inputDataSource;


    private final CacheConfiguration<OutputKey, OutputValue> outputDataSource;

    public TransformationContext(
            CacheConfiguration<InputKey, InputValue> inputDataSource,
            CacheConfiguration<OutputKey, OutputValue> outputDataSource
    ) {
        this.inputDataSource = inputDataSource;
        this.outputDataSource = outputDataSource;
    }


    CacheConfiguration<InputKey, InputValue> getInputDataSource() {
        return inputDataSource;
    }


    CacheConfiguration<OutputKey, OutputValue> getOutputDataSource() {
        return outputDataSource;
    }
}
