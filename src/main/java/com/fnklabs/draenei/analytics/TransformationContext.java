package com.fnklabs.draenei.analytics;

import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.NotNull;

class TransformationContext<InputKey, InputValue, OutputKey, OutputValue> {
    @NotNull
    private final CacheConfiguration<InputKey, InputValue> inputDataSource;

    @NotNull
    private final CacheConfiguration<OutputKey, OutputValue> outputDataSource;

    public TransformationContext(
            @NotNull CacheConfiguration<InputKey, InputValue> inputDataSource,
            @NotNull CacheConfiguration<OutputKey, OutputValue> outputDataSource
    ) {
        this.inputDataSource = inputDataSource;
        this.outputDataSource = outputDataSource;
    }

    @NotNull
    CacheConfiguration<InputKey, InputValue> getInputDataSource() {
        return inputDataSource;
    }

    @NotNull
    CacheConfiguration<OutputKey, OutputValue> getOutputDataSource() {
        return outputDataSource;
    }
}
