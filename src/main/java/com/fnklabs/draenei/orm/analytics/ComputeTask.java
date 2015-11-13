package com.fnklabs.draenei.orm.analytics;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;

public interface ComputeTask<ValueIn, KeyOut, ValueOut, ReducerValueOut> extends Serializable {
    @NotNull
    Mapper<ValueIn, KeyOut, ValueOut> getMapper();

    @Nullable
    Reducer<KeyOut, ValueOut, ReducerValueOut> getReducer();

}
