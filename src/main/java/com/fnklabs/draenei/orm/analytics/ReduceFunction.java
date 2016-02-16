package com.fnklabs.draenei.orm.analytics;

import org.jetbrains.annotations.Nullable;

import java.util.List;

public interface ReduceFunction<ValueIn, ValueOut> {
    ValueOut reduce(@Nullable List<ValueIn> values);
}
