package com.fnklabs.draenei.orm.analytics;

import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

class ResultReducer implements IgniteReducer<Integer, Integer> {
    private AtomicInteger result = new AtomicInteger();

    @Override
    public boolean collect(@Nullable Integer integer) {
        if (integer != null) {
            result.getAndAdd(integer);
        }

        return true;
    }

    @Override
    public Integer reduce() {
        return result.get();
    }
}
