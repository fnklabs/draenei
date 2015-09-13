package com.fnklabs.draenei;


import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

public interface MetricsFactory {
    Timer getTimer(Type type);

    Counter getCounter(Type type);

    Meter getMeter(Type type);

    Histogram getHistogram(Type type);

    interface Type {

    }
}
