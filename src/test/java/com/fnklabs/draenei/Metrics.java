package com.fnklabs.draenei;

import com.codahale.metrics.*;

import java.util.concurrent.TimeUnit;


public class Metrics implements MetricsFactory {
    private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    private static final Slf4jReporter REPORTER = Slf4jReporter
            .forRegistry(METRIC_REGISTRY)
            .convertRatesTo(TimeUnit.MILLISECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();

    public static void report() {
        REPORTER.report();
    }

    public static Timer getTimer(String type) {
        return METRIC_REGISTRY.timer(type);
    }

    @Override
    public Timer getTimer(MetricsFactory.Type type) {
        return METRIC_REGISTRY.timer(type.toString());
    }

    @Override
    public Counter getCounter(MetricsFactory.Type type) {
        return METRIC_REGISTRY.counter(type.toString());
    }

    @Override
    public Meter getMeter(Type type) {
        return METRIC_REGISTRY.meter(type.toString());
    }

    @Override
    public Histogram getHistogram(Type type) {
        return null;
    }

    static {
        REPORTER.start(5, TimeUnit.SECONDS);
    }


}