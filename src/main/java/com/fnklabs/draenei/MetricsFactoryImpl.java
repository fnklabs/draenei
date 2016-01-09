package com.fnklabs.draenei;

import com.codahale.metrics.*;

import java.util.concurrent.TimeUnit;


public class MetricsFactoryImpl implements MetricsFactory {
    private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    private static final Slf4jReporter REPORTER = Slf4jReporter.forRegistry(METRIC_REGISTRY)
                                                               .convertRatesTo(TimeUnit.SECONDS)
                                                               .convertDurationsTo(TimeUnit.MILLISECONDS)
                                                               .build();
    private static MetricsFactory METRICS_FACTORY = new MetricsFactoryImpl();

    public static void report() {
        REPORTER.report();
    }

    public static Timer getTimer(String type) {
        return METRIC_REGISTRY.timer(type);
    }

    public static MetricsFactory getInstance() {
        return METRICS_FACTORY;
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