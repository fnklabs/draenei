package com.fnklabs.draenei.orm.analytics;

public interface MapFunction<KeyIn, ValueIn, Output> {
    Output map(KeyIn key, ValueIn entry);
}
