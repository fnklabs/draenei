package com.fnklabs.draenei.orm.analytics;

import java.io.Serializable;

public interface Reducer<Key, Value, ValueOut> extends Serializable {

    ValueOut reduce(Key key, Iterable<Value> values);
}
