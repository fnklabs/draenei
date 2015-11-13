package com.fnklabs.draenei.orm.analytics;

import java.io.Serializable;

public interface Mapper<ValueIn, KeyOut, ValueOut> extends Serializable {
    /**
     * @param keyIn
     * @param valueIn
     * @param context
     */
    void map(long keyIn, ValueIn valueIn, Context<KeyOut, ValueOut> context);
}
