package com.fnklabs.draenei.orm.analytics;

import java.io.Serializable;

public interface Mapper<ValueIn, KeyOut, ValueOut> extends Serializable {
    /**
     * Map operation
     *
     * @param keyIn   Input key
     * @param valueIn Input value
     * @param context Context that can apply new key and value
     */
    void map(long keyIn, ValueIn valueIn, Context<KeyOut, ValueOut> context);
}
