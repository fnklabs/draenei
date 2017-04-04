package com.fnklabs.draenei.orm;

import com.fnklabs.metrics.Timer;
import com.google.common.util.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Function for stopping timer on future completion
 *
 * @param <Input> Future type
 */
class FutureTimerCallback<Input> implements FutureCallback<Input> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FutureTimerCallback.class);
    private final Timer timer;

    FutureTimerCallback(Timer timer) {
        this.timer = timer;
    }

    @Override
    public void onSuccess(Input result) {
        timer.stop();
    }

    @Override
    public void onFailure(Throwable t) {
        timer.stop();
        LOGGER.warn("Cant complete operation", t);
    }
}
