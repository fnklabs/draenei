package com.fnklabs.draenei;

import org.apache.ignite.lang.IgniteRunnable;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.UUID;

class IgniteTask implements IgniteRunnable, Serializable {
    @Override
    public void run() {
        LoggerFactory.getLogger(getClass()).debug("Task: {}", UUID.randomUUID());
    }
}
