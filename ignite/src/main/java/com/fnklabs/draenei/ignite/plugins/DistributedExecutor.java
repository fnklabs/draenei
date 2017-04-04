package com.fnklabs.draenei.ignite.plugins;

import org.apache.ignite.IgniteQueue;

import java.util.concurrent.Executor;

class DistributedExecutor implements Executor {
    private final IgniteQueue<Runnable> queue;

    DistributedExecutor(IgniteQueue<Runnable> queue) {
        this.queue = queue;
    }

    @Override
    public void execute(Runnable command) {
        queue.put(command);
    }
}
