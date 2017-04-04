package com.fnklabs.draenei.ignite.plugins;

public class ExecutorConfiguration {

    private final String name;

    private final int maxThreadsPerNode;

    private final int poolSize;

    public ExecutorConfiguration(String name, int maxThreadsPerNode, int poolSize) {
        this.name = name;
        this.maxThreadsPerNode = maxThreadsPerNode;
        this.poolSize = poolSize;
    }

    public String getName() {
        return name;
    }

    public int getMaxThreadsPerNode() {
        return maxThreadsPerNode;
    }

    public int getPoolSize() {
        return poolSize;
    }
}
