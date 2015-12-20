package com.fnklabs.draenei;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceFactory {
    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final int CORE_POOL_SIZE = AVAILABLE_PROCESSORS;
    private static final int MAX_POOL_SIZE = AVAILABLE_PROCESSORS;

    public static final ExecutorService DEFAULT_EXECUTOR = new ThreadPoolExecutor(
            CORE_POOL_SIZE,
            MAX_POOL_SIZE,
            0L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(Integer.MAX_VALUE),
            new ThreadFactory("draenei-executor-"),
            new ThreadPoolExecutor.CallerRunsPolicy()
    );
}
