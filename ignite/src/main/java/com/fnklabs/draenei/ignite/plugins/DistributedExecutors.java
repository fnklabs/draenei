package com.fnklabs.draenei.ignite.plugins;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.plugin.IgnitePlugin;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DistributedExecutors implements IgnitePlugin, Closeable {
    private final Ignite ignite;

    private final Map<String, ThreadPoolExecutor> executors = new ConcurrentHashMap<>();

    private final Map<String, IgniteQueue<Runnable>> runnableQueues = new ConcurrentHashMap<>();

    DistributedExecutors(Ignite ignite) {
        this.ignite = ignite;
    }

    public Executor get(ExecutorConfiguration executorConfiguration) {
        IgniteQueue<Runnable> queue = runnableQueues.computeIfAbsent(executorConfiguration.getName(), name -> {
            return ignite.queue(executorConfiguration.getName(), executorConfiguration.getPoolSize(), getCfg());
        });

        ThreadPoolExecutor executor = executors.computeIfAbsent(executorConfiguration.getName(), k -> {
            return new ThreadPoolExecutor(
                    executorConfiguration.getMaxThreadsPerNode(),
                    executorConfiguration.getMaxThreadsPerNode(),
                    Integer.MAX_VALUE,
                    TimeUnit.DAYS,
                    queue,
                    new NamedThreadFactory(executorConfiguration.getName()),
                    new ThreadPoolExecutor.AbortPolicy()

            );
        });


        return new DistributedExecutor(queue);
    }

    @Override
    public void close() throws IOException {
        executors.forEach((k, v) -> {
            v.shutdown();

            LoggerFactory.getLogger(DistributedExecutors.class).info("Shutdown executor: {}", k);
        });

        runnableQueues.forEach((k, v) -> {
            v.close();

            LoggerFactory.getLogger(DistributedExecutors.class).info("Shutdown executor queue: {}", k);
        });
    }


    private CollectionConfiguration getCfg() {
        CollectionConfiguration collectionConfiguration = new CollectionConfiguration();
        collectionConfiguration.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        collectionConfiguration.setCacheMode(CacheMode.PARTITIONED);
        collectionConfiguration.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
        collectionConfiguration.setBackups(1);
        collectionConfiguration.setCollocated(true);
        collectionConfiguration.setOffHeapMaxMemory(256 * 1024 * 1024); // 256 MB

        return collectionConfiguration;
    }

    private static class NamedThreadFactory implements ThreadFactory {

        private static final String NAME_PATTERN = "%s-%d";

        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String threadNamePrefix;

        /**
         * Creates a new {@link org.apache.lucene.util.NamedThreadFactory} instance
         *
         * @param threadNamePrefix the name prefix assigned to each thread created.
         */
        NamedThreadFactory(String threadNamePrefix) {
            final SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.threadNamePrefix = threadNamePrefix;
        }

        /**
         * Creates a new {@link Thread}
         *
         * @see ThreadFactory#newThread(Runnable)
         */
        @Override
        public Thread newThread(Runnable r) {
            final Thread t = new Thread(group, r, String.format(NAME_PATTERN, threadNamePrefix, threadNumber.getAndIncrement()), 0);
            t.setDaemon(false);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }

    }
}
