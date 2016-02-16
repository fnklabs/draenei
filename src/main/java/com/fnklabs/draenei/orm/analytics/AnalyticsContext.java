package com.fnklabs.draenei.orm.analytics;

import com.fnklabs.draenei.orm.CassandraClientFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;

/**
 * Analytics context provide basic analytics functionality
 */
public class AnalyticsContext {

    /**
     * Cassandra client factory
     */
    @NotNull
    private final CassandraClientFactory cassandraClientFactory;

    /**
     * Ignite instance
     */
    @NotNull
    private final Ignite ignite;

    /**
     * Construct analytics context
     *
     * @param cassandraClientFactory Cassandra client factory instance
     * @param ignite                 Ignite instance
     */
    public AnalyticsContext(@NotNull CassandraClientFactory cassandraClientFactory, @NotNull Ignite ignite) {
        this.cassandraClientFactory = cassandraClientFactory;
        this.ignite = ignite;
    }


    @NotNull
    protected CassandraClientFactory getCassandraClientFactory() {
        return cassandraClientFactory;
    }

    @NotNull
    protected Ignite getIgnite() {
        return ignite;
    }

}
