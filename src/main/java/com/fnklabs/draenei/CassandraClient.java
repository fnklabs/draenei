package com.fnklabs.draenei;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import com.ecyrd.speed4j.StopWatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class CassandraClient {
    public static final int RECONNECTION_DELAY_TIME = 5000;
    /**
     * Read timeout in ms
     */
    private static final int READ_TIMEOUT = 10000;
    /**
     * Read timeout in ms
     */
    private static final int CONNECT_TIMEOUT_MILLIS = 30000;
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClient.class);
    /**
     * Cassandra cluster instance
     */
    private final Cluster cluster;

    /**
     * Prepared statements map that allow solve problem with several prepared statements execution is same query
     */
    private final ConcurrentHashMap<String, PreparedStatement> preparedStatementsMap = new ConcurrentHashMap<String, PreparedStatement>();

    /**
     * Cassandra session instance
     */
    private final Session session;

    private final ListeningExecutorService executorService;

    private final MetricsFactory metricsFactory;

    /**
     * Construct cassandra client
     *
     * @param username        Username
     * @param password        Password
     * @param keyspace        Default keyspace
     * @param hosts           Cassandra nodes
     * @param metricsFactory  Metrics Factory that will be used for metrics
     * @param executorService Client executor service
     * @param hostDistance    Cassandra host distance
     *
     * @throws IllegalArgumentException if can't connect to cluster
     */
    public CassandraClient(@NotNull String username,
                           @NotNull String password,
                           @NotNull String keyspace,
                           @NotNull String hosts,
                           @NotNull MetricsFactory metricsFactory,
                           @NotNull ListeningExecutorService executorService,
                           @NotNull HostDistance hostDistance) {

        this.metricsFactory = metricsFactory;
        this.executorService = executorService;

        Cluster.Builder builder = Cluster.builder()
                                         .withPort(9042)
                                         .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
                                         .withQueryOptions(getQueryOptions())
                                         .withRetryPolicy(new LoggingRetryPolicy(FallthroughRetryPolicy.INSTANCE))
                                         .withLoadBalancingPolicy(getLoadBalancingPolicy(hostDistance))
                                         .withReconnectionPolicy(new ConstantReconnectionPolicy(RECONNECTION_DELAY_TIME))
                                         .withSocketOptions(getSocketOptions());

        if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
            builder = builder.withCredentials(username, password);
        }

        String[] hostList = StringUtils.split(hosts, ",");

        LOGGER.info("Cassandra nodes: {}", hostList);

        for (String host : hostList) {
            builder.addContactPoint(host);
        }

        try {
            cluster = builder.build();
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Cant build cluster", e);
            throw e;
        }

        Metadata metadata = cluster.getMetadata();

        LOGGER.info(String.format("Connect to cluster: %s", metadata.getClusterName()));

        for (Host host : metadata.getAllHosts()) {
            LOGGER.info(String.format("DataCenter: %s; Host: %s; Rack: %s", host.getDatacenter(), host.getAddress(), host.getRack()));
        }

        cluster.init();

        session = createSession(cluster, keyspace);
    }

    @NotNull
    public KeyspaceMetadata getKeyspaceMetadata(@NotNull String keyspace) {
        return cluster.getMetadata().getKeyspace(keyspace);
    }

    @NotNull
    public TableMetadata getTableMetadata(@NotNull String tablename) {
        return getKeyspaceMetadata(getSession().getLoggedKeyspace()).getTable(tablename);
    }

    @NotNull
    public TableMetadata getTableMetadata(@NotNull String keyspace, @NotNull String tablename) {
        return getKeyspaceMetadata(keyspace).getTable(tablename);
    }


    public void dumpMetrics() {
        Metrics metrics = cluster.getMetrics();

        Metrics.Errors errorMetrics = metrics.getErrorMetrics();

        LOGGER.warn("Session connections: {}\n" +
                        "Queries: {} Errors: {}\n" +
                        "Connected to host: {}\n" +
                        "Known hosts: {}\n" +
                        "Request timer: {}\n" +
                        "Error metrics\n" +
                        "Connection errors: {}" +
                        "\nWrite timeouts: {} Read timeouts: {} Unavailables: {} Other: {}" +
                        "\nRetries: {} Retries on write timeout: {} Retries on read timeout: {} Retries on unavailable {}" +
                        "\nIgnores: {} Ignores on write timeout: {} Ignores on read timeout: {} Ignore on unavailable {}",
                metrics.getOpenConnections().getValue(),
                getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_COUNT).getCount(), getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_ERRORS).getCount(),
                metrics.getConnectedToHosts().getValue(),
                metrics.getKnownHosts().getValue(),
                metrics.getRequestsTimer().getOneMinuteRate(),


                errorMetrics.getConnectionErrors().getCount(),
                errorMetrics.getWriteTimeouts().getCount(), errorMetrics.getReadTimeouts().getCount(), errorMetrics.getUnavailables().getCount(), errorMetrics.getOthers()
                                                                                                                                                              .getCount(),
                errorMetrics.getRetries().getCount(), errorMetrics.getRetriesOnWriteTimeout().getCount(), errorMetrics.getRetriesOnReadTimeout()
                                                                                                                      .getCount(), errorMetrics.getRetriesOnUnavailable()
                                                                                                                                               .getCount(),
                errorMetrics.getIgnores().getCount(), errorMetrics.getIgnoresOnWriteTimeout().getCount(), errorMetrics.getIgnoresOnReadTimeout()
                                                                                                                      .getCount(), errorMetrics.getIgnoresOnUnavailable()
                                                                                                                                               .getCount());


        LOGGER.warn("Active statements: {}", getMetricsFactory().getCounter(MetricsType.CASSANDRA_PROCESSING_QUERIES).getCount());
    }

    @NotNull
    public PreparedStatement prepare(@NotNull String query) {
        return preparedStatementsMap.compute(query, new ComputePreparedStatement());
    }

    public ResultSet execute(String query) {
        getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_COUNT).inc();

        return getSession().execute(query);
    }

    public ResultSetFuture executeAsync(String query) throws IllegalStateException {
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.CASSANDRA_EXECUTE).time();

        getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_COUNT).inc();
        getMetricsFactory().getCounter(MetricsType.CASSANDRA_PROCESSING_QUERIES).inc();

        ResultSetFuture resultSetFuture = getSession().executeAsync(query);
        Futures.addCallback(resultSetFuture, new StatementExecutionCallback(query), executorService);

        monitorFuture(time, resultSetFuture);

        return resultSetFuture;
    }

    public ResultSetFuture executeAsync(BoundStatement boundStatement) throws IllegalStateException {
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.CASSANDRA_EXECUTE).time();

        getMetricsFactory().getCounter(MetricsType.CASSANDRA_PROCESSING_QUERIES).inc();
        getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_COUNT).inc();

        ResultSetFuture resultSetFuture = getSession().executeAsync(boundStatement);

        Futures.addCallback(resultSetFuture, new StatementExecutionCallback(boundStatement.preparedStatement().getQueryString()), executorService);
        monitorFuture(time, resultSetFuture);

        return resultSetFuture;
    }

    /**
     * Initiate close cluster and session operations
     */
    public void close() {
        session.close();
        cluster.close();
    }

    /**
     * Get Cluster session
     *
     * @return Session instance
     */
    @NotNull
    public Session getSession() {
        return session;
    }

    /**
     * Execute statement
     *
     * @param statement Statement that must be executed
     *
     * @return ResultSetFuture
     */
    @NotNull
    public ResultSetFuture executeAsync(Statement statement) {
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.CASSANDRA_EXECUTE).time();

        getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_COUNT).inc();

        ResultSetFuture resultSetFuture = getSession().executeAsync(statement);

        Futures.addCallback(resultSetFuture, new StatementExecutionCallback(statement.getKeyspace()), executorService);
        monitorFuture(time, resultSetFuture);

        return resultSetFuture;
    }

    /**
     * Create session
     *
     * @param cluster  Cluster instance
     * @param keyspace Default keyspace
     *
     * @return Session instance
     */
    private Session createSession(@NotNull Cluster cluster, @NotNull String keyspace) {
        Session session = cluster.connect(keyspace);
        session.init();

        return session;
    }

    private MetricsFactory getMetricsFactory() {
        return metricsFactory;
    }

    private <T> void monitorFuture(Timer.Context timer, com.google.common.util.concurrent.ListenableFuture<T> future) {
        Futures.addCallback(future, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                timer.stop();
            }

            @Override
            public void onFailure(Throwable t) {
                timer.stop();
                LOGGER.warn("Cant complete operation", t);
            }
        }, executorService);
    }

    private enum MetricsType implements MetricsFactory.Type {
        CASSANDRA_EXECUTOR_QUEUE_SIZE,
        CASSANDRA_EXECUTE,
        CASSANDRA_QUERIES_COUNT,
        CASSANDRA_QUERIES_ERRORS,
        CASSANDRA_PROCESSING_QUERIES,
    }

    @NotNull
    private static QueryOptions getQueryOptions() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM);
        return queryOptions;
    }

    @NotNull
    private static SocketOptions getSocketOptions() {
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis(CONNECT_TIMEOUT_MILLIS);
        socketOptions.setReadTimeoutMillis(READ_TIMEOUT);
        socketOptions.setKeepAlive(true);
        socketOptions.setTcpNoDelay(true);
        return socketOptions;
    }

    @NotNull
    private static LoadBalancingPolicy getLoadBalancingPolicy(@NotNull final HostDistance hostDistance) {
        LatencyAwarePolicy.Builder latencyPolicyBuilder = LatencyAwarePolicy.builder(new RoundRobinPolicy() {
            @Override
            public HostDistance distance(Host host) {
                return hostDistance;
            }
        });

        LatencyAwarePolicy latencyAwarePolicy = latencyPolicyBuilder.build();

        return new TokenAwarePolicy(latencyAwarePolicy);
    }

    private class ComputePreparedStatement implements BiFunction<String, PreparedStatement, PreparedStatement> {

        @Override
        public PreparedStatement apply(String s, PreparedStatement preparedStatement) {
            if (preparedStatement == null) {
                preparedStatement = getSession().prepare(s);
            }

            return preparedStatement;
        }
    }

    private class StatementExecutionCallback implements FutureCallback<ResultSet> {
        private StopWatch stopWatch;

        public StatementExecutionCallback(String query) {
            this.stopWatch = new StopWatch(query);
        }

        @Override
        public void onSuccess(ResultSet result) {
            decrementActiveStatements();
        }

        @Override
        public void onFailure(Throwable t) {
            LOGGER.warn("Cant execute bound statement. " + stopWatch.toString(), t);

            getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_ERRORS).inc();
            decrementActiveStatements();
        }

        protected void decrementActiveStatements() {
            getMetricsFactory().getCounter(MetricsType.CASSANDRA_PROCESSING_QUERIES).inc();
        }
    }
}
