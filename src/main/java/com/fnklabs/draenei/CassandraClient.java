package com.fnklabs.draenei;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import com.fnklabs.metrics.Metrics;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class CassandraClient {
    private static final int RECONNECTION_DELAY_TIME = 5000;
    /**
     * Read timeout in ms
     */
    private static final int READ_TIMEOUT = 15000;
    /**
     * Connection timeout in ms
     */
    private static final int CONNECT_TIMEOUT_MILLIS = 30000;

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClient.class);
    private static final com.fnklabs.metrics.Metrics METRICS = MetricsFactory.getMetrics();
    /**
     * Prepared statements map that allow solve problem with several prepared statements execution is same query
     */
    private final ConcurrentHashMap<String, PreparedStatement> preparedStatementsMap = new ConcurrentHashMap<String, PreparedStatement>();
    /**
     * Cassandra session instance
     */
    private final Session session;

    /**
     * Construct cassandra client
     *
     * @param username     Username
     * @param password     Password
     * @param keyspace     Default keyspace
     * @param hosts        Cassandra nodes
     * @param hostDistance Cassandra host distance
     *
     * @throws IllegalArgumentException if can't connect to cluster
     */
    public CassandraClient(@NotNull String username,
                           @NotNull String password,
                           @NotNull String keyspace,
                           @NotNull String hosts,
                           @NotNull HostDistance hostDistance) {


        Cluster.Builder builder = Cluster.builder()
                                         .withPort(9042)
                                         .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
//                                         .withCompression(ProtocolOptions.Compression.SNAPPY)
                                         .withQueryOptions(getQueryOptions())
                                         .withRetryPolicy(new LoggingRetryPolicy(FallthroughRetryPolicy.INSTANCE))
                                         .withLoadBalancingPolicy(getLoadBalancingPolicy(hostDistance))
                                         .withReconnectionPolicy(new ConstantReconnectionPolicy(RECONNECTION_DELAY_TIME))
                                         .withPoolingOptions(getPoolingOptions())
                                         .withSocketOptions(getSocketOptions())
                                         .withTimestampGenerator(new AtomicMonotonicTimestampGenerator());

        if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
            builder = builder.withCredentials(username, password);
        }

        String[] hostList = StringUtils.split(hosts, ",");

        LOGGER.info("Cassandra nodes: {}", hosts);

        for (String host : hostList) {
            builder.addContactPoint(host);
        }

        try {
            Cluster cluster = builder.build();

            Metadata metadata = cluster.getMetadata();

            LOGGER.info(String.format("Connecting to cluster: %s", metadata.getClusterName()));

            for (Host host : metadata.getAllHosts()) {
                LOGGER.info(String.format("DataCenter: %s; Host: %s; Rack: %s", host.getDatacenter(), host.getAddress(), host.getRack()));
            }

            cluster.init();

            session = createSession(cluster, keyspace);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Cant build cluster", e);
            throw e;
        }
    }

    public CassandraClient(@NotNull Session session) {
        this.session = session;
    }

    @NotNull
    public KeyspaceMetadata getKeyspaceMetadata(@NotNull String keyspace) {
        return getSession().getCluster().getMetadata().getKeyspace(keyspace);
    }

    @NotNull
    public TableMetadata getTableMetadata(@NotNull String tablename) {
        return getKeyspaceMetadata(getSession().getLoggedKeyspace()).getTable(tablename);
    }

    @NotNull
    public TableMetadata getTableMetadata(@NotNull String keyspace, @NotNull String tablename) {
        return getKeyspaceMetadata(keyspace).getTable(tablename);
    }

    @NotNull
    public PreparedStatement prepare(@NotNull String query) {
        return preparedStatementsMap.compute(query, new ComputePreparedStatement());
    }

    /**
     * Execute CQL query
     *
     * @param query CQL query
     *
     * @return Result set
     */
    public ResultSet execute(@NotNull String query) {
        getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_COUNT.name()).inc();
        Timer time = getMetricsFactory().getTimer(MetricsType.CASSANDRA_EXECUTE.name());

        ResultSet resultSet = getSession().execute(query);
        time.stop();

        return resultSet;
    }

    /**
     * Execute statement
     *
     * @param statement Statement
     *
     * @return Execution result set
     */
    public ResultSet execute(@NotNull Statement statement) {
        Timer time = getMetricsFactory().getTimer(MetricsType.CASSANDRA_EXECUTE.name());

        getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_COUNT.name()).inc();

        ResultSet resultSetFuture = getSession().execute(statement);

        time.stop();

        return resultSetFuture;
    }

    /**
     * Execute cql query asynchronously
     *
     * @param query CQL query
     *
     * @return ResultSetFuture
     */
    public ResultSetFuture executeAsync(@NotNull String query) {
        Timer time = getMetricsFactory().getTimer(MetricsType.CASSANDRA_EXECUTE_ASYNC.name());

        getMetricsFactory().getCounter(MetricsType.CASSANDRA_PROCESSING_QUERIES.name()).inc();

        ResultSetFuture resultSetFuture = getSession().executeAsync(query);

        Futures.addCallback(resultSetFuture, new StatementExecutionCallback(query));

        monitorFuture(time, resultSetFuture);

        return resultSetFuture;
    }

    /**
     * Execute statement asynchronously
     *
     * @param statement Statement that must be executed
     *
     * @return ResultSetFuture
     */
    @NotNull
    public ResultSetFuture executeAsync(@NotNull Statement statement) {
        Timer time = getMetricsFactory().getTimer(MetricsType.CASSANDRA_EXECUTE_ASYNC.name());

        getMetricsFactory().getCounter(MetricsType.CASSANDRA_PROCESSING_QUERIES.name()).inc();

        ResultSetFuture resultSetFuture = getSession().executeAsync(statement);

        String query = (statement instanceof BoundStatement) ? ((BoundStatement) statement).preparedStatement().getQueryString() : statement.toString();

        Futures.addCallback(resultSetFuture, new StatementExecutionCallback(query));
        monitorFuture(time, resultSetFuture);

        return resultSetFuture;
    }

    /**
     * Initiate close cluster and session operations
     */
    public void close() {
        session.close();
        session.getCluster().close();
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

    public Set<Host> getMembers() {
        return getSession().getCluster().getMetadata().getAllHosts();
    }

    protected <T> void monitorFuture(@NotNull Timer timer, @NotNull ListenableFuture<T> future) {
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
        });
    }

    @NotNull
    protected SocketOptions getSocketOptions() {
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis(CONNECT_TIMEOUT_MILLIS);
        socketOptions.setReadTimeoutMillis(READ_TIMEOUT);
        socketOptions.setKeepAlive(true);
        socketOptions.setTcpNoDelay(true);
        return socketOptions;
    }

    @NotNull
    protected QueryOptions getQueryOptions() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM);
        return queryOptions;
    }

    @NotNull
    protected PoolingOptions getPoolingOptions() {
        return new PoolingOptions();
    }

    private Metrics getMetricsFactory() {
        return METRICS;
    }

    private enum MetricsType {
        CASSANDRA_EXECUTE,
        CASSANDRA_QUERIES_COUNT,
        CASSANDRA_QUERIES_ERRORS,
        CASSANDRA_PROCESSING_QUERIES,
        CASSANDRA_EXECUTE_ASYNC, CASSANDRA_PREPARE_STMT,
    }

    @NotNull
    protected static LoadBalancingPolicy getLoadBalancingPolicy(@NotNull final HostDistance hostDistance) {
        RoundRobinPolicy roundRobinPolicy = new RoundRobinPolicy() {
            @Override
            public HostDistance distance(Host host) {
                return hostDistance;
            }
        };

        return new TokenAwarePolicy(roundRobinPolicy);
    }

    /**
     * Create session
     *
     * @param cluster  Cluster instance
     * @param keyspace Default keyspace
     *
     * @return Session instance
     */
    private static Session createSession(@NotNull Cluster cluster, @NotNull String keyspace) {
        Session session = cluster.connect(keyspace);
        session.init();

        return session;
    }

    private class ComputePreparedStatement implements BiFunction<String, PreparedStatement, PreparedStatement> {

        @Override
        public PreparedStatement apply(String s, PreparedStatement preparedStatement) {
            if (preparedStatement == null) {
                Timer timer = getMetricsFactory().getTimer(MetricsType.CASSANDRA_PREPARE_STMT.name());
                try {
                    preparedStatement = getSession().prepare(s);
                } catch (Exception e) {
                    LOGGER.error("Cant prepare query: " + s, e);
                    throw e;
                }
                timer.stop();
            }

            return preparedStatement;
        }
    }

    private class StatementExecutionCallback implements FutureCallback<ResultSet> {
        private final String query;

        public StatementExecutionCallback(String query) {
            this.query = query;
        }

        @Override
        public void onSuccess(ResultSet result) {
            getMetricsFactory().getCounter(MetricsType.CASSANDRA_PROCESSING_QUERIES.name()).dec();
            getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_COUNT.name()).inc();
        }

        @Override
        public void onFailure(Throwable t) {
            LOGGER.warn("Cant execute bound statement: " + query, t);

            getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_COUNT.name()).inc();
            getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_ERRORS.name()).inc();
            getMetricsFactory().getCounter(MetricsType.CASSANDRA_PROCESSING_QUERIES.name()).dec();
        }

    }
}
