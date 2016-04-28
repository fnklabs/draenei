package com.fnklabs.draenei;

import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.fnklabs.metrics.Metrics;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.base.Objects;
import com.google.common.base.Verify;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    private static final Metrics METRICS = MetricsFactory.getMetrics();

    /**
     * Prepared statements map that allow solve problem with several prepared statements execution is same query
     */
    private final ConcurrentHashMap<SessionQuery, PreparedStatement> preparedStatementsMap = new ConcurrentHashMap<>();

    /**
     *
     */
    private final Map<String, Session> sessionsByKeyspace = new ConcurrentHashMap<>();


    private final String defaultKeyspace;

    private final Cluster cluster;

    /**
     * Construct cassandra client
     *
     * @param username        Username
     * @param password        Password
     * @param defaultKeyspace Default keyspace
     * @param hosts           Cassandra nodes
     * @param port            Cassandra port
     *
     * @throws IllegalArgumentException if can't connect to cluster
     */
    public CassandraClient(@Nullable String username,
                           @Nullable String password,
                           @NotNull String defaultKeyspace,
                           @NotNull String hosts,
                           int port) {


        Cluster.Builder builder = Cluster.builder()
                                         .withPort(port)
                                         .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
                                         .withQueryOptions(getQueryOptions())
                                         .withRetryPolicy(new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE))
                                         .withLoadBalancingPolicy(getLoadBalancingPolicy())
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

        this.defaultKeyspace = defaultKeyspace;

        try {
            cluster = builder.build();

            getCluster().init();

            debugClusterInfo(getCluster().getMetadata());

        } catch (IllegalArgumentException e) {
            LOGGER.warn("Cant build cluster", e);
            throw e;
        }
    }

    private static void debugClusterInfo(Metadata metadata) {
        LOGGER.info(String.format("Connecting to cluster: %s", metadata.getClusterName()));

        for (Host host : metadata.getAllHosts()) {
            LOGGER.info(String.format("DataCenter: %s; Host: %s; Rack: %s", host.getDatacenter(), host.getAddress(), host.getRack()));
        }
    }

    public Map<TokenRange, Set<Host>> getTokenRangeOwners(String keyspace) {
        Map<TokenRange, Set<Host>> tokenRangeOwners = new HashMap<>();

        getMembers().stream()
                    .forEach(host -> {
                        Set<TokenRange> tokenRanges = getTokenRanges(host, keyspace);

                        tokenRanges.stream()
                                   .forEach(tokenRange -> {
                                       tokenRangeOwners.compute(tokenRange, (s, hosts) -> {
                                           if (hosts == null) {
                                               hosts = new LinkedHashSet<Host>();
                                           }
                                           hosts.add(host);

                                           return hosts;
                                       });
                                   });
                    });

        return tokenRangeOwners;
    }

    @NotNull
    public KeyspaceMetadata getKeyspaceMetadata(@NotNull String keyspace) {
        return getCluster().getMetadata().getKeyspace(keyspace);
    }

    public String getDefaultKeyspace() {
        return defaultKeyspace;
    }

    public List<String> getKeyspaces() {
        return getCluster().getMetadata()
                           .getKeyspaces()
                           .stream()
                           .map(KeyspaceMetadata::getName)
                           .collect(Collectors.toList());
    }

    @NotNull
    public TableMetadata getTableMetadata(@NotNull String keyspace, @NotNull String tablename) {
        TableMetadata tableMetadata = getKeyspaceMetadata(keyspace).getTable(tablename);

        Verify.verifyNotNull(tableMetadata, String.format("Table metadata is null %s", tablename));

        return tableMetadata;
    }

    @NotNull
    public PreparedStatement prepare(@NotNull String keyspace, @NotNull String query) {
        return preparedStatementsMap.computeIfAbsent(new SessionQuery(keyspace, query), new ComputePreparedStatement());
    }

    /**
     * Execute CQL query
     *
     * @param query CQL query
     *
     * @return Result set
     */
    public ResultSet execute(@NotNull String query) {
        return execute(defaultKeyspace, query);
    }

    /**
     * Execute CQL query
     *
     * @param query CQL query
     *
     * @return Result set
     */
    public ResultSet execute(String keyspace, @NotNull String query) {
        getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_COUNT.name()).inc();

        Timer time = getMetricsFactory().getTimer(MetricsType.CASSANDRA_EXECUTE.name());

        ResultSet resultSet = getOrCreateSession(keyspace).execute(query);
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
        return execute(defaultKeyspace, statement);
    }

    /**
     * Execute statement
     *
     * @param statement Statement
     *
     * @return Execution result set
     */
    public ResultSet execute(String keyspace, @NotNull Statement statement) {
        Timer time = getMetricsFactory().getTimer(MetricsType.CASSANDRA_EXECUTE.name());

        getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_COUNT.name()).inc();

        ResultSet resultSetFuture = getOrCreateSession(keyspace).execute(statement);

        time.stop();

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
        return executeAsync(defaultKeyspace, statement);
    }

    /**
     * Execute statement asynchronously
     *
     * @param statement Statement that must be executed
     *
     * @return ResultSetFuture
     */
    @NotNull
    public ResultSetFuture executeAsync(@NotNull String keyspace, @NotNull Statement statement) {
        Timer time = getMetricsFactory().getTimer(MetricsType.CASSANDRA_EXECUTE_ASYNC.name());

        getMetricsFactory().getCounter(MetricsType.CASSANDRA_PROCESSING_QUERIES.name()).inc();

        ResultSetFuture resultSetFuture = getOrCreateSession(keyspace).executeAsync(statement);

        String query = (statement instanceof BoundStatement) ? ((BoundStatement) statement).preparedStatement().getQueryString() : statement.toString();

        Futures.addCallback(resultSetFuture, new StatementExecutionCallback(keyspace, query));
        monitorFuture(time, resultSetFuture);

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
        return executeAsync(defaultKeyspace, query);
    }

    /**
     * Execute cql query asynchronously
     *
     * @param query CQL query
     *
     * @return ResultSetFuture
     */
    public ResultSetFuture executeAsync(@NotNull String keyspace, @NotNull String query) {
        Timer time = getMetricsFactory().getTimer(MetricsType.CASSANDRA_EXECUTE_ASYNC.name());

        getMetricsFactory().getCounter(MetricsType.CASSANDRA_PROCESSING_QUERIES.name()).inc();

        ResultSetFuture resultSetFuture = getOrCreateSession(keyspace).executeAsync(query);

        Futures.addCallback(resultSetFuture, new StatementExecutionCallback(keyspace, query));

        monitorFuture(time, resultSetFuture);

        return resultSetFuture;
    }

    /**
     * Initiate close cluster and session operations
     */
    public void close() {
        sessionsByKeyspace.entrySet()
                          .forEach(session -> session.getValue().close());

        cluster.close();
    }

    public Set<Host> getMembers() {
        return getCluster().getMetadata().getAllHosts();
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

    @NotNull
    protected LoadBalancingPolicy getLoadBalancingPolicy() {
        RoundRobinPolicy roundRobinPolicy = new RoundRobinPolicy();

        return new TokenAwarePolicy(roundRobinPolicy);
    }

    private Set<TokenRange> getTokenRanges(Host host, String keyspace) {
        Metadata metadata = cluster.getMetadata();

        Set<TokenRange> ranges = metadata.getTokenRanges(keyspace, host);

        return ranges.stream()
                     .flatMap(range -> range.unwrap().stream())
                     .collect(Collectors.toSet());
    }

    private <T> void monitorFuture(@NotNull Timer timer, @NotNull ListenableFuture<T> future) {
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

    private Cluster getCluster() {
        return cluster;
    }

    private Metrics getMetricsFactory() {
        return METRICS;
    }

    /**
     * Create session
     *
     * @param keyspace Default keyspace
     *
     * @return Session instance
     */
    private Session getOrCreateSession(@NotNull String keyspace) {
        Timer timer = getMetricsFactory().getTimer("cassandraClient.getOrCreateSession");

        Session currentSession = sessionsByKeyspace.computeIfAbsent(keyspace, key -> {
            LOGGER.debug("Create session for [{}]", keyspace);

            Session session = getCluster().connect(key);
            session.init();

            return session;
        });

        timer.stop();

        return currentSession;
    }

    private enum MetricsType {
        CASSANDRA_EXECUTE,
        CASSANDRA_QUERIES_COUNT,
        CASSANDRA_QUERIES_ERRORS,
        CASSANDRA_PROCESSING_QUERIES,
        CASSANDRA_EXECUTE_ASYNC, CASSANDRA_PREPARE_STMT,
    }

    private static class SessionQuery {
        private final String keyspace;
        private final String query;

        private SessionQuery(String keyspace, String query) {
            this.keyspace = keyspace;
            this.query = query;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(keyspace, query);
        }

        public String getKeyspace() {
            return keyspace;
        }

        public String getQuery() {
            return query;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof SessionQuery) {
                SessionQuery that = (SessionQuery) obj;

                return Objects.equal(getKeyspace(), that.getKeyspace()) && Objects.equal(getQuery(), that.getQuery());
            }

            return false;
        }
    }

    private class ComputePreparedStatement implements Function<SessionQuery, PreparedStatement> {


        @Override
        public PreparedStatement apply(SessionQuery sessionQuery) {

            Timer timer = getMetricsFactory().getTimer(MetricsType.CASSANDRA_PREPARE_STMT.name());

            try {
                return getOrCreateSession(sessionQuery.getKeyspace()).prepare(sessionQuery.getQuery());
            } catch (Exception e) {
                LOGGER.error("Cant prepare query: " + sessionQuery.getQuery(), e);
                throw e;
            } finally {
                timer.stop();
            }
        }
    }


    private class StatementExecutionCallback implements FutureCallback<ResultSet> {
        private final String keyspace;
        private final String query;

        StatementExecutionCallback(String keyspace, String query) {
            this.keyspace = keyspace;
            this.query = query;
        }

        @Override
        public void onSuccess(ResultSet result) {
            getMetricsFactory().getCounter(MetricsType.CASSANDRA_PROCESSING_QUERIES.name()).dec();
            getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_COUNT.name()).inc();
        }

        @Override
        public void onFailure(Throwable t) {
            LOGGER.warn(String.format("Cant execute bound statement [%S]: %s", keyspace, query), t);

            getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_COUNT.name()).inc();
            getMetricsFactory().getCounter(MetricsType.CASSANDRA_QUERIES_ERRORS.name()).inc();
            getMetricsFactory().getCounter(MetricsType.CASSANDRA_PROCESSING_QUERIES.name()).dec();
        }

    }
}
