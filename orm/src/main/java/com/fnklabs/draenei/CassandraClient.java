package com.fnklabs.draenei;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import com.fnklabs.draenei.orm.DateTimeCodec;
import com.fnklabs.metrics.Metrics;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.base.Objects;
import com.google.common.base.Verify;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CassandraClient {
    private static final int RECONNECTION_DELAY_TIME = 5000;
    /**
     * Read timeout in ms
     */
    private static final int READ_TIMEOUT = 60000;
    /**
     * Connection timeout in ms
     */
    private static final int CONNECT_TIMEOUT_MILLIS = 120000;

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClient.class);

    private static final Metrics METRICS = MetricsFactory.getMetrics();

    /**
     * Prepared statements map that allow solve problem with several prepared statements execution is same query
     */
    private final ConcurrentHashMap<PreparedStmtKey, PreparedStatement> preparedStatementsMap = new ConcurrentHashMap<>();

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
     *
     * @throws IllegalArgumentException if can't connect to cluster
     */
    public CassandraClient(@Nullable String username,
                           @Nullable String password,
                           String defaultKeyspace,
                           String hosts) {
        this(username, password, defaultKeyspace, hosts, 9042);
    }

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
                           String defaultKeyspace,
                           String hosts,
                           int port) {

        Cluster.Builder builder = Cluster.builder()
                                         .withPort(port)
                                         .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
                                         .withCodecRegistry(CodecRegistry.DEFAULT_INSTANCE.register(new DateTimeCodec()))
                                         .withQueryOptions(getQueryOptions())
                                         .withRetryPolicy(new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE))
                                         .withLoadBalancingPolicy(getLoadBalancingPolicy())
                                         .withReconnectionPolicy(new ConstantReconnectionPolicy(RECONNECTION_DELAY_TIME))
                                         .withPoolingOptions(getPoolingOptions())
                                         .withSocketOptions(getSocketOptions())
                                         .withTimestampGenerator(new AtomicMonotonicTimestampGenerator());

        if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
            builder.withCredentials(username, password);
        }

        String[] hostList = StringUtils.split(hosts, ",");

        LOGGER.info("Cassandra nodes: {}", hosts);

        for (String host : hostList) {
            builder.addContactPoint(host);
        }

        this.defaultKeyspace = defaultKeyspace;

        try {
            cluster = builder.build().init();

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

    /**
     * Get token ranges by host for keyspace
     *
     * @param keyspace Keyspace name
     *
     * @return TokenRanges by host
     */
    public Map<TokenRange, Set<HostAndPort>> getTokensOwner(String keyspace) {
        Map<TokenRange, Set<HostAndPort>> tokenRanges = new HashMap<>();

        for (Host host : getMembers()) {
            Set<TokenRange> tokensRange = getTokenRanges(host, keyspace);

            tokensRange.forEach(tokenRange -> {
                tokenRanges.compute(tokenRange, (key, value) -> {
                    if (value == null) {
                        value = new HashSet<>();
                    }

                    value.add(HostAndPort.fromHost(host.getAddress().getHostAddress()));

                    return value;
                });
            });
        }

        LOGGER.debug("Token ranges by host: {}", tokenRanges);

        return tokenRanges;
    }

    public KeyspaceMetadata getKeyspaceMetadata(String keyspace) {
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

    public TableMetadata getTableMetadata(String keyspace, String tablename) {
        TableMetadata tableMetadata = getKeyspaceMetadata(keyspace).getTable(tablename);

        Verify.verifyNotNull(tableMetadata, String.format("Table metadata is null `%s`.`%s`", keyspace, tablename));

        return tableMetadata;
    }

    public PreparedStatement prepare(String keyspace, String query) {
        return preparedStatementsMap.computeIfAbsent(new PreparedStmtKey(keyspace, query), new ComputePreparedStatement());
    }

    /**
     * Execute CQL query
     *
     * @param query CQL query
     *
     * @return Result set
     */
    public ResultSet execute(String query) {
        return execute(defaultKeyspace, query);
    }

    /**
     * Execute CQL query
     *
     * @param query CQL query
     *
     * @return Result set
     */
    public ResultSet execute(String keyspace, String query) {
        getMetricsFactory().getCounter(MetricType.QUERIES_COUNT).inc();

        return execute(keyspace, new SimpleStatement(query));
    }

    /**
     * Execute statement
     *
     * @param statement Statement
     *
     * @return Execution result set
     */
    public ResultSet execute(Statement statement) {
        return execute(defaultKeyspace, statement);
    }

    /**
     * Execute statement
     *
     * @param statement Statement
     *
     * @return Execution result set
     */
    public ResultSet execute(String keyspace, Statement statement) {
        try (Timer timer = MetricsFactory.getMetrics().getTimer(MetricType.QUERY_EXECUTE)) {
            return getOrCreateSession(keyspace).execute(statement);
        } catch (IOException e) {
            throw new QueryException(e);
        }
    }

    /**
     * Execute statement asynchronously
     *
     * @param statement Statement that must be executed
     *
     * @return ResultSetFuture
     */

    public ResultSetFuture executeAsync(Statement statement) {
        return executeAsync(defaultKeyspace, statement);
    }

    /**
     * Execute statement asynchronously
     *
     * @param statement Statement that must be executed
     *
     * @return ResultSetFuture
     */

    public ResultSetFuture executeAsync(String keyspace, Statement statement) {
        Timer timer = getMetricsFactory().getTimer(MetricType.QUERY_EXECUTE_ASYNC);

        getMetricsFactory().getCounter(MetricType.QUERIES_PROCESSING).inc();

        ResultSetFuture resultSetFuture = getOrCreateSession(keyspace).executeAsync(statement);

        String query = (statement instanceof BoundStatement) ? ((BoundStatement) statement).preparedStatement().getQueryString() : statement.toString();

        Futures.addCallback(resultSetFuture, new StatementExecutionCallback(keyspace, query));
        stopTimer(resultSetFuture, timer);

        return resultSetFuture;
    }

    /**
     * Execute cql query asynchronously
     *
     * @param query CQL query
     *
     * @return ResultSetFuture
     */
    public ResultSetFuture executeAsync(String query) {
        return executeAsync(defaultKeyspace, query);
    }

    /**
     * Execute cql query asynchronously
     *
     * @param query CQL query
     *
     * @return ResultSetFuture
     */
    public ResultSetFuture executeAsync(String keyspace, String query) {
        return executeAsync(keyspace, new SimpleStatement(query));
    }

    /**
     * Initiate close cluster and session operations
     */
    public void close() {
        sessionsByKeyspace.forEach((key, value) -> value.close());

        cluster.close();
    }

    public Set<Host> getMembers() {
        return getCluster().getMetadata().getAllHosts();
    }

    protected SocketOptions getSocketOptions() {
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis(CONNECT_TIMEOUT_MILLIS);
        socketOptions.setReadTimeoutMillis(READ_TIMEOUT);
        socketOptions.setKeepAlive(true);
        socketOptions.setTcpNoDelay(true);
        return socketOptions;
    }

    protected QueryOptions getQueryOptions() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM);
        return queryOptions;
    }

    protected PoolingOptions getPoolingOptions() {
        return new PoolingOptions();
    }

    protected LoadBalancingPolicy getLoadBalancingPolicy() {
        RoundRobinPolicy roundRobinPolicy = new RoundRobinPolicy();

        return new TokenAwarePolicy(roundRobinPolicy);
    }

    /**
     * Get keyspace token ranges that belongs to provided host
     *
     * @param host     Host
     * @param keyspace Keyspace
     *
     * @return Set of token ranges
     */
    private Set<TokenRange> getTokenRanges(Host host, String keyspace) {
        return getCluster().getMetadata()
                           .getTokenRanges(keyspace, host)
                           .stream()
                           .flatMap(range -> range.unwrap().stream())
                           .collect(Collectors.toSet());
    }

    /**
     * Stop timer on future completion
     *
     * @param future Future
     * @param timer  Timer that myst be stopped
     * @param <T>    Future type
     */
    private <T> void stopTimer(ListenableFuture<T> future, Timer timer) {
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
    private Session getOrCreateSession(String keyspace) {
        try (Timer timer = getMetricsFactory().getTimer(MetricType.SESSION_CREATE)) {

            return sessionsByKeyspace.computeIfAbsent(keyspace, key -> {
                LOGGER.debug("Create session for [{}]", keyspace);

                Session session = getCluster().connect(key);
                session.init();

                return session;
            });
        } catch (IOException e) {
            throw new QueryException(e);
        }
    }

    private enum MetricType {
        ;

        public static final String QUERIES_COUNT = "draenei.cassandra.query.count";
        public static final String QUERIES_PROCESSING = "draenei.cassandra.query.processing";
        public static final String QUERY_EXECUTE = "draenei.cassandra.query.execute";
        public static final String QUERY_EXECUTE_ASYNC = "draenei.cassandra.query.execute-async";
        public static final String SESSION_CREATE = "draenei.cassandra.session.create";
        public static final String STMT_PREPARE = "draenei.cassandra.stmt.prepare";
        public static final String QUERIES_ERRORS = "draenei.cassandra.query.errors";
    }

    /**
     * Key for prepared STMTs cache
     */
    private static class PreparedStmtKey {
        private final String keyspace;
        private final String query;

        private PreparedStmtKey(String keyspace, String query) {
            this.keyspace = keyspace;
            this.query = query;
        }

        String getKeyspace() {
            return keyspace;
        }

        String getQuery() {
            return query;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getKeyspace(), getQuery());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof PreparedStmtKey) {
                PreparedStmtKey that = (PreparedStmtKey) obj;

                return Objects.equal(getKeyspace(), that.getKeyspace()) && Objects.equal(getQuery(), that.getQuery());
            }

            return false;
        }
    }

    private class ComputePreparedStatement implements Function<PreparedStmtKey, PreparedStatement> {
        @Override
        public PreparedStatement apply(PreparedStmtKey preparedStmtKey) {
            try (Timer timer = getMetricsFactory().getTimer(MetricType.STMT_PREPARE)) {
                return getOrCreateSession(preparedStmtKey.getKeyspace()).prepare(preparedStmtKey.getQuery());
            } catch (Exception e) {
                LOGGER.error("Cant prepare query: " + preparedStmtKey.getQuery(), e);
                throw new QueryException(e);
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
            getMetricsFactory().getCounter(MetricType.QUERIES_PROCESSING).dec();
            getMetricsFactory().getCounter(MetricType.QUERIES_COUNT).inc();
        }

        @Override
        public void onFailure(Throwable t) {
            LOGGER.warn(String.format("Cant execute bound statement [%S]: %s", keyspace, query), t);

            getMetricsFactory().getCounter(MetricType.QUERIES_COUNT).inc();
            getMetricsFactory().getCounter(MetricType.QUERIES_ERRORS).inc();
            getMetricsFactory().getCounter(MetricType.QUERIES_PROCESSING).dec();
        }

    }
}
