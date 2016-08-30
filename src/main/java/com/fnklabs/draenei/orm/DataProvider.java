package com.fnklabs.draenei.orm;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.orm.exception.CanNotBuildEntryCacheKey;
import com.fnklabs.draenei.orm.exception.MetadataException;
import com.fnklabs.draenei.orm.exception.QueryException;
import com.fnklabs.metrics.Metrics;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

public class DataProvider<V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataProvider.class);
    /**
     * hashing function to build Entity hash code
     */
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

    @NotNull
    private static final com.fnklabs.metrics.Metrics METRICS = MetricsFactory.getMetrics();
    /**
     * Entity class
     */
    @NotNull
    private final Class<V> clazz;
    /**
     * Current entity metadata
     */
    @NotNull
    private final EntityMetadata entityMetadata;
    @NotNull
    private final CassandraClient cassandraClient;
    @NotNull
    private final Function<Row, V> mapToObjectFunction;

    @NotNull
    private final ExecutorService executorService;

    /**
     * Construct provider
     *
     * @param clazz           Entity class
     * @param cassandraClient CassandraClient instance
     * @param executorService ExecutorService that will be used for processing ResultSetFuture to occupy CassandraDriver ThreadPool
     */
    public DataProvider(@NotNull Class<V> clazz, @NotNull CassandraClient cassandraClient, @NotNull ExecutorService executorService) {
        this.clazz = clazz;
        this.cassandraClient = cassandraClient;
        this.executorService = executorService;
        this.entityMetadata = build(clazz);
        this.mapToObjectFunction = new MapToObjectFunction<>(clazz, entityMetadata);
    }

    /**
     * Save entity asynchronously
     *
     * @param entity Target entity
     *
     * @return Operation status result
     */
    public ListenableFuture<Boolean> saveAsync(@NotNull V entity) {
        Timer saveAsyncTimer = METRICS.getTimer(MetricsType.DATA_PROVIDER_SAVE.name());

        Insert insert = QueryBuilder.insertInto(getEntityMetadata().getTableName());

        List<ColumnMetadata> columns = getEntityMetadata().getFieldMetaData();

        columns.forEach(column -> insert.value(column.getName(), QueryBuilder.bindMarker()));

        ListenableFuture<Boolean> resultFuture;

        try {
            PreparedStatement prepare = getCassandraClient().prepare(getEntityMetadata().getKeyspace(), insert.getQueryString());
            prepare.setConsistencyLevel(getWriteConsistencyLevel());

            BoundStatement boundStatement = createBoundStatement(prepare, entity, columns);

            ResultSetFuture input = getCassandraClient().executeAsync(boundStatement);
            resultFuture = Futures.transform(input, ResultSet::wasApplied, getExecutorService());
        } catch (SyntaxError e) {
            LOGGER.warn("Can't prepare query: " + insert.getQueryString(), e);

            resultFuture = Futures.immediateFailedFuture(e);
        }

        monitorFuture(saveAsyncTimer, resultFuture);

        return resultFuture;
    }

    /**
     * Save entity asynchronously
     *
     * @param entity Target entity
     *
     * @return Operation status result
     */
    public Boolean save(@NotNull V entity) {
        Timer saveAsyncTimer = METRICS.getTimer(MetricsType.DATA_PROVIDER_SAVE.name());

        Insert insert = QueryBuilder.insertInto(getEntityMetadata().getTableName());
        List<ColumnMetadata> columns = getEntityMetadata().getFieldMetaData();
        columns.forEach(column -> insert.value(column.getName(), QueryBuilder.bindMarker()));

        try {
            String keyspace = getEntityMetadata().getKeyspace();

            PreparedStatement prepare = getCassandraClient().prepare(keyspace, insert.getQueryString());
            prepare.setConsistencyLevel(getWriteConsistencyLevel());

            BoundStatement boundStatement = createBoundStatement(prepare, entity, columns);

            ResultSet input = getCassandraClient().execute(keyspace, boundStatement);

            return input.wasApplied();
        } catch (SyntaxError e) {
            LOGGER.warn("Can't prepare query: " + insert.getQueryString(), e);

        } finally {
            saveAsyncTimer.stop();
        }

        return false;
    }

    /**
     * Remove entity asynchronously
     *
     * @param entity Target entity
     *
     * @return Operation status result
     */
    public ListenableFuture<Boolean> removeAsync(@NotNull V entity) {
        Timer removeAsyncTimer = METRICS.getTimer(MetricsType.DATA_PROVIDER_REMOVE.name());

        Delete from = QueryBuilder.delete()
                                  .from(getEntityMetadata().getTableName());

        int primaryKeysSize = getEntityMetadata().getPrimaryKeysSize();

        Delete.Where where = null;

        for (int i = 0; i < primaryKeysSize; i++) {
            Optional<PrimaryKeyMetadata> primaryKey = getEntityMetadata().getPrimaryKey(i);

            if (!primaryKey.isPresent()) {
                throw new QueryException(String.format("Invalid primary key index: %d", i));
            }

            PrimaryKeyMetadata primaryKeyMetadata = primaryKey.get();

            if (i == 0) {
                where = from.where(QueryBuilder.eq(primaryKeyMetadata.getName(), QueryBuilder.bindMarker()));
            } else {
                where = where.and(QueryBuilder.eq(primaryKeyMetadata.getName(), QueryBuilder.bindMarker()));
            }

        }

        assert where != null;

        PreparedStatement prepare = getCassandraClient().prepare(getEntityMetadata().getKeyspace(), where.getQueryString());
        prepare.setConsistencyLevel(getWriteConsistencyLevel());

        BoundStatement boundStatement = new BoundStatement(prepare);

        for (int i = 0; i < primaryKeysSize; i++) {
            Optional<PrimaryKeyMetadata> primaryKey = getEntityMetadata().getPrimaryKey(i);

            if (!primaryKey.isPresent()) {
                throw new QueryException(String.format("Invalid primary key index: %d", i));
            }

            PrimaryKeyMetadata primaryKeyMetadata = primaryKey.get();

            Object value = primaryKeyMetadata.readValue(entity);

            boundStatement.setBytesUnsafe(i, primaryKeyMetadata.serialize(value));
        }

        ResultSetFuture resultSetFuture = getCassandraClient().executeAsync(boundStatement);

        ListenableFuture<Boolean> transform = Futures.transform(resultSetFuture, ResultSet::wasApplied, getExecutorService());

        monitorFuture(removeAsyncTimer, transform);

        return transform;
    }

    /**
     * Get record async by specified keys and send result to consumer
     *
     * @param keys Primary keys
     *
     * @return True if result will be completed successfully and False if result will be completed with error
     */
    public ListenableFuture<V> findOneAsync(Object... keys) {
        Timer time = getMetrics().getTimer(MetricsType.DATA_PROVIDER_FIND_ONE.name());

        ListenableFuture<V> transform = Futures.transform(findAsync(keys), (List<V> result) -> result.isEmpty() ? null : result.get(0));

        monitorFuture(time, transform);

        return transform;
    }

    /**
     * Get record async by specified keys and send result to consumer
     *
     * @param keys Primary keys
     *
     * @return True if result will be completed successfully and False if result will be completed with error
     */
    public V findOne(Object... keys) {
        Timer time = getMetrics().getTimer(MetricsType.DATA_PROVIDER_FIND_ONE.name());

        List<V> resultList = find(keys);

        time.stop();

        return resultList.isEmpty() ? null : resultList.get(0);
    }

    /**
     * Get record async by specified keys and send result to consumer
     *
     * @param keys Primary keys
     *
     * @return True if result will be completed successfully and False if result will be completed with error
     */
    public ListenableFuture<List<V>> findAsync(Object... keys) {
        Timer timer = getMetrics().getTimer(MetricsType.DATA_PROVIDER_FIND.name());

        List<Object> parameters = new ArrayList<>();

        Collections.addAll(parameters, keys);

        ListenableFuture<List<V>> resultFuture = fetchAsync(parameters);

        monitorFuture(timer, resultFuture);

        return resultFuture;
    }

    /**
     * Get records  by specified keys and send result to consumer
     *
     * @param keys Primary keys
     *
     * @return True if result will be completed successfully and False if result will be completed with error
     */
    public List<V> find(Object... keys) {
        Timer timer = getMetrics().getTimer(MetricsType.DATA_PROVIDER_FIND.name());

        List<Object> parameters = new ArrayList<>();

        Collections.addAll(parameters, keys);

        List<V> resultFuture = fetch(parameters);

        timer.stop();

        return resultFuture;
    }

    public String getKeyspace() {
        return getEntityMetadata().getKeyspace();
    }

    public <UserCallback extends Function<V, Boolean>> int load(long start, long end, UserCallback consumer) {
        Timer timer = getMetrics().getTimer("data_provider.load");

        Select select = QueryBuilder.select()
                                    .all()
                                    .from(getEntityMetadata().getTableName());

        String[] primaryKeys = new String[getEntityMetadata().getPartitionKeySize()];

        for (int i = 0; i < getEntityMetadata().getPartitionKeySize(); i++) {
            Optional<PrimaryKeyMetadata> primaryKey = getEntityMetadata().getPrimaryKey(i);

            if (!primaryKey.isPresent()) {
                throw new QueryException(String.format("Invalid primary key index: %d", i));
            }

            PrimaryKeyMetadata primaryKeyMetadata = primaryKey.get();

            String columnName = primaryKeyMetadata.getName();

            primaryKeys[i] = columnName;
        }


        Statement statement = select.where(QueryBuilder.gt(QueryBuilder.token(primaryKeys), QueryBuilder.bindMarker()))
                                    .and(QueryBuilder.lte(QueryBuilder.token(primaryKeys), QueryBuilder.bindMarker()))
                                    .setConsistencyLevel(ConsistencyLevel.ONE)
                                    .setFetchSize(getEntityMetadata().getMaxFetchSize());


        PreparedStatement prepare = getCassandraClient().prepare(getEntityMetadata().getKeyspace(), statement.toString());

        BoundStatement boundStatement = prepare.bind(start, end);
        boundStatement.setConsistencyLevel(ConsistencyLevel.ONE);
        boundStatement.setFetchSize(getEntityMetadata().getMaxFetchSize());

        ResultSet resultSet = getCassandraClient().execute(boundStatement);

        int loadedItems = fetchResultSet(resultSet, consumer);

        timer.stop();

        LOGGER.debug("Complete load data `{}` in range ({},{}] in {}", loadedItems, start, end, timer);

        return loadedItems;
    }

    <Input> ListenableFuture<Boolean> monitorFuture(Timer timer, ListenableFuture<Input> listenableFuture) {
        return monitorFuture(timer, listenableFuture, new Function<Input, Boolean>() {
            @Override
            public Boolean apply(Input input) {
                return true;
            }
        });
    }

    protected List<V> fetch(List<Object> keys) {
        List<V> result = new ArrayList<>();

        fetch(keys, result::add);

        return result;
    }

    Class<V> getEntityClass() {
        return clazz;
    }

    /**
     * Build hash code for entity
     *
     * @param entity Input entity
     *
     * @return Cache key
     */
    long buildHashCode(@NotNull V entity) {
        Timer timer = getMetrics().getTimer(MetricsType.DATA_PROVIDER_CREATE_KEY.name());

        List<Object> keys = getPrimaryKeys(entity);

        long hashCode = buildHashCode(keys);

        timer.stop();

        return hashCode;
    }

    @NotNull
    List<Object> getPrimaryKeys(@NotNull V entity) {
        int primaryKeysSize = getEntityMetadata().getPrimaryKeysSize();

        List<Object> keys = new ArrayList<>();

        for (int i = 0; i < primaryKeysSize; i++) {
            Optional<PrimaryKeyMetadata> primaryKey = getEntityMetadata().getPrimaryKey(i);

            if (primaryKey.isPresent()) {
                PrimaryKeyMetadata primaryKeyMetadata = primaryKey.get();

                Object value = primaryKeyMetadata.readValue(entity);
                keys.add(value);
            }
        }
        return keys;
    }

    /**
     * Build cache key
     *
     * @param keys Entity keys
     *
     * @return Cache key
     */
    final long buildHashCode(Object... keys) {
        ArrayList<Object> keyList = new ArrayList<>();

        Collections.addAll(keyList, keys);

        return buildHashCode(keyList);
    }

    @NotNull
    protected Metrics getMetrics() {
        return METRICS;
    }

    /**
     * Map row result to object
     *
     * @param row ResultSet row
     *
     * @return Mapped object or null if can't map fields
     */
    @Nullable
    protected V mapToObject(@NotNull Row row) {
        return mapToObjectFunction.apply(row);
    }

    @NotNull
    private CassandraClient getCassandraClient() {
        return cassandraClient;
    }

    /**
     * Monitor future completion
     *
     * @param timer            Timer that will be close on Future success or failure
     * @param listenableFuture Listenable future
     * @param userCallback     User callback that will be executed on Future success
     * @param <Input>          Future class type
     * @param <Output>         User callback output
     *
     * @return Listenable future
     */
    private <Input, Output> ListenableFuture<Output> monitorFuture(Timer timer, ListenableFuture<Input> listenableFuture, Function<Input, Output> userCallback) {
        Futures.addCallback(listenableFuture, new FutureTimerCallback<>(timer));

        return Futures.transform(listenableFuture, new JdkFunctionWrapper<>(userCallback));
    }

    private ListenableFuture<List<V>> fetchAsync(List<Object> keys) {
        List<V> result = new ArrayList<>();

        return Futures.transform(fetchAsync(keys, result::add), (Boolean fetchResult) -> result);
    }

    private ListenableFuture<Boolean> fetchAsync(List<Object> keys, Function<V, Boolean> consumer) {
        BoundStatement boundStatement = getFetchBoundStatement(keys);

        ResultSetFuture resultSetFuture = getCassandraClient().executeAsync(boundStatement);

        return Futures.transform(resultSetFuture, (ResultSet resultSet) -> {
            fetchResultSet(resultSet, consumer);

            return true;
        }, getExecutorService());
    }

    protected void fetch(List<Object> keys, Function<V, Boolean> consumer) {
        BoundStatement boundStatement = getFetchBoundStatement(keys);

        ResultSet resultSet = getCassandraClient().execute(boundStatement);

        fetchResultSet(resultSet, consumer);
    }

    private int fetchResultSet(ResultSet resultSet, Function<V, Boolean> consumer) {
        int loadedItems = 0;

        Timer fetchResultSetTimer = getMetrics().getTimer("data_provider.load.fetch");

        for (Row row : resultSet) {
            METRICS.getCounter(MetricsType.DATA_PROVIDER_FETCH_RESULT_SET.name()).inc();

            V instance = mapToObject(row);

            loadedItems++;

            if (instance != null) {
                if (!consumer.apply(instance)) {
                    break;
                }
            }
        }

        fetchResultSetTimer.stop();

        LOGGER.debug("Complete to map `{}` items from ResultSet in {}", loadedItems, fetchResultSetTimer);

        return loadedItems;
    }

    @NotNull
    private BoundStatement getFetchBoundStatement(List<Object> keys) {
        BoundStatement boundStatement;

        Select select = QueryBuilder.select()
                                    .all()
                                    .from(getEntityMetadata().getTableName());

        int parametersLength = keys.size();

        if (parametersLength > 0) {

            if (parametersLength < getEntityMetadata().getMinPrimaryKeys() || parametersLength > getEntityMetadata().getPrimaryKeysSize()) {
                throw new QueryException(String.format("Invalid number of parameters at least composite keys must me provided. Expected: %d Actual: %d", getEntityMetadata()
                        .getPartitionKeySize(), parametersLength));
            }

            Select.Where where = null;

            for (int i = 0; i < parametersLength; i++) {
                Optional<PrimaryKeyMetadata> primaryKey = getEntityMetadata().getPrimaryKey(i);

                if (!primaryKey.isPresent()) {
                    throw new QueryException(String.format("Invalid primary key index: %d", i));
                }

                PrimaryKeyMetadata primaryKeyMetadata = primaryKey.get();

                String columnName = primaryKeyMetadata.getName();

                if (i == 0) {
                    where = select.where(QueryBuilder.eq(columnName, QueryBuilder.bindMarker()));
                } else {
                    where = where.and(QueryBuilder.eq(columnName, QueryBuilder.bindMarker()));
                }
            }

            assert where != null;

            PreparedStatement prepare = getCassandraClient().prepare(getEntityMetadata().getKeyspace(), where.getQueryString());
            prepare.setConsistencyLevel(getReadConsistencyLevel());

            boundStatement = new BoundStatement(prepare);

            bindPrimaryKeysParameters(keys, boundStatement);

        } else {
            PreparedStatement statement = getCassandraClient().prepare(getEntityMetadata().getKeyspace(), select.getQueryString());
            statement.setConsistencyLevel(getReadConsistencyLevel());

            boundStatement = new BoundStatement(statement);
        }

        boundStatement.setFetchSize(getEntityMetadata().getMaxFetchSize());
        boundStatement.setConsistencyLevel(getEntityMetadata().getReadConsistencyLevel());
        return boundStatement;
    }

    private ConsistencyLevel getReadConsistencyLevel() {
        return getEntityMetadata().getReadConsistencyLevel();
    }

    private ConsistencyLevel getWriteConsistencyLevel() {
        return getEntityMetadata().getWriteConsistencyLevel();
    }

    @NotNull
    private EntityMetadata getEntityMetadata() {
        return entityMetadata;
    }

    @NotNull
    private ExecutorService getExecutorService() {
        return executorService;
    }

    private long buildHashCode(List<Object> keys) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);

            for (Object key : keys) {
                if (key instanceof ByteBuffer) {
                    objectOutputStream.write(((ByteBuffer) key).array());
                } else {
                    objectOutputStream.writeObject(key);
                }
            }

            return HASH_FUNCTION.hashBytes(out.toByteArray()).asLong();

        } catch (IOException e) {
            LOGGER.warn("Can't build cache key", e);

            throw new CanNotBuildEntryCacheKey(getEntityClass(), e);
        }
    }

    private void bindPrimaryKeysParameters(@NotNull List<Object> keys, @NotNull BoundStatement boundStatement) {
        for (int i = 0; i < keys.size(); i++) {
            Optional<PrimaryKeyMetadata> primaryKey = getEntityMetadata().getPrimaryKey(i);

            if (!primaryKey.isPresent()) {
                throw new QueryException(String.format("Invalid primary key index: %d", i));
            }

            PrimaryKeyMetadata primaryKeyMetadata = primaryKey.get();

            boundStatement.setBytesUnsafe(i, primaryKeyMetadata.serialize(keys.get(i)));
        }
    }

    /**
     * Create BoundStatement from PreparedStatement and bind parameter values from entity
     *
     * @param prepare PreparedStatement
     * @param entity  Entity from which will be read data
     * @param columns Bind columns
     *
     * @return BoundStatement
     */
    @NotNull
    private BoundStatement createBoundStatement(@NotNull PreparedStatement prepare, @NotNull V entity, @NotNull List<ColumnMetadata> columns) {
        BoundStatement boundStatement = new BoundStatement(prepare);
        boundStatement.setConsistencyLevel(getEntityMetadata().getWriteConsistencyLevel());

        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata column = columns.get(i);

            Object value = column.readValue(entity);

            boundStatement.setBytesUnsafe(i, column.serialize(value));
        }

        return boundStatement;
    }

    /**
     * Build entity metadata from entity class
     *
     * @param clazz Entity class
     *
     * @return EntityMetadata
     *
     * @throws MetadataException
     */
    private EntityMetadata build(Class<V> clazz) throws MetadataException {
        return EntityMetadata.buildEntityMetadata(clazz, getCassandraClient());
    }

    private enum MetricsType {
        DATA_PROVIDER_FIND_ONE,
        DATA_PROVIDER_SAVE,
        DATA_PROVIDER_REMOVE,
        DATA_PROVIDER_FIND,
        DATA_PROVIDER_CREATE_KEY,
        DATA_PROVIDER_LOAD_BY_TOKEN_RANGE,
        DATA_PROVIDER_FETCH_RESULT_SET;

    }


}
