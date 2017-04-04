package com.fnklabs.draenei.orm;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.CassandraClientFactory;
import com.fnklabs.draenei.QueryException;
import com.fnklabs.draenei.orm.exception.MetadataException;
import com.fnklabs.metrics.Metrics;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.base.Verify;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class DataProvider<V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataProvider.class);

    private static final com.fnklabs.metrics.Metrics METRICS = MetricsFactory.getMetrics();

    protected final CassandraClientFactory cassandraClientFactory;

    /**
     * Entity class
     */
    private final Class<V> clazz;
    /**
     * Current entity metadata
     */

    private final EntityMetadata entityMetadata;

    private final Function<Row, V> mapToObjectFunction;

    /**
     * Construct provider
     *
     * @param clazz                  Entity class
     * @param cassandraClientFactory CassandraClient instance
     */
    public DataProvider(Class<V> clazz, CassandraClientFactory cassandraClientFactory) {
        this.clazz = clazz;
        this.cassandraClientFactory = cassandraClientFactory;
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
    public ListenableFuture<Boolean> saveAsync(V entity) {
        Timer saveAsyncTimer = METRICS.getTimer(MetricsType.DATA_PROVIDER_SAVE.name());


        try {
            Insert insert = QueryBuilder.insertInto(getEntityMetadata().getTableName());

            List<ColumnMetadata> columns = getEntityMetadata().getFieldMetaData();

            columns.forEach(column -> insert.value(column.getName(), QueryBuilder.bindMarker()));

            PreparedStatement prepare = getCassandraClient().prepare(getEntityMetadata().getKeyspace(), insert.getQueryString());
            prepare.setConsistencyLevel(getWriteConsistencyLevel());

            BoundStatement boundStatement = createBoundStatement(prepare, entity, columns);

            ListenableFuture<Boolean> resultFuture = Futures.transform(
                    getCassandraClient().executeAsync(boundStatement),
                    ResultSet::wasApplied
            );

            addStopTimerToFuture(saveAsyncTimer, resultFuture);

            return resultFuture;
        } catch (SyntaxError e) {
            LOGGER.warn("Can't prepare query", e);

            return Futures.immediateFailedFuture(e);
        }
    }

    /**
     * Save entity asynchronously
     *
     * @param entity Target entity
     *
     * @return Operation status result
     */
    public Boolean save(V entity) {
        return Futures.getUnchecked(saveAsync(entity));
    }

    public Boolean remove(Object... keys) {
        try (Timer removeAsyncTimer = METRICS.getTimer(MetricsType.DATA_PROVIDER_REMOVE.name())) {

            BoundStatement boundStatement = new BoundStatement(prepareRemoveStatement());

            for (int i = 0; i < getEntityMetadata().getPrimaryKeysSize(); i++) {
                Optional<PrimaryKeyMetadata> primaryKey = getEntityMetadata().getPrimaryKey(i);

                if (!primaryKey.isPresent()) {
                    throw new QueryException(String.format("Invalid primary key index: %d", i));
                }

                Object value = keys[i];

                boundStatement.set(i, value, primaryKey.get().typeCodec());
            }

            ResultSet resultSetFuture = getCassandraClient().execute(boundStatement);

            return resultSetFuture.wasApplied();
        } catch (Exception e) {

        }

        return false;
    }

    public Boolean remove(V entity) {
        return Futures.getUnchecked(removeAsync(entity));
    }

    /**
     * Remove entity asynchronously
     *
     * @param entity Target entity
     *
     * @return Operation status result
     */
    public ListenableFuture<Boolean> removeAsync(V entity) {
        Timer removeAsyncTimer = METRICS.getTimer(MetricsType.DATA_PROVIDER_REMOVE.name());

        BoundStatement boundStatement = new BoundStatement(prepareRemoveStatement());

        for (int i = 0; i < getEntityMetadata().getPrimaryKeysSize(); i++) {
            Optional<PrimaryKeyMetadata> primaryKey = getEntityMetadata().getPrimaryKey(i);

            if (!primaryKey.isPresent()) {
                throw new QueryException(String.format("Invalid primary key index: %d", i));
            }

            PrimaryKeyMetadata primaryKeyMetadata = primaryKey.get();

            Object value = primaryKeyMetadata.readValue(entity);

            boundStatement.set(i, value, primaryKeyMetadata.typeCodec());
        }

        ResultSetFuture resultSetFuture = getCassandraClient().executeAsync(boundStatement);

        ListenableFuture<Boolean> transform = Futures.transform(resultSetFuture, ResultSet::wasApplied);

        addStopTimerToFuture(removeAsyncTimer, transform);

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

        addStopTimerToFuture(time, transform);

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

        addStopTimerToFuture(timer, resultFuture);

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

    <Input> void addStopTimerToFuture(Timer timer, ListenableFuture<Input> listenableFuture) {
        Futures.addCallback(listenableFuture, new FutureTimerCallback<>(timer));
    }

    Class<V> getEntityClass() {
        return clazz;
    }

    List<Object> getPrimaryKeys(V entity) {
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

    protected List<V> fetch(List<Object> keys) {
        List<V> result = new ArrayList<>();

        fetch(keys, result::add);

        return result;
    }

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
    protected V mapToObject(Row row) {
        return mapToObjectFunction.apply(row);
    }

    private PreparedStatement prepareRemoveStatement() {
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

        Verify.verifyNotNull(where);

        PreparedStatement prepare = getCassandraClient().prepare(getEntityMetadata().getKeyspace(), where.getQueryString());
        prepare.setConsistencyLevel(getWriteConsistencyLevel());

        return prepare;
    }

    private CassandraClient getCassandraClient() {
        return cassandraClientFactory.create();
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
        });
    }

    private void fetch(List<Object> keys, Function<V, Boolean> consumer) {
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

    private EntityMetadata getEntityMetadata() {
        return entityMetadata;
    }

    private void bindPrimaryKeysParameters(List<Object> keys, BoundStatement boundStatement) {
        for (int i = 0; i < keys.size(); i++) {
            Optional<PrimaryKeyMetadata> primaryKey = getEntityMetadata().getPrimaryKey(i);

            if (!primaryKey.isPresent()) {
                throw new QueryException(String.format("Invalid primary key index: %d", i));
            }

            PrimaryKeyMetadata primaryKeyMetadata = primaryKey.get();

            boundStatement.set(i, keys.get(i), primaryKeyMetadata.getFieldType());
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

    private BoundStatement createBoundStatement(PreparedStatement prepare, V entity, List<ColumnMetadata> columns) {
        BoundStatement boundStatement = new BoundStatement(prepare);
        boundStatement.setConsistencyLevel(getEntityMetadata().getWriteConsistencyLevel());

        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata column = columns.get(i);

            Object value = column.readValue(entity);

            boundStatement.set(i, value, column.typeCodec());
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