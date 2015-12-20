package com.fnklabs.draenei.orm;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.ExecutorServiceFactory;
import com.fnklabs.draenei.MetricsFactory;
import com.fnklabs.draenei.orm.exception.CanNotBuildEntryCacheKey;
import com.fnklabs.draenei.orm.exception.MetadataException;
import com.fnklabs.draenei.orm.exception.QueryException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

public class DataProvider<V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataProvider.class);
    /**
     * hashing function to build Entity hash code
     */
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

    /**
     * Map for saving DataProviders by DataProvider class
     */
    private static final Map<Class, DataProvider> DATA_PROVIDERS_REGISTRY = new ConcurrentHashMap<>();

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
    private final CassandraClientFactory cassandraClient;

    @NotNull
    private final MetricsFactory metricsFactory;

    @NotNull
    private final Function<Row, V> mapToObjectFunction;

    @NotNull
    private final ExecutorService executorService;

    /**
     * Construct provider
     *
     * @param clazz                  Entity class
     * @param cassandraClientFactory CassandraClientFactory instance
     * @param executorService        ExecutorService that will be used for processing ResultSetFuture to occupy CassandraDriver ThreadPool
     * @param metricsFactory         MetricsFactory MetricsFactory instance
     */
    public DataProvider(@NotNull Class<V> clazz,
                        @NotNull CassandraClientFactory cassandraClientFactory,
                        @NotNull ExecutorService executorService,
                        @NotNull MetricsFactory metricsFactory) {
        this.clazz = clazz;
        this.cassandraClient = cassandraClientFactory;
        this.executorService = executorService;
        this.metricsFactory = metricsFactory;
        this.entityMetadata = build(clazz);
        this.mapToObjectFunction = new MapToObjectFunction<>(clazz, entityMetadata);
    }

    public DataProvider(@NotNull Class<V> clazz,
                        @NotNull CassandraClientFactory cassandraClient,
                        @NotNull MetricsFactory metricsFactory) {
        this.clazz = clazz;
        this.cassandraClient = cassandraClient;
        this.metricsFactory = metricsFactory;
        this.executorService = ExecutorServiceFactory.DEFAULT_EXECUTOR;
        this.entityMetadata = build(clazz);
        this.mapToObjectFunction = new MapToObjectFunction<>(clazz, entityMetadata);
    }

    /**
     * Get DataProvider service
     *
     * @param clazz                  DataProvider class
     * @param cassandraClientFactory CassandraClientFactory instance
     * @param metricsFactory         MetricsFactory instance
     * @param <T>                    DataProvider class type
     *
     * @return DataProvider instance
     */
    public static <T> DataProvider<T> getDataProvider(Class<T> clazz,
                                                      @NotNull CassandraClientFactory cassandraClientFactory,
                                                      @NotNull MetricsFactory metricsFactory) {
        return DATA_PROVIDERS_REGISTRY.compute(clazz, (dataProviderClass, dataProvider) -> {
            if (dataProvider == null) {
                return new DataProvider<T>(clazz, cassandraClientFactory, metricsFactory);
            }
            return dataProvider;
        });
    }

    /**
     * Build hash code for entity
     *
     * @param entity Input entity
     *
     * @return Cache key
     */
    public final long buildHashCode(@NotNull V entity) {
        Timer.Context timer = getMetricsFactory().getTimer(MetricsType.DATA_PROVIDER_CREATE_KEY).time();

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

        long hashCode = buildHashCode(keys);

        timer.stop();

        return hashCode;
    }

    /**
     * Save entity asynchronously
     *
     * @param entity Target entity
     *
     * @return Operation status result
     */
    public ListenableFuture<Boolean> saveAsync(@NotNull V entity) {
        Timer.Context saveAsyncTimer = metricsFactory.getTimer(MetricsType.DATA_PROVIDER_SAVE).time();

        Insert insert = QueryBuilder.insertInto(getEntityMetadata().getTableName());

        List<ColumnMetadata> columns = getEntityMetadata().getFieldMetaData();

        columns.forEach(column -> insert.value(column.getName(), QueryBuilder.bindMarker()));

        ListenableFuture<Boolean> resultFuture;

        try {
            PreparedStatement prepare = getCassandraClient().prepare(insert.getQueryString());
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
     * Remove entity asynchronously
     *
     * @param entity Target entity
     *
     * @return Operation status result
     */
    public ListenableFuture<Boolean> removeAsync(@NotNull V entity) {
        Timer.Context removeAsyncTimer = metricsFactory.getTimer(MetricsType.DATA_PROVIDER_REMOVE).time();

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

        PreparedStatement prepare = getCassandraClient().prepare(where.getQueryString());
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
        Timer.Context time = getMetricsFactory().getTimer(MetricsType.DATA_PROVIDER_FIND_ONE).time();

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
    public ListenableFuture<List<V>> findAsync(Object... keys) {
        Timer.Context timer = getMetricsFactory().getTimer(MetricsType.DATA_PROVIDER_FIND).time();

        List<Object> parameters = new ArrayList<>();

        Collections.addAll(parameters, keys);

        ListenableFuture<List<V>> resultFuture = fetch(parameters);

        monitorFuture(timer, resultFuture);

        return resultFuture;
    }

    public <UserCallback extends Consumer<V>> int load(long startToken, long endToken, UserCallback consumer) {
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

        Select.Where where = select.where(QueryBuilder.gte(QueryBuilder.token(primaryKeys), QueryBuilder.bindMarker()));

        if (endToken != Long.MAX_VALUE) {
            where = where.and(QueryBuilder.lt(QueryBuilder.token(primaryKeys), QueryBuilder.bindMarker()));
        } else {
            where = where.and(QueryBuilder.lte(QueryBuilder.token(primaryKeys), QueryBuilder.bindMarker()));
        }

        PreparedStatement prepare = getCassandraClient().prepare(where.getQueryString());
        prepare.setConsistencyLevel(getReadConsistencyLevel());

        BoundStatement boundStatement = new BoundStatement(prepare);
        boundStatement.bind(startToken, endToken);

        boundStatement.setFetchSize(getEntityMetadata().getMaxFetchSize());
        boundStatement.setConsistencyLevel(getEntityMetadata().getReadConsistencyLevel());

        ResultSet resultSet = getCassandraClient().execute(boundStatement);

        Iterator<Row> iterator = resultSet.iterator();

        int loadedItems = 0;

        while (iterator.hasNext()) {
            Row next = iterator.next();

            V instance = mapToObject(next);

            if (instance != null) {
                consumer.accept(instance);
            }
            loadedItems++;
        }

        return loadedItems;
    }

    public Class<V> getEntityClass() {
        return clazz;
    }

    /**
     * Build cache key
     *
     * @param keys Entity keys
     *
     * @return Cache key
     */
    protected final long buildHashCode(Object... keys) {
        ArrayList<Object> keyList = new ArrayList<>();

        Collections.addAll(keyList, keys);

        return buildHashCode(keyList);
    }

    @NotNull
    protected MetricsFactory getMetricsFactory() {
        return metricsFactory;
    }

    protected String getTableName() {
        return getEntityMetadata().getTableName();
    }

    protected int getMaxFetchSize() {
        return getEntityMetadata().getMaxFetchSize();
    }

    protected ListenableFuture<List<V>> fetch(List<Object> keys) {
        List<V> result = new ArrayList<>();

        return Futures.transform(fetch(keys, result::add), (Boolean fetchResult) -> result);
    }

    protected ListenableFuture<Boolean> fetch(List<Object> keys, Consumer<V> consumer) {
        BoundStatement boundStatement;

        Select select = QueryBuilder.select()
                                    .all()
                                    .from(getEntityMetadata().getTableName());

        int parametersLength = keys.size();

        if (parametersLength > 0) {

            if (parametersLength < getEntityMetadata().getMinPrimaryKeys() || parametersLength > getEntityMetadata().getPrimaryKeysSize()) {
                throw new QueryException(String.format("Invalid number of parameters at least composite keys must me provided. Expected: %d Actual: %d", getEntityMetadata().getPartitionKeySize(), parametersLength));
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

            PreparedStatement prepare = getCassandraClient().prepare(where.getQueryString());
            prepare.setConsistencyLevel(getReadConsistencyLevel());

            boundStatement = new BoundStatement(prepare);

            bindPrimaryKeysParameters(keys, boundStatement);

        } else {
            PreparedStatement statement = getCassandraClient().prepare(select.getQueryString());
            statement.setConsistencyLevel(getReadConsistencyLevel());

            boundStatement = new BoundStatement(statement);
        }

        boundStatement.setFetchSize(getEntityMetadata().getMaxFetchSize());
        boundStatement.setConsistencyLevel(getEntityMetadata().getReadConsistencyLevel());

        ResultSetFuture resultSetFuture = getCassandraClient().executeAsync(boundStatement);

        return Futures.transform(resultSetFuture, (ResultSet resultSet) -> {
            Iterator<Row> iterator = resultSet.iterator();

            while (iterator.hasNext()) {
                Row next = iterator.next();

                V instance = mapToObject(next);

                consumer.accept(instance);
            }

            return true;
        }, getExecutorService());
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
    protected CassandraClient getCassandraClient() {
        return cassandraClient.create();
    }

    @NotNull
    protected final EntityMetadata getEntityMetadata() {
        return entityMetadata;
    }

    protected ConsistencyLevel getReadConsistencyLevel() {
        return getEntityMetadata().getReadConsistencyLevel();
    }

    protected ConsistencyLevel getWriteConsistencyLevel() {
        return getEntityMetadata().getWriteConsistencyLevel();
    }

    protected <Input> ListenableFuture<Boolean> monitorFuture(Timer.Context timer, ListenableFuture<Input> listenableFuture) {
        return monitorFuture(timer, listenableFuture, new Function<Input, Boolean>() {
            @Override
            public Boolean apply(Input input) {
                return true;
            }
        });
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
    protected <Input, Output> ListenableFuture<Output> monitorFuture(Timer.Context timer, ListenableFuture<Input> listenableFuture, Function<Input, Output> userCallback) {
        Futures.addCallback(listenableFuture, new FutureTimerCallback<Input>(timer));

        return Futures.transform(listenableFuture, new JdkFunctionWrapper<Input, Output>(userCallback));
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


    private enum MetricsType implements MetricsFactory.Type {
        DATA_PROVIDER_FIND_ONE,
        DATA_PROVIDER_SAVE,
        DATA_PROVIDER_REMOVE,
        DATA_PROVIDER_FIND,
        DATA_PROVIDER_CREATE_KEY,
    }


}
