package com.fnklabs.draenei.orm;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.MetricsFactory;
import com.fnklabs.draenei.orm.annotations.*;
import com.fnklabs.draenei.orm.exception.MetadataException;
import com.fnklabs.draenei.orm.exception.QueryException;
import com.google.common.util.concurrent.*;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

public class DataProvider<V> {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DataProvider.class);

    private final Class<V> clazz;
    private final EntityMetadata entityMetadata;
    private final CassandraClient cassandraClient;
    private final MetricsFactory metricsFactory;
    private final ListeningExecutorService executorService;

    public DataProvider(Class<V> clazz, CassandraClient cassandraClient, MetricsFactory metricsFactory, ListeningExecutorService executorService) {
        this.clazz = clazz;
        this.cassandraClient = cassandraClient;
        this.metricsFactory = metricsFactory;
        this.entityMetadata = build(clazz);
        this.executorService = executorService;
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

        List<FieldMetadata> columns = getEntityMetadata().getColumns();

        columns.forEach(column -> insert.value(column.getName(), QueryBuilder.bindMarker()));

        String queryString = insert.getQueryString();


        ListenableFuture<Boolean> resultFuture = null;

        try {
            PreparedStatement prepare = getCassandraClient().prepare(queryString);
            BoundStatement boundStatement = new BoundStatement(prepare);


            for (int i = 0; i < columns.size(); i++) {
                FieldMetadata column = columns.get(i);

                Object value = null;
                try {
                    value = column.getReadMethod().invoke(entity);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    LOGGER.warn("cant invoke read method", e);
                }
                boundStatement.setBytesUnsafe(i, getEntityMetadata().serialize(column, value));
            }

            boundStatement.setConsistencyLevel(getEntityMetadata().getConsistencyLevel());
            boundStatement.setRetryPolicy(FallthroughRetryPolicy.INSTANCE);

            resultFuture = Futures.transform(getCassandraClient().executeAsync(boundStatement), ResultSet::wasApplied, executorService);
        } catch (SyntaxError e) {
            LOGGER.warn("Cant prepare query: " + queryString, e);

            SettableFuture<Boolean> booleanSettableFuture = SettableFuture.<Boolean>create();
            booleanSettableFuture.setException(e);

            resultFuture = booleanSettableFuture;
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

        Delete from = QueryBuilder
                .delete()
                .from(getEntityMetadata().getTableName());

        int primaryKeysSize = getEntityMetadata().getPrimaryKeysSize();

        Delete.Where where = null;

        for (int i = 0; i < primaryKeysSize; i++) {
            Optional<PrimaryKeyMetadata> primaryKey = getEntityMetadata().getPrimaryKey(i);
            PrimaryKeyMetadata primaryKeyMetadata = primaryKey.get();

            if (i == 0) {
                where = from.where(QueryBuilder.eq(primaryKeyMetadata.getName(), QueryBuilder.bindMarker()));
            } else {
                where = where.and(QueryBuilder.eq(primaryKeyMetadata.getName(), QueryBuilder.bindMarker()));
            }

        }

        assert where != null;

        PreparedStatement prepare = getCassandraClient().prepare(where.getQueryString());

        BoundStatement boundStatement = new BoundStatement(prepare);

        for (int i = 0; i < primaryKeysSize; i++) {
            Optional<PrimaryKeyMetadata> primaryKey = getEntityMetadata().getPrimaryKey(i);
            PrimaryKeyMetadata primaryKeyMetadata = primaryKey.get();

            Method readMethod = primaryKeyMetadata.getReadMethod();

            Object value = null;
            try {
                value = readMethod.invoke(entity);
            } catch (IllegalAccessException | InvocationTargetException | NullPointerException e) {
                LOGGER.warn("cant invoke read method", e);
            }

            boundStatement.setBytesUnsafe(i, getEntityMetadata().serialize(primaryKeyMetadata, value));
        }


        ResultSetFuture resultSetFuture = getCassandraClient().executeAsync(boundStatement);

        ListenableFuture<Boolean> transform = Futures.transform(resultSetFuture, (ResultSet resultSet) -> resultSet.wasApplied(), executorService);

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

        ListenableFuture<V> transform = Futures.transform(findAsync(keys), (List<V> result) -> result.isEmpty() ? null : result.get(0), executorService);

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

        List<V> result = Collections.synchronizedList(new ArrayList<>());

        List<Object> parameters = new ArrayList<>();

        Collections.addAll(parameters, keys);

        ListenableFuture<Boolean> resultFuture = seek(result::add, parameters);

        ListenableFuture<List<V>> transform = Futures.transform(resultFuture, (Boolean status) -> result, executorService);

        monitorFuture(timer, transform);

        return transform;
    }

    @PreDestroy
    public void shutDown() {
        getExecutorService().shutdown();
        try {
            getExecutorService().awaitTermination(600, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected MetricsFactory getMetricsFactory() {
        return metricsFactory;
    }

    protected String getTableName() {
        return getEntityMetadata().getTableName();
    }

    protected Class<V> getEntityClass() {
        return clazz;
    }

    protected int getMaxFetchSize() {
        return getEntityMetadata().getMaxFetchSize();
    }

    protected ListenableFuture<Boolean> seek(Consumer<V> consumer, List<Object> keys) {
        BoundStatement boundStatement;

        Select select = QueryBuilder
                .select()
                .all()
                .from(getEntityMetadata().getTableName());

        int parametersLength = keys.size();

        if (parametersLength > 0) {

            if (parametersLength < getEntityMetadata().getMinPrimaryKeys() || parametersLength > getEntityMetadata().getPrimaryKeysSize()) {
                throw new QueryException(String.format("Invalid number of parameters at least composite keys must me provided. Expected: %d Actual: %d", getEntityMetadata().getCompositeKeysSize(), parametersLength));
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
            boundStatement = new BoundStatement(prepare);

            bindPrimaryKeysParameters(keys, boundStatement);

        } else {
            PreparedStatement statement = getCassandraClient().prepare(select.getQueryString());

            boundStatement = new BoundStatement(statement);
        }

        boundStatement.setFetchSize(getEntityMetadata().getMaxFetchSize());
        boundStatement.setConsistencyLevel(getEntityMetadata().getConsistencyLevel());

        return Futures.transform(getCassandraClient().executeAsync(boundStatement), (ResultSet resultSet) -> {
            List<ListenableFuture<Boolean>> futureList = new ArrayList<>();

            lazyFetch(resultSet, row -> {
                V instance = mapToObject(row);


                ListenableFuture<Boolean> listenableFuture = executorService.submit(() -> {
                    consumer.accept(instance);
                    return true;
                });

                futureList.add(listenableFuture);
            });

            ListenableFuture<List<Boolean>> listenableFutures = Futures.successfulAsList(futureList);

            return Futures.transform(listenableFutures, (List<Boolean> resultStatus) -> true, executorService);
        }, executorService);
    }

    protected void bindPrimaryKeysParameters(List<Object> keys, BoundStatement boundStatement) {
        for (int i = 0; i < keys.size(); i++) {
            Optional<PrimaryKeyMetadata> primaryKey = getEntityMetadata().getPrimaryKey(i);

            if (!primaryKey.isPresent()) {
                throw new QueryException(String.format("Invalid primary key index: %d", i));
            }

            PrimaryKeyMetadata primaryKeyMetadata = primaryKey.get();

            boundStatement.setBytesUnsafe(i, getEntityMetadata().serialize(primaryKeyMetadata, keys.get(i)));
        }
    }

    protected V mapToObject(Row row) {
        V instance = null;

        try {
            instance = clazz.newInstance();

            List<FieldMetadata> columns = getEntityMetadata().getColumns();

            for (FieldMetadata column : columns) {
                if (row.getColumnDefinitions().contains(column.getName())) {

                    Object deserializedValue = entityMetadata.deserialize(column, row.getBytesUnsafe(column.getName()));

                    if (deserializedValue == null) {
                        continue;
                    }
                    Method writeMethod = column.getWriteMethod();


                    if (writeMethod == null || instance == null) {
                        LOGGER.warn("Write method is null");
                    } else {
                        try {
                            writeMethod.invoke(instance, deserializedValue);
                        } catch (InvocationTargetException | IllegalAccessException e) {
                            LOGGER.warn("Cant invoker write method", e);
                        }
                    }
                }
            }

        } catch (InstantiationException | IllegalAccessException e) {
            LOGGER.warn("Cant retrieve entity instance", e);
        }
        return instance;
    }

    protected CassandraClient getCassandraClient() {
        return cassandraClient;
    }

    /**
     * Lazy fetch all result and send one by one results asynchronously  to consumer
     *
     * @param resultSet ResultSet
     * @param consumer  Row(result) consumer
     */
    protected void lazyFetch(ResultSet resultSet, Consumer<Row> consumer) {
        AtomicLong fetchedRows = new AtomicLong(0);

        Iterator<Row> iterator = resultSet.iterator();


        for (; ; ) {
            Row next;
            try {
                next = iterator.next();
            } catch (NoSuchElementException e) {
                break;
            }

            if (next == null) {
//                LOGGER.warn("Row is null, skipping...");
            } else {
                fetchedRows.getAndIncrement();

                consumer.accept(next);
            }

            if (!iterator.hasNext()) {
                break;
            }


            boolean isExhausted = resultSet.isExhausted();
            boolean isFullyFetched = resultSet.isFullyFetched();
            int availableWithoutFetching = resultSet.getAvailableWithoutFetching();
            long fetchedRowsCount = fetchedRows.get();

//            LOGGER.debug("Is exhausted: {} Is fully fetched {} Available: {} fetched: {}", isExhausted, isFullyFetched, availableWithoutFetching, fetchedRowsCount);
        }
    }

    final protected EntityMetadata getEntityMetadata() {
        return entityMetadata;
    }

    protected ListeningExecutorService getExecutorService() {
        return executorService;
    }

    protected ConsistencyLevel getConsistencyLevel() {
        return getEntityMetadata().getConsistencyLevel();
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
        Futures.addCallback(listenableFuture, new TimerFutureCallback<Input>(timer), getExecutorService());

        return Futures.transform(listenableFuture, new JdkFunctionWrapper<Input, Output>(userCallback), getExecutorService());
    }

    private EntityMetadata build(Class<V> clazz) throws MetadataException {
        EntityMetadata metadata = buildEntityMetadata(clazz);

        metadata.validate();

        return metadata;
    }

    private EntityMetadata buildEntityMetadata(Class<V> clazz) throws MetadataException {

        Table annotation = clazz.getAnnotation(Table.class);

        if (annotation == null) {
            throw new MetadataException(String.format("Table annotation is missing for %s", clazz.getName()));
        }

        EntityMetadata entityMetadata = new EntityMetadata(annotation.name(), annotation.compactStorage(), annotation.fetchSize(), annotation.consistencyLevel(), getCassandraClient()
                .getTableMetadata(annotation.name()));

        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(clazz);

            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

            for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {

                try {
                    Field field = clazz.getDeclaredField(propertyDescriptor.getName());

                    List<FieldMetadata> fieldMetadataList = buildColumnMetadataList(propertyDescriptor, field);

                    fieldMetadataList.forEach(entityMetadata::addColumnMetadata);
                } catch (NoSuchFieldException e) {
                }

//                LOGGER.debug("Property descriptor: {} {}", propertyDescriptor.getName(), propertyDescriptor.getDisplayName());
            }
        } catch (IntrospectionException e) {
            LOGGER.warn(e.getMessage(), e);
        }

        return entityMetadata;
    }

    private enum MetricsType implements MetricsFactory.Type {
        DATA_PROVIDER_FIND_ONE,
        DATA_PROVIDER_SAVE,
        DATA_PROVIDER_REMOVE, DATA_PROVIDER_FIND,
    }

    private static List<FieldMetadata> buildColumnMetadataList(PropertyDescriptor propertyDescriptor, Field field) {
        List<FieldMetadata> fieldMetadataList = new ArrayList<>();

        Column columnAnnotation = field.getDeclaredAnnotation(Column.class);

        if (columnAnnotation != null) {

            String columnName = columnAnnotation.name();

            if (StringUtils.isEmpty(columnName)) {
                columnName = propertyDescriptor.getName();
            }

            fieldMetadataList.add(new FieldMetadata<>(propertyDescriptor, field.getType(), columnName));

            Enumerated enumeratedAnnotation = field.getDeclaredAnnotation(Enumerated.class);

            if (enumeratedAnnotation != null) {
                fieldMetadataList.add(new EnumeratedMetadata<>(propertyDescriptor, field.getType(), columnName));
            }


            ClusteringKey clusteringKeyAnnotation = field.getDeclaredAnnotation(ClusteringKey.class);

            if (clusteringKeyAnnotation != null) {
                fieldMetadataList.add(new ClusteringKeyMetadata<>(propertyDescriptor, columnName, clusteringKeyAnnotation.order(), field.getType()));
            }

            CompositeKey compositeKeyAnnotation = field.getDeclaredAnnotation(CompositeKey.class);

            if (compositeKeyAnnotation != null) {
                fieldMetadataList.add(new CompositeKeyMetadata<>(propertyDescriptor, columnName, compositeKeyAnnotation.order(), field.getType()));
            }
        }
        return fieldMetadataList;
    }

    private static class JdkFunctionWrapper<Input, Output> implements com.google.common.base.Function<Input, Output> {
        private final Function<Input, Output> jdkFunction;

        private JdkFunctionWrapper(Function<Input, Output> jdkFunction) {
            this.jdkFunction = jdkFunction;
        }

        @Override
        public Output apply(Input input) {
            return jdkFunction.apply(input);
        }
    }

    /**
     * Function for stoping timer on future completion
     *
     * @param <Input> Future type
     */
    private static class TimerFutureCallback<Input> implements FutureCallback<Input> {
        private final Timer.Context timer;

        private TimerFutureCallback(Timer.Context timer) {
            this.timer = timer;
        }

        @Override
        public void onSuccess(Input result) {
            timer.stop();
        }

        @Override
        public void onFailure(Throwable t) {
            timer.stop();
            LOGGER.warn("Cant complete operation", t);
        }
    }
}
