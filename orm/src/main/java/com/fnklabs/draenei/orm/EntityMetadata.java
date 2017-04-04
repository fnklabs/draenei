package com.fnklabs.draenei.orm;

import com.datastax.driver.core.*;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.orm.annotations.Collection;
import com.fnklabs.draenei.orm.annotations.*;
import com.fnklabs.draenei.orm.exception.MetadataException;
import com.google.common.base.Verify;
import com.google.common.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.*;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Contains entity metadata information builder from entity class
 */
class EntityMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(EntityMetadata.class);


    private final String tableName;


    private final String keyspace;

    private final boolean compactStorage;

    private final int maxFetchSize;


    private final ConsistencyLevel readConsistencyLevel;


    private final ConsistencyLevel writeConsistencyLevel;


    private final Map<String, ColumnMetadata> columnsMetadata = new HashMap<>();

    private final Map<Integer, PrimaryKeyMetadata> primaryKeys = new HashMap<>();

    /**
     * DataStax table metadata need to serialize and deserialize data
     */
    private final TableMetadata tableMetadata;

    private EntityMetadata(String tableName,
                           String keyspace,
                           boolean compactStorage,
                           int maxFetchSize,
                           ConsistencyLevel readConsistencyLevel,
                           ConsistencyLevel writeConsistencyLevel,
                           TableMetadata tableMetadata) {
        this.tableName = tableName;
        this.keyspace = keyspace;
        this.compactStorage = compactStorage;
        this.maxFetchSize = maxFetchSize;
        this.readConsistencyLevel = readConsistencyLevel;
        this.writeConsistencyLevel = writeConsistencyLevel;
        this.tableMetadata = tableMetadata;
    }

    /**
     * Build cassandra entity metadata
     *
     * @param clazz           Entity class
     * @param cassandraClient Cassandra Client from which will be retrieved table information
     * @param <V>             Entity class type
     *
     * @return Entity metadata
     *
     * @throws MetadataException
     */
    static <V> EntityMetadata buildEntityMetadata(Class<V> clazz, CassandraClient cassandraClient) throws MetadataException {
        Table tableAnnotation = clazz.getAnnotation(Table.class);

        if (tableAnnotation == null) {
            throw new MetadataException(String.format("Table annotation is missing for %s", clazz.getName()));
        }

        String tableKeyspace = StringUtils.isEmpty(tableAnnotation.keyspace()) ? cassandraClient.getDefaultKeyspace() : tableAnnotation.keyspace();

        String tableName = tableAnnotation.name();

        TableMetadata tableMetadata = cassandraClient.getTableMetadata(tableKeyspace, tableName);

        EntityMetadata entityMetadata = new EntityMetadata(
                tableName,
                tableKeyspace,
                tableAnnotation.compactStorage(),
                tableAnnotation.fetchSize(),
                tableAnnotation.readConsistencyLevel(),
                tableAnnotation.writeConsistencyLevel(),
                tableMetadata
        );

        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(clazz);

            for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {

                ColumnMetadata columnMetadata = buildColumnMetadata(propertyDescriptor, clazz, cassandraClient, tableMetadata);

                if (columnMetadata != null) {
                    entityMetadata.addColumnMetadata(columnMetadata);
                }

                LOGGER.debug("Property descriptor: {} {}", propertyDescriptor.getName(), propertyDescriptor.getDisplayName());
            }
        } catch (IntrospectionException e) {
            LOGGER.warn("Can't build column metadata", e);
        }

        EntityMetadata.validate(entityMetadata);

        return entityMetadata;
    }

    /**
     * Build column metadata from field
     *
     * @param propertyDescriptor Field property descriptor
     * @param clazz              Entity java class
     *
     * @return Column metadata or null if property is not column
     */
    @Nullable
    static ColumnMetadata buildColumnMetadata(PropertyDescriptor propertyDescriptor,
                                              Class clazz,
                                              CassandraClient cassandraClient,
                                              TableMetadata tableMetadata) {

        try {
            Field field = clazz.getDeclaredField(propertyDescriptor.getName());

            if (field.isAnnotationPresent(Column.class)) {

                Column columnAnnotation = field.getDeclaredAnnotation(Column.class);

                String columnName = getColumnName(propertyDescriptor, columnAnnotation);

                Verify.verifyNotNull(tableMetadata.getColumn(columnName), String.format("Column metadata `%s` not found", columnName));

                DataType dataType = tableMetadata.getColumn(columnName).getType();

                ColumnMetadata columnMetadata = new BaseColumnMetadata(
                        propertyDescriptor,
                        clazz,
                        field.getType(),
                        columnName,
                        dataType,
                        getTypeCode(dataType, field)
                );

                if (field.isAnnotationPresent(PrimaryKey.class)) {
                    PrimaryKey primaryKeyAnnotation = field.getDeclaredAnnotation(PrimaryKey.class);
                    columnMetadata = new PrimaryKeyMetadata(columnMetadata, primaryKeyAnnotation.order(), primaryKeyAnnotation.isPartitionKey());
                }


                return columnMetadata;
            }
        } catch (NoSuchFieldException e) {
            if (!StringUtils.equals("class", propertyDescriptor.getDisplayName())) {
//                LOGGER.warn("Can't get field", e);
            }
        }

        return null;
    }

    /**
     * Get TypeCoded for provided Entity field
     *
     * @param dataType DataType
     * @param field    Entity Field type
     *
     * @return
     */
    private static TypeCodec getTypeCode(DataType dataType, Field field) {
        if (field.isAnnotationPresent(Enumerated.class)) {
            Enumerated enumerated = field.getDeclaredAnnotation(Enumerated.class);

            Class<Enum<?>> enumType = enumerated.enumType();

            CodecRegistry.DEFAULT_INSTANCE.register(new EnumCodec(enumType));
        }

        if (field.isAnnotationPresent(Collection.class)) {
            Collection collection = field.getDeclaredAnnotation(Collection.class);

            TypeToken typeToken = collectionTypeToken(field.getType(), collection.elementType());

            return CodecRegistry.DEFAULT_INSTANCE.codecFor(dataType, typeToken);
        }

        if (field.isAnnotationPresent(com.fnklabs.draenei.orm.annotations.Map.class)) {

            com.fnklabs.draenei.orm.annotations.Map map = field.getDeclaredAnnotation(com.fnklabs.draenei.orm.annotations.Map.class);

            TypeToken typeToken = TypeTokens.mapOf(map.elementKeyType(), map.elementValueType());

            return CodecRegistry.DEFAULT_INSTANCE.codecFor(dataType, typeToken);
        }

        return CodecRegistry.DEFAULT_INSTANCE.codecFor(dataType, field.getType());
    }

    private static TypeToken collectionTypeToken(Class<?> fieldType, Class<?> elementType) {
        if (fieldType.isAssignableFrom(Set.class)) {
            return TypeTokens.setOf(elementType);
        } else if (fieldType.isAssignableFrom(List.class)) {
            return TypeTokens.listOf(elementType);
        } else {
            throw new RuntimeException("Invalid collection type");
        }
    }

    /**
     * Validate entity metadata
     *
     * @param entityMetadata Entity Metadata instance
     *
     * @throws MetadataException if invalid metadada was provided
     */
    private static void validate(EntityMetadata entityMetadata) {
        if (StringUtils.isEmpty(entityMetadata.getTableName())) {
            throw new MetadataException(String.format("Invalid table name: \"%s\"", entityMetadata.getTableName()));
        }

        if (entityMetadata.getPartitionKeySize() < 1) {
            throw new MetadataException(String.format("Entity \"%s\"must contains primary key", entityMetadata.getTableName()));
        }
    }

    private static String getColumnName(PropertyDescriptor propertyDescriptor, Column columnAnnotation) {
        String columnName = columnAnnotation.name();

        if (StringUtils.isEmpty(columnName)) {
            columnName = propertyDescriptor.getName();
        }

        return columnName;
    }

    String getKeyspace() {
        return keyspace;
    }

    int getPartitionKeySize() {
        return columnsMetadata.entrySet()
                              .stream()
                              .mapToInt(entry -> entry.getValue() instanceof PrimaryKeyMetadata && ((PrimaryKeyMetadata) entry.getValue()).isPartitionKey() ? 1 : 0)
                              .sum();
    }

    Optional<PrimaryKeyMetadata> getPrimaryKey(int oder) {
        return Optional.ofNullable(primaryKeys.get(oder));
    }

    /**
     * Get field metadata
     *
     * @return
     */
    List<ColumnMetadata> getFieldMetaData() {
        return columnsMetadata.entrySet()
                              .stream()
                              .map(Map.Entry::getValue)
                              .collect(Collectors.toList());
    }

    String getTableName() {
        return tableName;
    }

    int getMaxFetchSize() {
        return maxFetchSize;
    }

    int getMinPrimaryKeys() {
        if (getPartitionKeySize() > 0) {
            return getPartitionKeySize();
        }

        return 1;
    }

    int getPrimaryKeysSize() {
        return tableMetadata.getPrimaryKey().size();
    }

    ConsistencyLevel getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }

    ConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    @Nullable
    public ColumnMetadata getColumn(String name) {
        return columnsMetadata.get(name);
    }

    protected boolean isCompactStorage() {
        return compactStorage;
    }

    protected int getClusteringKeysSize() {
        return tableMetadata.getClusteringColumns().size();
    }

    private void addColumnMetadata(ColumnMetadata columnMetadata) {
        columnsMetadata.put(columnMetadata.getName(), columnMetadata);

        if (columnMetadata instanceof PrimaryKeyMetadata) {
            PrimaryKeyMetadata primaryKeyMetadata = (PrimaryKeyMetadata) columnMetadata;
            primaryKeys.put(primaryKeyMetadata.getOrder(), primaryKeyMetadata);
        }
    }
}