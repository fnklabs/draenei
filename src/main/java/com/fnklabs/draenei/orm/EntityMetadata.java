package com.fnklabs.draenei.orm;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.orm.annotations.*;
import com.fnklabs.draenei.orm.exception.MetadataException;
import com.google.common.base.Verify;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Contains entity metadata information builder from entity class
 */
class EntityMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(EntityMetadata.class);

    @NotNull
    private final String tableName;

    @NotNull
    private final String keyspace;

    private final boolean compactStorage;

    private final int maxFetchSize;

    @NotNull
    private final ConsistencyLevel readConsistencyLevel;

    @NotNull
    private final ConsistencyLevel writeConsistencyLevel;

    @NotNull
    private final HashMap<String, ColumnMetadata> columnsMetadata = new HashMap<>();
    /**
     * DataStax table metadata need to serialize and deserialize data
     */
    @NotNull
    private final TableMetadata tableMetadata;

    @NotNull
    private final HashMap<Integer, PrimaryKeyMetadata> primaryKeys = new HashMap<>();

    private EntityMetadata(@NotNull String tableName,
                           @NotNull String keyspace, boolean compactStorage,
                           int maxFetchSize,
                           @NotNull ConsistencyLevel readConsistencyLevel,
                           @NotNull ConsistencyLevel writeConsistencyLevel,
                           @NotNull TableMetadata tableMetadata) {
        this.tableName = tableName;
        this.keyspace = keyspace;
        this.compactStorage = compactStorage;
        this.maxFetchSize = maxFetchSize;
        this.readConsistencyLevel = readConsistencyLevel;
        this.writeConsistencyLevel = writeConsistencyLevel;
        this.tableMetadata = tableMetadata;
    }

    protected boolean isCompactStorage() {
        return compactStorage;
    }

    protected int getClusteringKeysSize() {
        return tableMetadata.getClusteringColumns().size();
    }

    private void addColumnMetadata(@NotNull ColumnMetadata columnMetadata) {
        columnsMetadata.put(columnMetadata.getName(), columnMetadata);

        if (columnMetadata instanceof PrimaryKeyMetadata) {
            PrimaryKeyMetadata primaryKeyMetadata = (PrimaryKeyMetadata) columnMetadata;
            primaryKeys.put(primaryKeyMetadata.getOrder(), primaryKeyMetadata);
        }
    }

    /**
     * Validate entity metadata
     *
     * @param entityMetadata Entity Metadata instance
     *
     * @throws MetadataException if invalid metadada was provided
     */
    private static void validate(@NotNull EntityMetadata entityMetadata) {
        if (StringUtils.isEmpty(entityMetadata.getTableName())) {
            throw new MetadataException(String.format("Invalid table name: \"%s\"", entityMetadata.getTableName()));
        }

        if (entityMetadata.getPartitionKeySize() < 1) {
            throw new MetadataException(String.format("Entity \"%s\"must contains primary key", entityMetadata.getTableName()));
        }
    }

    private static String getColumnName(@NotNull PropertyDescriptor propertyDescriptor, @NotNull Column columnAnnotation) {
        String columnName = columnAnnotation.name();

        if (StringUtils.isEmpty(columnName)) {
            columnName = propertyDescriptor.getName();
        }

        return columnName;
    }

    @NotNull
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
                              .map(entry -> entry.getValue())
                              .collect(Collectors.toList());
    }

    @NotNull
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

    @NotNull
    ConsistencyLevel getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }

    @NotNull
    ConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
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
    static <V> EntityMetadata buildEntityMetadata(@NotNull Class<V> clazz, @NotNull CassandraClient cassandraClient) throws MetadataException {
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

                ColumnMetadata columnMetadata = buildColumnMetadata(propertyDescriptor, clazz, tableMetadata);

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
    static ColumnMetadata buildColumnMetadata(@NotNull PropertyDescriptor propertyDescriptor, @NotNull Class clazz, @NotNull TableMetadata tableMetadata) {

        try {
            Field field = clazz.getDeclaredField(propertyDescriptor.getName());

            if (field.isAnnotationPresent(Column.class)) {

                Column columnAnnotation = field.getDeclaredAnnotation(Column.class);

                String columnName = getColumnName(propertyDescriptor, columnAnnotation);

                ColumnMetadata columnMetadata = new BaseColumnMetadata(propertyDescriptor, clazz, field.getType(), columnName, tableMetadata.getColumn(columnName));

                if (field.isAnnotationPresent(Enumerated.class)) {
                    Enumerated enumeratedAnnotation = field.getDeclaredAnnotation(Enumerated.class);

                    columnMetadata = new EnumeratedMetadata(columnMetadata, enumeratedAnnotation.enumType());
                }
                if (field.isAnnotationPresent(UDTColumn.class)) {
                    UDTColumn udtColumnAnnotation = field.getDeclaredAnnotation(UDTColumn.class);

                    UserType userType = tableMetadata.getKeyspace().getUserType(udtColumnAnnotation.name());

                    Verify.verifyNotNull(userType);

                    columnMetadata = new UserDataTypeMetadata(columnMetadata, tableMetadata, udtColumnAnnotation.udtType(), userType);
                }

                if (field.isAnnotationPresent(PrimaryKey.class)) {
                    PrimaryKey primaryKeyAnnotation = field.getDeclaredAnnotation(PrimaryKey.class);
                    columnMetadata = new PrimaryKeyMetadata(columnMetadata, primaryKeyAnnotation.order(), primaryKeyAnnotation.isPartitionKey());
                }


                return columnMetadata;
            }
        } catch (NoSuchFieldException e) {
            LOGGER.warn("Can't get field", e);
        }

        return null;
    }
}