package com.fnklabs.draenei.orm;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.TableMetadata;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.orm.annotations.Table;
import com.fnklabs.draenei.orm.exception.MetadataException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
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

    public EntityMetadata(@NotNull String tableName,
                          boolean compactStorage,
                          int maxFetchSize,
                          @NotNull ConsistencyLevel readConsistencyLevel,
                          @NotNull ConsistencyLevel writeConsistencyLevel,
                          @NotNull TableMetadata tableMetadata) {
        this.tableName = tableName;
        this.compactStorage = compactStorage;
        this.maxFetchSize = maxFetchSize;
        this.readConsistencyLevel = readConsistencyLevel;
        this.writeConsistencyLevel = writeConsistencyLevel;
        this.tableMetadata = tableMetadata;
    }

    public int getPartitionKeySize() {
        return columnsMetadata.entrySet()
                              .stream()
                              .mapToInt(entry -> {
                                          return entry.getValue() instanceof PrimaryKeyMetadata && ((PrimaryKeyMetadata) entry.getValue()).isPartitionKey() ? 1 : 0;
                                      }
                              )
                              .sum();
    }

    protected void addColumnMetadata(@NotNull ColumnMetadata columnMetadata) {
        columnsMetadata.put(columnMetadata.getName(), columnMetadata);

        if (columnMetadata instanceof PrimaryKeyMetadata) {
            PrimaryKeyMetadata primaryKeyMetadata = (PrimaryKeyMetadata) columnMetadata;
            primaryKeys.put(primaryKeyMetadata.getOrder(), primaryKeyMetadata);
        }
    }

    protected Optional<PrimaryKeyMetadata> getPrimaryKey(int oder) {
        return Optional.ofNullable(primaryKeys.get(oder));
    }

    protected boolean isCompactStorage() {
        return compactStorage;
    }

    /**
     * Get field metadata
     *
     * @return
     */
    protected List<ColumnMetadata> getFieldMetaData() {
        return columnsMetadata.entrySet()
                              .stream()
                              .map(entry -> entry.getValue())
                              .collect(Collectors.toList());
    }

    @NotNull
    protected String getTableName() {
        return tableName;
    }

    protected int getClusteringKeysSize() {
        return tableMetadata.getClusteringColumns().size();
    }

    protected int getMaxFetchSize() {
        return maxFetchSize;
    }

    protected int getMinPrimaryKeys() {
        if (getPartitionKeySize() > 0) {
            return getPartitionKeySize();
        }

        return 1;
    }

    protected int getPrimaryKeysSize() {
        return tableMetadata.getPrimaryKey().size();
    }

    @NotNull
    protected ConsistencyLevel getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }

    @NotNull
    protected ConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    /**
     * Validate entity metadata
     *
     * @param entityMetadata Entity Metadata instance
     *
     * @throws MetadataException if invalid metadada was provided
     */
    protected static void validate(@NotNull EntityMetadata entityMetadata) {
        if (StringUtils.isEmpty(entityMetadata.getTableName())) {
            throw new MetadataException(String.format("Invalid table name: \"%s\"", entityMetadata.getTableName()));
        }

        if (entityMetadata.getPartitionKeySize() < 1) {
            throw new MetadataException(String.format("Entity \"%s\"must contains primary key", entityMetadata.getTableName()));
        }
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
    protected static <V> EntityMetadata buildEntityMetadata(@NotNull Class<V> clazz, @NotNull CassandraClient cassandraClient) throws MetadataException {
        Table tableAnnotation = clazz.getAnnotation(Table.class);

        if (tableAnnotation == null) {
            throw new MetadataException(String.format("Table annotation is missing for %s", clazz.getName()));
        }

        TableMetadata tableMetadata = cassandraClient.getTableMetadata(tableAnnotation.name());

        EntityMetadata entityMetadata = new EntityMetadata(
                tableAnnotation.name(),
                tableAnnotation.compactStorage(),
                tableAnnotation.fetchSize(),
                tableAnnotation.readConsistencyLevel(),
                tableAnnotation.writeConsistencyLevel(),
                tableMetadata
        );

        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(clazz);

            for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {

                try {
                    ColumnMetadata columnMetadata = ColumnMetadata.buildColumnMetadata(propertyDescriptor, clazz, tableMetadata);

                    if (columnMetadata != null) {
                        entityMetadata.addColumnMetadata(columnMetadata);
                    }

                    LOGGER.debug("Property descriptor: {} {}", propertyDescriptor.getName(), propertyDescriptor.getDisplayName());
                } catch (NoSuchFieldException e) {
//                    LOGGER.warn("Cant build column metadata", e);
                }
            }
        } catch (IntrospectionException e) {
            LOGGER.warn("Can't build column metadata", e);
        }

        EntityMetadata.validate(entityMetadata);

        return entityMetadata;
    }
}