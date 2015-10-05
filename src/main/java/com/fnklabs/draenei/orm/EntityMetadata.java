package com.fnklabs.draenei.orm;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TableMetadata;
import com.fnklabs.draenei.CassandraClient;
import com.fnklabs.draenei.orm.annotations.Column;
import com.fnklabs.draenei.orm.annotations.Enumerated;
import com.fnklabs.draenei.orm.annotations.PrimaryKey;
import com.fnklabs.draenei.orm.annotations.Table;
import com.fnklabs.draenei.orm.exception.MetadataException;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class EntityMetadata {
    public static final Logger LOGGER = LoggerFactory.getLogger(EntityMetadata.class);
    @NotNull
    private final String tableName;
    private final boolean compactStorage;
    private final int maxFetchSize;
    @NotNull
    private final ConsistencyLevel consistencyLevel;
    @NotNull
    private final HashMap<String, List<ColumnMetadata>> columnsMetadata = new HashMap<>();
    @NotNull
    private final TableMetadata tableMetadata;
    @NotNull
    private final HashMap<Integer, PrimaryKeyMetadata> primaryKeys = new HashMap<>();

    public EntityMetadata(@NotNull String tableName,
                          boolean compactStorage,
                          int maxFetchSize,
                          @NotNull ConsistencyLevel consistencyLevel,
                          @NotNull TableMetadata tableMetadata) {
        this.tableName = tableName;
        this.compactStorage = compactStorage;
        this.maxFetchSize = maxFetchSize;
        this.consistencyLevel = consistencyLevel;
        this.tableMetadata = tableMetadata;
    }

    protected void addColumnMetadata(@NotNull ColumnMetadata columnMetadata) {
        List<ColumnMetadata> metadataList = columnsMetadata.getOrDefault(columnMetadata.getName(), new ArrayList<>());
        metadataList.add(columnMetadata);

        columnsMetadata.put(columnMetadata.getName(), metadataList);

        metadataList.forEach(metadata -> {
            if (metadata instanceof PrimaryKeyMetadata) {
                primaryKeys.put(((PrimaryKeyMetadata) metadata).getOrder(), (PrimaryKeyMetadata) metadata);
            }
        });
    }

    protected Optional<PrimaryKeyMetadata> getPrimaryKey(int oder) {
        return Optional.ofNullable(primaryKeys.get(oder));
    }

    protected <T> ByteBuffer serialize(ColumnMetadata<T> columnMetadata, Object value) {
        if (value == null) {
            return null;
        }
        com.datastax.driver.core.ColumnMetadata column = tableMetadata.getColumn(columnMetadata.getName());

        if (value.getClass().isEnum()) {
            return DataType.text().serialize(value.toString(), ProtocolVersion.NEWEST_SUPPORTED);
        }

        return column.getType().serialize(value, ProtocolVersion.NEWEST_SUPPORTED);
    }

    protected <T> T deserialize(ColumnMetadata<T> columnMetadata, ByteBuffer bytesUnsafe) {
        if (bytesUnsafe == null) {
            return null;
        }
        com.datastax.driver.core.ColumnMetadata column = tableMetadata.getColumn(columnMetadata.getName());

        if (columnMetadata.getType().isEnum()) {
            Object value = DataType.text().deserialize(bytesUnsafe, ProtocolVersion.NEWEST_SUPPORTED);

            Object[] enumConstants = columnMetadata.getType().getEnumConstants();

            HashMap<String, Object> fromStringEnum = new HashMap<String, Object>(enumConstants.length);

            for (Object constant : enumConstants)
                fromStringEnum.put(constant.toString(), constant);

            return (T) fromStringEnum.get(value);

        }

        return (T) column.getType().deserialize(bytesUnsafe, ProtocolVersion.NEWEST_SUPPORTED);
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
                              .flatMap(entry -> {
                                  return entry.getValue().stream().filter(item -> item.getClass().equals(ColumnMetadata.class));
                              })
                              .collect(Collectors.toList());
    }

    @NotNull
    protected String getTableName() {
        return tableName;
    }

    protected int getClusteringKeysSize() {
        return tableMetadata.getClusteringColumns().size();
    }

    protected int getPartitionKeysSize() {
        int compositeKeysSize = columnsMetadata.entrySet()
                                               .stream()
                                               .mapToInt(entry -> {
                                                           boolean anyMatch = entry.getValue()
                                                                                   .stream()
                                                                                   .anyMatch(item -> {
                                                                                       return item instanceof PrimaryKeyMetadata && ((PrimaryKeyMetadata) item).isPartitionKey();
                                                                                   });

                                                           return anyMatch ? 1 : 0;
                                                       }
                                               ).sum();

        return compositeKeysSize;
    }

    protected int getMaxFetchSize() {
        return maxFetchSize;
    }

    protected int getMinPrimaryKeys() {
        if (getPartitionKeysSize() > 0) {
            return getPartitionKeysSize();
        }

        return 1;
    }

    protected int getPrimaryKeysSize() {
        return tableMetadata.getPrimaryKey().size();
    }

    @NotNull
    protected ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    protected void validate() {
        if (StringUtils.isEmpty(getTableName())) {
            throw new MetadataException(String.format("Invalid table name: \"%s\"", getTableName()));
        }

        if (getPartitionKeysSize() < 1) {
            throw new MetadataException(String.format("Entity \"%s\"must contains primary key", getClass().getName()));
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

        EntityMetadata entityMetadata = new EntityMetadata(
                tableAnnotation.name(),
                tableAnnotation.compactStorage(),
                tableAnnotation.fetchSize(),
                tableAnnotation.consistencyLevel(),
                cassandraClient.getTableMetadata(tableAnnotation.name())
        );

        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(clazz);

            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

            for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {

                try {
                    Field field = clazz.getDeclaredField(propertyDescriptor.getName());

                    List<ColumnMetadata> columnMetadataList = buildColumnMetadata(propertyDescriptor, field);

                    columnMetadataList.forEach(entityMetadata::addColumnMetadata);
                } catch (NoSuchFieldException e) {
                }

//                LOGGER.debug("Property descriptor: {} {}", propertyDescriptor.getName(), propertyDescriptor.getDisplayName());
            }
        } catch (IntrospectionException e) {
            LOGGER.warn(e.getMessage(), e);
        }

        entityMetadata.validate();

        return entityMetadata;
    }

    /**
     * Build column metadata from field
     *
     * @param propertyDescriptor Field property descriptor
     * @param field              Class field
     *
     * @return Column metadata
     */
    private static List<ColumnMetadata> buildColumnMetadata(@NotNull PropertyDescriptor propertyDescriptor, @NotNull Field field) {
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();

        Column columnAnnotation = field.getDeclaredAnnotation(Column.class);

        if (columnAnnotation != null) {

            ColumnMetadata<?> columnMetadata = buildColumnMetadata(propertyDescriptor, field, columnAnnotation);

            columnMetadataList.add(columnMetadata);

            EnumeratedMetadata<?> enumeratedMetadata = buildEnumeratedMetadata(propertyDescriptor, field, columnMetadata);

            if (enumeratedMetadata != null) {
                columnMetadataList.add(enumeratedMetadata);
            }

            PrimaryKeyMetadata<?> primaryKeyMetadata = buildPrimaryKeyMetadata(propertyDescriptor, field, columnAnnotation);

            if (primaryKeyMetadata != null) {
                columnMetadataList.add(primaryKeyMetadata);
            }
        }

        return columnMetadataList;
    }

    @Nullable
    private static PrimaryKeyMetadata<?> buildPrimaryKeyMetadata(@NotNull PropertyDescriptor propertyDescriptor, @NotNull Field field, @NotNull Column columnAnnotation) {
        PrimaryKey primaryKeyAnnotation = field.getDeclaredAnnotation(PrimaryKey.class);

        if (primaryKeyAnnotation != null) {
            PrimaryKeyMetadata<?> primaryKeyMetadata = new PrimaryKeyMetadata<>(
                    propertyDescriptor,
                    columnAnnotation.name(),
                    primaryKeyAnnotation.order(),
                    primaryKeyAnnotation.isPartitionKey(),
                    field.getType()
            );

            return primaryKeyMetadata;
        }
        return null;
    }

    @Nullable
    private static EnumeratedMetadata<?> buildEnumeratedMetadata(@NotNull PropertyDescriptor propertyDescriptor, @NotNull Field field, @NotNull ColumnMetadata<?> columnMetadata) {
        Enumerated enumeratedAnnotation = field.getDeclaredAnnotation(Enumerated.class);

        if (enumeratedAnnotation != null) {
            EnumeratedMetadata<?> e = new EnumeratedMetadata<>(propertyDescriptor, field.getType(), columnMetadata.getName());

            return e;
        }

        return null;
    }

    @NotNull
    private static ColumnMetadata<?> buildColumnMetadata(@NotNull PropertyDescriptor propertyDescriptor, @NotNull Field field, @NotNull Column columnAnnotation) {
        String columnName = columnAnnotation.name();

        if (StringUtils.isEmpty(columnName)) {
            columnName = propertyDescriptor.getName();
        }

        return new ColumnMetadata<>(propertyDescriptor, field.getType(), columnName);
    }
}
