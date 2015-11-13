package com.fnklabs.draenei.orm;


import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TableMetadata;
import com.fnklabs.draenei.orm.annotations.Column;
import com.fnklabs.draenei.orm.annotations.PrimaryKey;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Column metadata
 *
 * @param <T> Column java class type
 */
class ColumnMetadata<T> {
    /**
     * Column field java class type
     */
    private final Class<T> type;

    /**
     * Column name
     */
    private final String name;

    /**
     * Field read method
     */
    private final Method readMethod;

    /**
     * Field write method
     */
    private final Method writeMethod;

    /**
     * DataStax column metadata to serialize and deserialize data
     */
    private final com.datastax.driver.core.ColumnMetadata columnMetadata;

    private final Map<String, Object> enumValues = new HashMap<>();

    /**
     * @param propertyDescriptor Field property descriptor
     * @param type               Column java class type
     * @param name               Column name
     * @param columnMetadata
     */
    public ColumnMetadata(PropertyDescriptor propertyDescriptor, Class<T> type, String name, com.datastax.driver.core.ColumnMetadata columnMetadata) {
        this.columnMetadata = columnMetadata;
        readMethod = propertyDescriptor.getReadMethod();
        writeMethod = propertyDescriptor.getWriteMethod();

        this.type = type;
        this.name = name;

        if (type.isEnum()) {
            for (Object constant : type.getEnumConstants()) {
                enumValues.put(constant.toString(), constant);
            }
        }
    }

    public Method getReadMethod() {
        return readMethod;
    }

    public Method getWriteMethod() {
        return writeMethod;
    }

    public Class<T> getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    /**
     * Serialize column value to cassandra data
     *
     * @param value Column java value
     *
     * @return Cassandra data
     */
    protected ByteBuffer serialize(Object value) {
        if (value == null) {
            return null;
        }

        if (value.getClass().isEnum()) {
            return DataType.text().serialize(value.toString(), ProtocolVersion.NEWEST_SUPPORTED);
        }

        return columnMetadata.getType().serialize(value, ProtocolVersion.NEWEST_SUPPORTED);
    }

    /**
     * Deserialize data from cassandra to java type
     *
     * @param data Serialized cassandra type
     * @param <T>  Entity class java type
     *
     * @return Deserialized object
     */
    @SuppressWarnings("Unchecked")
    protected <T> T deserialize(@Nullable ByteBuffer data) {
        if (data == null) {
            return null;
        }

        Object deserializedObject = columnMetadata.getType().deserialize(data, ProtocolVersion.NEWEST_SUPPORTED);

        if (getType().isEnum()) {
            String value = (String) DataType.text().deserialize(data, ProtocolVersion.NEWEST_SUPPORTED);

            return (T) enumValues.get(value);
        }

        return (T) deserializedObject;
    }

    /**
     * Build column metadata from field
     *
     * @param propertyDescriptor Field property descriptor
     * @param clazz              Entity java class
     *
     * @return Column metadata
     */
    @Nullable
    protected static ColumnMetadata buildColumnMetadata(@NotNull PropertyDescriptor propertyDescriptor,
                                                        @NotNull Class clazz,
                                                        @NotNull TableMetadata tableMetadata) throws NoSuchFieldException {
        Field field = clazz.getDeclaredField(propertyDescriptor.getName());

        Column columnAnnotation = field.getDeclaredAnnotation(Column.class);

        if (columnAnnotation != null) {
            String columnName = getColumnName(propertyDescriptor, columnAnnotation);
            com.datastax.driver.core.ColumnMetadata columnMetadata = tableMetadata.getColumn(columnName);

            PrimaryKey primaryKeyAnnotation = field.getDeclaredAnnotation(PrimaryKey.class);

            if (primaryKeyAnnotation != null) {
                PrimaryKeyMetadata<?> primaryKeyMetadata = new PrimaryKeyMetadata<>(
                        propertyDescriptor,
                        columnAnnotation.name(),
                        primaryKeyAnnotation.order(),
                        primaryKeyAnnotation.isPartitionKey(),
                        field.getType(),
                        columnMetadata
                );

                return primaryKeyMetadata;
            }


            return new ColumnMetadata<>(propertyDescriptor, field.getType(), columnName, columnMetadata);
        }

        return null;
    }

    private static String getColumnName(@NotNull PropertyDescriptor propertyDescriptor, @NotNull Column columnAnnotation) {
        String columnName = columnAnnotation.name();

        if (StringUtils.isEmpty(columnName)) {
            columnName = propertyDescriptor.getName();
        }
        return columnName;
    }
}
