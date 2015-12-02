package com.fnklabs.draenei.orm;


import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TableMetadata;
import com.fnklabs.draenei.orm.annotations.Column;
import com.fnklabs.draenei.orm.annotations.PrimaryKey;
import com.fnklabs.draenei.orm.exception.MetadataException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
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
    public static final Logger LOGGER = LoggerFactory.getLogger(ColumnMetadata.class);
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
    @NotNull
    private final Method readMethod;

    /**
     * Field write method
     */
    @NotNull
    private final Method writeMethod;

    /**
     * DataStax column metadata to serialize and deserialize data
     */
    private final com.datastax.driver.core.ColumnMetadata columnMetadata;

    /**
     * Enum types
     */
    private final Map<String, Object> enumValues = new HashMap<>();

    /**
     * @param propertyDescriptor Field property descriptor
     * @param type               Column java class type
     * @param name               Column name
     * @param columnMetadata     datastax driver ColumnMetadata
     */
    public ColumnMetadata(PropertyDescriptor propertyDescriptor, Class<T> type, String name, com.datastax.driver.core.ColumnMetadata columnMetadata) {
        this.columnMetadata = columnMetadata;

        if (propertyDescriptor.getReadMethod() == null) {
            throw new MetadataException(String.format("Can't retrieve read method for %s#%s", type.getName(), propertyDescriptor.getName()));
        }

        this.readMethod = propertyDescriptor.getReadMethod();

        if (propertyDescriptor.getWriteMethod() == null) {
            throw new MetadataException(String.format("Can't retrieve write method for %s#%s", type.getName(), propertyDescriptor.getName()));
        }

        this.writeMethod = propertyDescriptor.getWriteMethod();
        this.type = type;
        this.name = name;

        if (type.isEnum()) { // todo unsafe operation. use #name() to retrieve enum name
            for (T constant : type.getEnumConstants()) {
                enumValues.put(constant.toString(), constant);
            }
        }
    }

    public Class<T> getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    protected void writeValue(@NotNull Object entity, @Nullable Object value) {
        if (value == null) { // don't set null value todo check method arguments annotations for NotNull
            return;
        }

        Method writeMethod = getWriteMethod();

        try {
            writeMethod.invoke(entity, value);
        } catch (InvocationTargetException | IllegalAccessException e) {
            LOGGER.warn("Can't invoker write method", e);
        }
    }

    /**
     * Read column value from entity
     *
     * @param object Entity object instance
     *
     * @return
     */
    @Nullable
    protected T readValue(Object object) {
        Method readMethod = getReadMethod();

        try {
            return (T) readMethod.invoke(object);
        } catch (IllegalAccessException | InvocationTargetException | ClassCastException e) {
            LOGGER.warn("Can't invoke read method", e);
        }

        return null;
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

    @NotNull
    private Method getReadMethod() {
        return readMethod;
    }

    @NotNull
    private Method getWriteMethod() {
        return writeMethod;
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
        String name = propertyDescriptor.getName();
        Field field = clazz.getDeclaredField(name);


        Column columnAnnotation = field.getDeclaredAnnotation(Column.class);

        if (columnAnnotation != null) {
            String columnName = getColumnName(propertyDescriptor, columnAnnotation);
            com.datastax.driver.core.ColumnMetadata columnMetadata = tableMetadata.getColumn(columnName);

            PrimaryKey primaryKeyAnnotation = field.getDeclaredAnnotation(PrimaryKey.class);

            if (primaryKeyAnnotation != null) {
                PrimaryKeyMetadata<?> primaryKeyMetadata = new PrimaryKeyMetadata<>(
                        propertyDescriptor,
                        columnName,
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
