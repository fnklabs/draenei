package com.fnklabs.draenei.orm;


import com.datastax.driver.core.ProtocolVersion;
import com.fnklabs.draenei.orm.exception.MetadataException;
import com.google.common.base.MoreObjects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * Column metadata
 */
class BaseColumnMetadata implements ColumnMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseColumnMetadata.class);

    /**
     * Column field java class type
     */
    private final Class type;

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
     * @param propertyDescriptor Field property descriptor
     * @param entityClassType    Entity class types used for log
     * @param type               Column java class type
     * @param name               Column name
     * @param columnMetadata     datastax driver ColumnMetadata
     */
    protected BaseColumnMetadata(PropertyDescriptor propertyDescriptor,
                                 Class entityClassType,
                                 Class type,
                                 String name,
                                 com.datastax.driver.core.ColumnMetadata columnMetadata) {

        if (propertyDescriptor.getReadMethod() == null) {
            throw new MetadataException(String.format("Can't retrieve read method for %s#%s", entityClassType.getName(), propertyDescriptor.getName()));
        }

        if (propertyDescriptor.getWriteMethod() == null) {
            throw new MetadataException(String.format("Can't retrieve write method for %s#%s", entityClassType.getName(), propertyDescriptor.getName()));
        }

        if (columnMetadata == null) {
            throw new MetadataException(String.format("Column metadata[%s] is null", name));
        }

        this.readMethod = propertyDescriptor.getReadMethod();
        this.writeMethod = propertyDescriptor.getWriteMethod();
        this.columnMetadata = columnMetadata;
        this.type = type;
        this.name = name;
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    @Override
    public String getName() {
        return name;
    }

    @NotNull
    @Override
    public Class getFieldType() {
        return type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeValue(@NotNull Object entity, @Nullable Object value) {
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
     * {@inheritDoc}
     */
    @Nullable
    @Override
    public <FieldType> FieldType readValue(@NotNull Object object) {
        Method readMethod = getReadMethod();

        try {
            return (FieldType) readMethod.invoke(object);
        } catch (IllegalAccessException | InvocationTargetException | ClassCastException e) {
            LOGGER.warn("Can't invoke read method: " + readMethod.getName(), e);
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer serialize(Object value) {
        if (value == null) {
            return null;
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
    @SuppressWarnings("Unchecked class type")
    @Override
    public <T> T deserialize(@Nullable ByteBuffer data) {
        if (data == null) {
            return null;
        }

        Object deserializedObject = columnMetadata.getType().deserialize(data, ProtocolVersion.NEWEST_SUPPORTED);

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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("name", getName())
                          .toString();
    }
}
