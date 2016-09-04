package com.fnklabs.draenei.orm;


import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
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
 * {@inheritDoc}
 */
class BaseColumnMetadata<E, T> implements ColumnMetadata<E, T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseColumnMetadata.class);

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
    private final DataType columnDataType;

    /**
     * @param propertyDescriptor Field property descriptor
     * @param entityClassType    Entity class types used for log
     * @param type               Column java class type
     * @param name               Column name
     * @param columnDataType     Column DataType
     */
    BaseColumnMetadata(PropertyDescriptor propertyDescriptor,
                       Class<E> entityClassType,
                       Class<T> type,
                       String name,
                       DataType columnDataType) {

        if (propertyDescriptor.getReadMethod() == null) {
            throw new MetadataException(String.format("Can't retrieve read method for %s#%s", entityClassType.getName(), propertyDescriptor.getName()));
        }

        if (propertyDescriptor.getWriteMethod() == null) {
            throw new MetadataException(String.format("Can't retrieve write method for %s#%s", entityClassType.getName(), propertyDescriptor.getName()));
        }

        if (columnDataType == null) {
            throw new MetadataException(String.format("Column metadata[%s] is null", name));
        }

        this.readMethod = propertyDescriptor.getReadMethod();
        this.writeMethod = propertyDescriptor.getWriteMethod();
        this.columnDataType = columnDataType;
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
    public Class<T> getFieldType() {
        return type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeValue(@NotNull E entity, @Nullable T value) {
        if (value == null) { // don't set null value todo check method arguments annotations for NotNull
            return;
        }

        Method writeMethod = getWriteMethod();

        try {
            writeMethod.invoke(entity, value);
        } catch (InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
            LOGGER.warn(String.format("Can't invoke write method [%s.%s]", writeMethod.getDeclaringClass(), writeMethod.getName()), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nullable
    @Override
    public T readValue(@NotNull Object object) {
        Method readMethod = getReadMethod();

        try {
            return (T) readMethod.invoke(object);
        } catch (IllegalAccessException | InvocationTargetException | ClassCastException e) {
            LOGGER.warn("Can't invoke read method: " + readMethod.getName(), e);
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer serialize(T value) {
        if (value == null) {
            return null;
        }

        return getCodec().serialize(value, ProtocolVersion.NEWEST_SUPPORTED);
    }

    /**
     * Deserialize data from cassandra to java type
     *
     * @param data Serialized cassandra type
     *
     * @return Deserialized object
     */
    @SuppressWarnings("Unchecked class type")
    @Override
    public T deserialize(@Nullable ByteBuffer data) {
        if (data == null) {
            return null;
        }

        return getCodec().deserialize(data, ProtocolVersion.NEWEST_SUPPORTED);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("name", getName())
                          .toString();
    }

    private TypeCodec<T> getCodec() {
        return CodecRegistry.DEFAULT_INSTANCE.codecFor(columnDataType, type);
    }

    @NotNull
    private Method getReadMethod() {
        return readMethod;
    }

    @NotNull
    private Method getWriteMethod() {
        return writeMethod;
    }
}
