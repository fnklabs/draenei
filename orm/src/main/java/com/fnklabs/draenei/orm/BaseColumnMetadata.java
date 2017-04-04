package com.fnklabs.draenei.orm;


import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.fnklabs.draenei.orm.exception.MetadataException;
import com.google.common.base.MoreObjects;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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

    private final Method readMethod;

    /**
     * Field write method
     */
    private final Method writeMethod;

    private final DataType dataType;

    private final TypeCodec typeCodec;

    /**
     * @param propertyDescriptor Field property descriptor
     * @param entityClassType    Entity class types used for log
     * @param type               Column java class type
     * @param name               Column name
     * @param columnDataType     Column DataType
     * @param typeCodec
     */
    BaseColumnMetadata(PropertyDescriptor propertyDescriptor,
                       Class entityClassType,
                       Class type,
                       String name,
                       DataType columnDataType,
                       TypeCodec typeCodec) {


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
        this.type = type;
        this.name = name;
        this.dataType = columnDataType;
        this.typeCodec = typeCodec;
    }

    /**
     * {@inheritDoc}
     */

    @Override
    public String getName() {
        return name;
    }


    @Override
    public Class getFieldType() {
        return type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeValue(Object entity, @Nullable Object value) {
        if (value == null) { // don't set null value todo check method arguments annotations for NotNull
            return;
        }

        Method writeMethod = getWriteMethod();

        if (!writeMethod.isAccessible()) {
            writeMethod.setAccessible(true);
        }

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
    public <FieldType> FieldType readValue(Object object) {
        Method readMethod = getReadMethod();

        if (!readMethod.isAccessible()) {
            readMethod.setAccessible(true);
        }

        try {
            return (FieldType) readMethod.invoke(object);
        } catch (IllegalAccessException | InvocationTargetException | ClassCastException e) {
            LOGGER.warn("Can't invoke read method: " + readMethod.getName(), e);
        }

        return null;
    }

    @Override
    public <FieldType> TypeCodec<FieldType> typeCodec() {
        return typeCodec;
    }


    private Method getReadMethod() {
        return readMethod;
    }


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
