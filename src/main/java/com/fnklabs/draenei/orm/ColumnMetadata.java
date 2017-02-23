package com.fnklabs.draenei.orm;

import com.datastax.driver.core.TypeCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

/**
 * Column metadata interface
 */
interface ColumnMetadata {
    /**
     * Get column name
     *
     * @return Column name
     */
    String getName();

    /**
     * Get field class type
     *
     * @return Class type of field
     */
    Class getFieldType();

    /**
     * Write value to entity/object
     *
     * @param entity Object on which write method will be invoked
     * @param value  Value to write
     */
    void writeValue(Object entity, @Nullable Object value);

    /**
     * Read value from object
     *
     * @param object      Object on which read method will be invoked
     * @param <FieldType> Class type to which value will be casted
     *
     * @return Object value or null
     */
    @Nullable
    <FieldType> FieldType readValue(Object object);


    /**
     * Get type codec for this field
     *
     * @param <FieldType> Field type
     *
     * @return
     */
    <FieldType> TypeCodec<FieldType> typeCodec();
}
