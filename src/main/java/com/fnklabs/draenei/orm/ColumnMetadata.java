package com.fnklabs.draenei.orm;

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
    @NotNull
    String getName();

    /**
     * Get field class type
     *
     * @return Class type of field
     */
    @NotNull
    Class getFieldType();

    /**
     * Write value to entity/object
     *
     * @param entity Object on which write method will be invoked
     * @param value  Value to write
     */
    void writeValue(@NotNull Object entity, @Nullable Object value);

    /**
     * Read value from object
     *
     * @param object      Object on which read method will be invoked
     * @param <FieldType> Class type to which value will be casted
     *
     * @return Object value or null
     */
    @Nullable
    <FieldType> FieldType readValue(@NotNull Object object);

    /**
     * Serialize value to cassandra value
     *
     * @param value Field value
     *
     * @return Serialized value
     */
    ByteBuffer serialize(@Nullable Object value);

    /**
     * Deserialize cassandra value to {@code <T>}
     *
     * @param data Value that must be deserialized
     * @param <T>  Class type to which value will be casted
     *
     * @return Deserialized value
     *
     * @throws ClassCastException if can't cast read value to FieldType
     */
    <T> T deserialize(@Nullable ByteBuffer data);
}
