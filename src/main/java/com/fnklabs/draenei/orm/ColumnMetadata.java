package com.fnklabs.draenei.orm;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

/**
 * Column metadata interface
 *
 * @param <E> Entity type
 * @param <T> Column type
 */
interface ColumnMetadata<E, T> {
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
    Class<T> getFieldType();

    /**
     * Write value to entity/object
     *
     * @param entity Object on which write method will be invoked
     * @param value  Value to write
     */
    void writeValue(@NotNull E entity, @Nullable T value);

    /**
     * Read value from entity
     *
     * @param entity Object on which read method will be invoked
     *
     * @return T value or null
     */
    @Nullable
    T readValue(@NotNull E entity);

    /**
     * Serialize value to cassandra value
     *
     * @param value Field value
     *
     * @return Serialized value
     */
    ByteBuffer serialize(@Nullable T value);

    /**
     * Deserialize cassandra value to {@code <T>}
     *
     * @param data Value that must be deserialized
     *
     * @return Deserialized value
     */
    T deserialize(@Nullable ByteBuffer data);
}
