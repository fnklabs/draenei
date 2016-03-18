package com.fnklabs.draenei.orm;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;


/**
 * PrimaryKey metadata decorator
 */
class PrimaryKeyMetadata implements ColumnMetadata {
    /**
     * Column metadata object
     */
    private final ColumnMetadata columnMetadata;

    /**
     * Primary key order
     */
    private final int order;

    /**
     * Flag that can determine if current key is belong to partition keys
     */
    private final boolean isPartitionKey;

    /**
     * @param columnMetadata Decorated column metadata
     * @param order          Primary key order (from 0 to n) 0 means first
     * @param isPartitionKey Flag that can determine if current key is belong to partition keys
     */
    PrimaryKeyMetadata(ColumnMetadata columnMetadata, int order, boolean isPartitionKey) {
        this.columnMetadata = columnMetadata;

        this.order = order;
        this.isPartitionKey = isPartitionKey;
    }

    int getOrder() {
        return order;
    }

    boolean isPartitionKey() {
        return isPartitionKey;
    }

    @NotNull
    @Override
    public String getName() {
        return columnMetadata.getName();
    }

    @NotNull
    @Override
    public Class getFieldType() {
        return columnMetadata.getFieldType();
    }

    @Override
    public void writeValue(@NotNull Object entity, @Nullable Object value) {
        columnMetadata.writeValue(entity, value);
    }

    @Nullable
    @Override
    public <FieldType> FieldType readValue(@NotNull Object object) throws ClassCastException {
        return columnMetadata.readValue(object);
    }

    @Override
    public ByteBuffer serialize(Object value) {
        return columnMetadata.serialize(value);
    }

    @Override
    public <T> T deserialize(@Nullable ByteBuffer data) {
        return columnMetadata.deserialize(data);
    }
}
