package com.fnklabs.draenei.orm;

import com.datastax.driver.core.TypeCodec;
import org.jetbrains.annotations.Nullable;


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


    @Override
    public String getName() {
        return columnMetadata.getName();
    }


    @Override
    public Class getFieldType() {
        return columnMetadata.getFieldType();
    }

    @Override
    public void writeValue(Object entity, @Nullable Object value) {
        columnMetadata.writeValue(entity, value);
    }

    @Nullable
    @Override
    public <FieldType> FieldType readValue(Object object) throws ClassCastException {
        return columnMetadata.readValue(object);
    }

    @Override
    public <FieldType> TypeCodec<FieldType> typeCodec() {
        return columnMetadata.typeCodec();
    }
}
