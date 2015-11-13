package com.fnklabs.draenei.orm;

import java.beans.PropertyDescriptor;

class PrimaryKeyMetadata<T> extends ColumnMetadata<T> {
    /**
     * Primary key order
     */
    private final int order;

    /**
     * Flag that can determine if current key is belong to partition keys
     */
    private final boolean isPartitionKey;


    /**
     * @param propertyDescriptor Field property descriptor
     * @param name               Primary key name
     * @param order              Primary key order (from 0 to n) 0 means first
     * @param isPartitionKey     Flag that can determine if current key is belong to partition keys
     * @param type               Field java class type
     */
    public PrimaryKeyMetadata(PropertyDescriptor propertyDescriptor,
                              String name,
                              int order,
                              boolean isPartitionKey,
                              Class<T> type,
                              com.datastax.driver.core.ColumnMetadata columnMetadata) {
        super(propertyDescriptor, type, name, columnMetadata);
        this.order = order;
        this.isPartitionKey = isPartitionKey;
    }

    public int getOrder() {
        return order;
    }

    public boolean isPartitionKey() {
        return isPartitionKey;
    }
}
