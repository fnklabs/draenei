package com.fnklabs.draenei.orm;

import java.beans.PropertyDescriptor;

class PrimaryKeyMetadata<T> extends ColumnMetadata<T> {
    /**
     * Primary key order
     */
    private final int order;

    /**
     * Flat that can determine if current key is belong to partition keys
     */
    private final boolean isPartitionKey;

    public PrimaryKeyMetadata(PropertyDescriptor propertyDescriptor, String name, int order, boolean isPartitionKey, Class<T> type) {
        super(propertyDescriptor, type, name);
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
