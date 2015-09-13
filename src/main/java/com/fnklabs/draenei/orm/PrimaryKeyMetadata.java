package com.fnklabs.draenei.orm;

import java.beans.PropertyDescriptor;

class PrimaryKeyMetadata<T> extends FieldMetadata<T> {
    private int order;

    public PrimaryKeyMetadata(PropertyDescriptor propertyDescriptor, String name, int order, Class<T> type) {
        super(propertyDescriptor, type, name);
        this.order = order;
    }

    public int getOrder() {
        return order;
    }
}
