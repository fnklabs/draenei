package com.fnklabs.draenei.orm;

import java.beans.PropertyDescriptor;

class CompositeKeyMetadata<T> extends PrimaryKeyMetadata<T> {

    public CompositeKeyMetadata(PropertyDescriptor propertyDescriptor, String name, int order, Class<T> type) {
        super(propertyDescriptor, name, order, type);
    }
}
