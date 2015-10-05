package com.fnklabs.draenei.orm;

import java.beans.PropertyDescriptor;

class EnumeratedMetadata<T> extends ColumnMetadata<T> {
    public EnumeratedMetadata(PropertyDescriptor propertyDescriptor, Class<T> type, String name) {
        super(propertyDescriptor, type, name);
    }
}
