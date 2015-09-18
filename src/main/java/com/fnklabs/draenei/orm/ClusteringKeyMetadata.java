package com.fnklabs.draenei.orm;

import java.beans.PropertyDescriptor;

class ClusteringKeyMetadata<T> extends PrimaryKeyMetadata<T> {
    public ClusteringKeyMetadata(PropertyDescriptor propertyDescriptor, String name, int order, Class<T> type) {
        super(propertyDescriptor, name, order, type);
    }
}
