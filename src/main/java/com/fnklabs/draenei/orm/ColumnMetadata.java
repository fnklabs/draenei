package com.fnklabs.draenei.orm;


import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;

/**
 * Column metadata
 *
 * @param <T> Column java class type
 */
class ColumnMetadata<T> {
    private final PropertyDescriptor propertyDescriptor;
    private final Class<T> type;
    private final String name;
    private final Method readMethod;
    private final Method writeMethod;

    public ColumnMetadata(PropertyDescriptor propertyDescriptor, Class<T> type, String name) {
        this.propertyDescriptor = propertyDescriptor;

        readMethod = propertyDescriptor.getReadMethod();
        writeMethod = propertyDescriptor.getWriteMethod();

        this.type = type;
        this.name = name;
    }

    public Method getReadMethod() {
        return readMethod;
    }

    public Method getWriteMethod() {
        return writeMethod;
    }

    public Class<T> getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    private PropertyDescriptor getPropertyDescriptor() {
        return propertyDescriptor;
    }


}
