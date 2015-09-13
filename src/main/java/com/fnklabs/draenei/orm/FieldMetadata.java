package com.fnklabs.draenei.orm;


import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;

class FieldMetadata<T> {
    private PropertyDescriptor propertyDescriptor;
    private Class<T> type;
    private String name;
    private Method readMethod;
    private Method writeMethod;

    public FieldMetadata(PropertyDescriptor propertyDescriptor, Class<T> type, String name) {
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

    private PropertyDescriptor getPropertyDescriptor() {
        return propertyDescriptor;
    }

    public Class<T> getType() {
        return type;
    }

    public String getName() {
        return name;
    }


}
