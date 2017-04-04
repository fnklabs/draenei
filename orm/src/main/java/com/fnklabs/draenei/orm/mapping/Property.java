package com.fnklabs.draenei.orm.mapping;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Property {
    public static final Logger LOGGER = LoggerFactory.getLogger(Property.class);

    private final String name;


    private final Method readMethod;


    private final Method writeMethod;


    private final Class propertyClassType;


    public Property(String name, Method readMethod, Method writeMethod, Class propertyClassType) {
        this.name = name;
        this.readMethod = readMethod;
        this.writeMethod = writeMethod;
        this.propertyClassType = propertyClassType;
    }

    public String getName() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    public void writeValue(Object entity, @Nullable Object value) {
        try {
            getWriteMethod().invoke(entity, value);
        } catch (InvocationTargetException | IllegalAccessException e) {
            LOGGER.warn("Can't invoker write method", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nullable
    public <FieldType> FieldType readValue(Object object) {
        try {
            return (FieldType) getReadMethod().invoke(object);
        } catch (IllegalAccessException | InvocationTargetException | ClassCastException e) {
            LOGGER.warn("Can't invoke read method: " + getReadMethod().getName(), e);
        }

        return null;
    }


    private Method getReadMethod() {
        return readMethod;
    }


    private Method getWriteMethod() {
        return writeMethod;
    }
}
