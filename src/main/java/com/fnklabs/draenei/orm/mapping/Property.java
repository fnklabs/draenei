package com.fnklabs.draenei.orm.mapping;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Property {
    public static final Logger LOGGER = LoggerFactory.getLogger(Property.class);
    @NotNull
    private final String name;

    @NotNull
    private final Method readMethod;

    @NotNull
    private final Method writeMethod;

    @NotNull
    private final Class propertyClassType;

    @NotNull
    public String getName() {
        return name;
    }

    public Property(String name, Method readMethod, Method writeMethod, Class propertyClassType) {
        this.name = name;
        this.readMethod = readMethod;
        this.writeMethod = writeMethod;
        this.propertyClassType = propertyClassType;
    }

    /**
     * {@inheritDoc}
     */
    public void writeValue(@NotNull Object entity, @Nullable Object value) {
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
    public <FieldType> FieldType readValue(@NotNull Object object) {
        try {
            return (FieldType) getReadMethod().invoke(object);
        } catch (IllegalAccessException | InvocationTargetException | ClassCastException e) {
            LOGGER.warn("Can't invoke read method: " + getReadMethod().getName(), e);
        }

        return null;
    }

    @NotNull
    private Method getReadMethod() {
        return readMethod;
    }

    @NotNull
    private Method getWriteMethod() {
        return writeMethod;
    }
}
