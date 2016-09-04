package com.fnklabs.draenei.orm;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class UserDataTypeMetadata<E, T> implements ColumnMetadata<E, T> {
    public static final Logger LOGGER = LoggerFactory.getLogger(UserDataTypeMetadata.class);
    @NotNull
    private final ColumnMetadata<E, T> columnMetadata;

    @NotNull
    private final Class<T> udtClassType;

    @NotNull
    private final UserType udtType;

    private Map<String, ColumnMetadata> udtColumnsMetadata = new HashMap<>();

    UserDataTypeMetadata(@NotNull Class udtClassType,
                         @NotNull UserType udtType,
                         @NotNull ColumnMetadata columnMetadata) {
        this.columnMetadata = columnMetadata;
        this.udtClassType = udtClassType;
        this.udtType = udtType;

        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(udtClassType);

            for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {

                ColumnMetadata udtColumnMetadata = EntityMetadata.buildUdtColumnMetadata(propertyDescriptor, udtClassType, udtType);

                if (udtColumnMetadata != null) {
                    udtColumnsMetadata.put(udtColumnMetadata.getName(), udtColumnMetadata);
                }

                LOGGER.debug("Property descriptor: {} {}", propertyDescriptor.getName(), propertyDescriptor.getDisplayName());
            }
        } catch (IntrospectionException e) {
            LOGGER.warn("Can't build column metadata", e);
        }
    }

    @NotNull
    @Override
    public String getName() {
        return columnMetadata.getName();
    }

    @NotNull
    @Override
    public Class<T> getFieldType() {
        return columnMetadata.getFieldType();
    }

    @Nullable
    @Override
    public T readValue(@NotNull E object) {
        return columnMetadata.readValue(object);
    }

    @Override
    public void writeValue(@NotNull E entity, @Nullable T value) {
        columnMetadata.writeValue(entity, value);
    }

    @Override
    public ByteBuffer serialize(@Nullable T value) {
        return CodecRegistry.DEFAULT_INSTANCE.codecFor(udtType).serialize(mapToUdt(value), ProtocolVersion.NEWEST_SUPPORTED);
    }

    @Override
    public T deserialize(@Nullable ByteBuffer data) {
        Object deserializedValue = columnMetadata.deserialize(data);

        if (deserializedValue instanceof Collection) {
            Stream<Object> objectStream = ((Collection<UDTValue>) deserializedValue).stream()
                                                                                    .flatMap(udtValue -> {
                                                                                        try {
                                                                                            Object newInstance = toObject(udtValue);

                                                                                            return Stream.of(newInstance);
                                                                                        } catch (InstantiationException | IllegalAccessException e) {
                                                                                            LOGGER.warn("Can't map to entity", e);
                                                                                        }

                                                                                        return Stream.empty();
                                                                                    });

            if (deserializedValue instanceof Set) {
                return (T) objectStream.collect(Collectors.toSet());
            } else if (deserializedValue instanceof List) {
                return (T) objectStream.collect(Collectors.toList());
            }
        } else if (deserializedValue instanceof UDTValue) {

            UDTValue udtValue = (UDTValue) deserializedValue;
            try {
                return (T) toObject(udtValue);
            } catch (InstantiationException | IllegalAccessException e) {
                LOGGER.warn("Can't map to entity", e);
            }
        }
        return null;
    }

    private T toObject(UDTValue udtValue) throws InstantiationException, IllegalAccessException {
        T newInstance = udtClassType.newInstance();

        udtType.getFieldNames()
               .forEach(fieldName -> {
                   try {
                       ByteBuffer dataBuffer = udtValue.getBytesUnsafe(fieldName);

                       ColumnMetadata columnMetadata = udtColumnsMetadata.get(fieldName);

                       Object fieldValue = columnMetadata.deserialize(dataBuffer);

                       columnMetadata.writeValue(newInstance, fieldValue);
                   } catch (IllegalArgumentException e) {
                       LOGGER.warn(String.format("Invalid field `%s` for UDT `%s` ", fieldName, getName()));
                   }

               });

        return newInstance;
    }

    @NotNull
    private UDTValue mapToUdt(@Nullable Object udtValue) {
        UDTValue udtValueInstance = udtType.newValue();

        if (udtValue == null) {
            return udtValueInstance;
        }

        udtType.getFieldNames()
               .forEach(field -> {
                   ColumnMetadata columnMetadata = udtColumnsMetadata.get(field);

                   try {
                       Object fieldValue = columnMetadata.readValue(udtValue);
                       ByteBuffer byteBuffer = columnMetadata.serialize(fieldValue);

//                       ByteBuffer serializedFieldValue = udtType.getFieldType(field).set(fieldValue, ProtocolVersion.NEWEST_SUPPORTED);
                       udtValueInstance.setBytesUnsafe(columnMetadata.getName(), byteBuffer);
                   } catch (Exception e) {
                       LOGGER.warn(String.format("Can't map to udt [%s]", udtClassType.getName()), e);
                   }
               });

        return udtValueInstance;
    }
}
