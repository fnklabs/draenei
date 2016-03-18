package com.fnklabs.draenei.orm;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TableMetadata;
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

class UserDataTypeMetadata implements ColumnMetadata {
    public static final Logger LOGGER = LoggerFactory.getLogger(UserDataTypeMetadata.class);
    @NotNull
    private final ColumnMetadata columnMetadata;

    @NotNull
    private final Class udtClassType;

    @NotNull
    private final UserType udtType;

    private Map<String, ColumnMetadata> udtColumnsMetadata = new HashMap<>();

    UserDataTypeMetadata(@NotNull ColumnMetadata columnMetadata, @NotNull TableMetadata tableMetadata, @NotNull Class udtClassType, @NotNull UserType udtType) {
        this.columnMetadata = columnMetadata;
        this.udtClassType = udtClassType;
        this.udtType = udtType;

        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(udtClassType);

            for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {

                ColumnMetadata udtColumnMetadata = EntityMetadata.buildColumnMetadata(propertyDescriptor, udtClassType, tableMetadata);

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
    public Class getFieldType() {
        return columnMetadata.getFieldType();
    }

    @Override
    public void writeValue(@NotNull Object entity, @Nullable Object value) {
        columnMetadata.writeValue(entity, value);
    }

    @Nullable
    @Override
    public <FieldType> FieldType readValue(@NotNull Object object) {
        return columnMetadata.readValue(object);
    }

    @Override
    public ByteBuffer serialize(@Nullable Object value) {

        if (value instanceof Collection) {

            Stream<UDTValue> stream = ((Collection<Object>) value).stream()
                                                                  .flatMap(item -> {
                                                                      if (item == null) {
                                                                          return Stream.<UDTValue>empty();
                                                                      }

                                                                      return Stream.<UDTValue>of(mapToUdt(value));
                                                                  });

            if (value instanceof Set) {
                return columnMetadata.serialize(stream.collect(Collectors.toSet()));
            } else if (value instanceof List) {
                return columnMetadata.serialize(stream.collect(Collectors.toList()));
            }
        }

        return udtType.serialize(mapToUdt(value), ProtocolVersion.NEWEST_SUPPORTED);
    }

    @Override
    public <T> T deserialize(@Nullable ByteBuffer data) {
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
        }

        return (T) deserializedValue;
    }

    private Object toObject(UDTValue udtValue) throws InstantiationException, IllegalAccessException {
        Object newInstance = udtClassType.newInstance();

        udtType.getFieldNames()
               .forEach(fieldName -> {
                   ColumnMetadata columnMetadata = udtColumnsMetadata.get(fieldName);

                   ByteBuffer dataBuffer = udtValue.getBytesUnsafe(fieldName);

                   Object fieldValue = columnMetadata.deserialize(dataBuffer);

                   columnMetadata.writeValue(newInstance, fieldValue);
               });

        return newInstance;
    }

    @NotNull
    private UDTValue mapToUdt(@NotNull Object udtValue) {
        UDTValue udtValueInstance = udtType.newValue();

        udtType.getFieldNames()
               .forEach(field -> {
                   ColumnMetadata columnMetadata = udtColumnsMetadata.get(field);

                   Object fieldValue = columnMetadata.readValue(udtValue);

                   ByteBuffer serializedFieldValue = columnMetadata.serialize(fieldValue);

                   udtValueInstance.setBytesUnsafe(columnMetadata.getName(), serializedFieldValue);

               });

        return udtValueInstance;
    }
}
