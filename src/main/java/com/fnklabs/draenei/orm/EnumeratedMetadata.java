package com.fnklabs.draenei.orm;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class EnumeratedMetadata implements ColumnMetadata {
    @NotNull
    private final Class enumType;

    /**
     * Enum types
     */
    private final Map<String, Object> enumValues = new HashMap<>();

    private final ColumnMetadata columnMetadata;


    /**
     * @param columnMetadata
     * @param enumType
     */
    protected EnumeratedMetadata(@NotNull ColumnMetadata columnMetadata, @NotNull Class enumType) {
        this.columnMetadata = columnMetadata;
        this.enumType = enumType;


        /**
         * Need for deserialization, because enum saved as string
         */
        if (enumType.isEnum()) { // todo unsafe operation. use #name() to retrieve enum name
            for (Object constant : enumType.getEnumConstants()) {
                enumValues.put(constant.toString(), constant);
            }
        }
    }

    @Override
    public ByteBuffer serialize(final Object value) {
        if (value == null) {
            return null;
        }


        if (value instanceof Collection) {
            Collection<Object> collection = (Collection<Object>) value;

            Stream<String> enumStream = collection.stream()
                                                  .map(new MapEnumToString());
            if (value instanceof Set) {
                Set<String> setValue = enumStream.collect(Collectors.toSet());

                return columnMetadata.serialize(setValue);
            } else if (value instanceof List) {
                List<String> listValue = enumStream.collect(Collectors.toList());

                return columnMetadata.serialize(listValue);
            }


        }

        return columnMetadata.serialize(value.toString());
    }

    private static class MapEnumToString implements Function<Object, String> {

        @Override
        public String apply(Object o) {
            try {
                return o.toString();
            } catch (NullPointerException e) {
                e.printStackTrace();
            }

            return null;
        }
    }

    @Override
    public <T> T deserialize(@Nullable ByteBuffer data) {
        Object deserializedValue = columnMetadata.deserialize(data);

        if (deserializedValue == null) {
            return null;
        }

        if (deserializedValue instanceof Collection) {
            Collection<String> collection = (Collection<String>) deserializedValue;
            Stream<Object> enumStream = collection.stream()
                                                  .map(value -> enumValues.get(value));


            if (deserializedValue instanceof Set) {
                return (T) enumStream.collect(Collectors.toSet());
            } else if (deserializedValue instanceof List) {
                return (T) enumStream.collect(Collectors.toList());
            }

        }

        return (T) enumValues.get(deserializedValue);
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
}
