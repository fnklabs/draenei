package com.fnklabs.draenei.orm;

import com.datastax.driver.core.TableMetadata;
import com.fnklabs.draenei.orm.annotations.Column;
import com.fnklabs.draenei.orm.annotations.Enumerated;
import com.fnklabs.draenei.orm.annotations.PrimaryKey;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

class ColumnMetadataBuilder {
    /**
     * Build column metadata from field
     *
     * @param propertyDescriptor Field property descriptor
     * @param clazz              Entity java class
     *
     * @return Column metadata or null if property is not column
     */
    @Nullable
    protected static ColumnMetadata buildColumnMetadata(@NotNull PropertyDescriptor propertyDescriptor, @NotNull Class clazz, @NotNull TableMetadata tableMetadata) {

        try {
            Field field = clazz.getDeclaredField(propertyDescriptor.getName());

            if (field.isAnnotationPresent(Column.class)) {

                Column columnAnnotation = field.getDeclaredAnnotation(Column.class);

                String columnName = getColumnName(propertyDescriptor, columnAnnotation);

                ColumnMetadata columnMetadata = new BaseColumnMetadata(propertyDescriptor, clazz, field.getType(), columnName, tableMetadata.getColumn(columnName));

                if (field.isAnnotationPresent(Enumerated.class)) {
                    Enumerated enumeratedAnnotation = field.getDeclaredAnnotation(Enumerated.class);

                    columnMetadata = new EnumeratedMetadata(columnMetadata, enumeratedAnnotation.enumType());
                }

                if (field.isAnnotationPresent(PrimaryKey.class)) {
                    PrimaryKey primaryKeyAnnotation = field.getDeclaredAnnotation(PrimaryKey.class);
                    columnMetadata = new PrimaryKeyMetadata(columnMetadata, primaryKeyAnnotation.order(), primaryKeyAnnotation.isPartitionKey());
                }

                return columnMetadata;
            }
        } catch (NoSuchFieldException e) {
            LoggerFactory.getLogger(ColumnMetadataBuilder.class).warn("Can't get field", e);
        }

        return null;
    }

    private static String getColumnName(@NotNull PropertyDescriptor propertyDescriptor, @NotNull Column columnAnnotation) {
        String columnName = columnAnnotation.name();

        if (StringUtils.isEmpty(columnName)) {
            columnName = propertyDescriptor.getName();
        }

        return columnName;
    }
}