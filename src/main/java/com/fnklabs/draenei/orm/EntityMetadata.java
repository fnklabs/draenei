package com.fnklabs.draenei.orm;

import com.datastax.driver.core.*;
import com.fnklabs.draenei.orm.exception.MetadataException;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

class EntityMetadata {
    private String tableName;
    private boolean compactStorage;
    private int maxFetchSize;
    private ConsistencyLevel consistencyLevel;

    private HashMap<String, List<FieldMetadata>> columnsMetadata = new HashMap<>();
    private TableMetadata tableMetadata;

    private HashMap<Integer, PrimaryKeyMetadata> primaryKeys = new HashMap<>();

    public EntityMetadata(String tableName, boolean compactStorage, int maxFetchSize, ConsistencyLevel consistencyLevel, TableMetadata tableMetadata) {
        this.tableName = tableName;
        this.compactStorage = compactStorage;
        this.maxFetchSize = maxFetchSize;
        this.consistencyLevel = consistencyLevel;
        this.tableMetadata = tableMetadata;
    }

    protected void addColumnMetadata(FieldMetadata fieldMetadata) {
        List<FieldMetadata> metadataList = columnsMetadata.getOrDefault(fieldMetadata.getName(), new ArrayList<>());
        metadataList.add(fieldMetadata);

        columnsMetadata.put(fieldMetadata.getName(), metadataList);

        metadataList.forEach(metadata -> {
            if (metadata instanceof PrimaryKeyMetadata) {
                primaryKeys.put(((PrimaryKeyMetadata) metadata).getOrder(), (PrimaryKeyMetadata) metadata);
            }
        });
    }

    protected Optional<PrimaryKeyMetadata> getPrimaryKey(int oder) {
        return Optional.of(primaryKeys.get(oder));
    }

    protected <T> ByteBuffer serialize(FieldMetadata<T> fieldMetadata, Object value) {
        if (value == null) {
            return null;
        }
        ColumnMetadata column = tableMetadata.getColumn(fieldMetadata.getName());

        if (value.getClass().isEnum()) {
            return DataType.text().serialize(value.toString(), ProtocolVersion.NEWEST_SUPPORTED);
        }

        return column.getType().serialize(value, ProtocolVersion.NEWEST_SUPPORTED);
    }

    protected List<FieldMetadata> getColumns() {
        List<FieldMetadata> fieldMetadataList = new ArrayList<>();

        columnsMetadata.forEach((columnName, columnsMetadata) -> {
            columnsMetadata.stream().filter(item -> item.getClass().equals(FieldMetadata.class)).forEach(fieldMetadataList::add);
        });

        return fieldMetadataList;
    }

    protected boolean isCompactStorage() {
        return compactStorage;
    }

    protected <T> T deserialize(FieldMetadata<T> fieldMetadata, ByteBuffer bytesUnsafe) {
        if (bytesUnsafe == null) {
            return null;
        }
        ColumnMetadata column = tableMetadata.getColumn(fieldMetadata.getName());

        if (fieldMetadata.getType().isEnum()) {
            Object value = DataType.text().deserialize(bytesUnsafe, ProtocolVersion.NEWEST_SUPPORTED);

            Object[] enumConstants = fieldMetadata.getType().getEnumConstants();

            HashMap<String, Object> fromStringEnum = new HashMap<String, Object>(enumConstants.length);

            for (Object constant : enumConstants)
                fromStringEnum.put(constant.toString(), constant);

            return (T) fromStringEnum.get(value);

        }

        return (T) column.getType().deserialize(bytesUnsafe, ProtocolVersion.NEWEST_SUPPORTED);
    }

    protected String getTableName() {
        return tableName;
    }

    protected int getClusteringKeysSize() {
        return tableMetadata.getClusteringColumns().size();
    }

    protected int getCompositeKeysSize() {

        int compositeKeysSize = columnsMetadata.entrySet()
                                               .stream()
                                               .mapToInt(entry -> {
                                                           boolean hasCompositeKey = entry.getValue()
                                                                                          .stream()
                                                                                          .anyMatch(item -> item instanceof CompositeKeyMetadata);

                                                           if (hasCompositeKey) {
                                                               return 1;
                                                           }

                                                           return 0;
                                                       }
                                               ).sum();

        return compositeKeysSize;
    }

    protected int getMaxFetchSize() {
        return maxFetchSize;
    }

    protected int getMinPrimaryKeys() {
        if (getCompositeKeysSize() > 0) {
            return getCompositeKeysSize();
        }

        return 1;
    }

    protected int getPrimaryKeysSize() {
        return tableMetadata.getPrimaryKey().size();
    }

    protected ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    protected void validate() {
        if (StringUtils.isEmpty(getTableName())) {
            throw new MetadataException(String.format("Invalid table name: \"%s\"", getTableName()));
        }

        if (getPrimaryKeysSize() < 1) {
            throw new MetadataException(String.format("Entity \"%s\"must contains primary key", getClass().getName()));
        }
    }
}
