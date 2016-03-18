package com.fnklabs.draenei.orm;

import com.datastax.driver.core.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

/**
 * Map data from {@link Row} to object
 *
 * @param <ReturnValue>
 */
class MapToObjectFunction<ReturnValue> implements Function<Row, ReturnValue> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapToObjectFunction.class);

    private final Class<ReturnValue> clazz;
    private final EntityMetadata entityMetadata;

    MapToObjectFunction(Class<ReturnValue> clazz, EntityMetadata entityMetadata) {
        this.clazz = clazz;
        this.entityMetadata = entityMetadata;
    }

    @Override
    public ReturnValue apply(Row row) {
        ReturnValue instance = null;

        try {
            instance = clazz.newInstance();

            List<ColumnMetadata> columns = entityMetadata.getFieldMetaData();

            for (ColumnMetadata column : columns) {
                if (row.getColumnDefinitions().contains(column.getName())) {

                    ByteBuffer data = row.getBytesUnsafe(column.getName());

                    Object deserializedValue = column.deserialize(data);

                    column.writeValue(instance, deserializedValue);
                }
            }

        } catch (InstantiationException | IllegalAccessException e) {
            LOGGER.warn("Cant retrieve entity instance", e);
        }
        return instance;
    }
}
