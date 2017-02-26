package com.fnklabs.draenei.orm;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        try (Timer timer = MetricsFactory.getMetrics().getTimer("dataprovider.map_to_object")) {
            ReturnValue instance = clazz.newInstance();

            for (ColumnDefinitions.Definition column : row.getColumnDefinitions()) {
                ColumnMetadata columnMetadata = entityMetadata.getColumn(column.getName());

                if (columnMetadata != null) {
                    Object value = row.get(column.getName(), columnMetadata.typeCodec());
                    columnMetadata.writeValue(instance, value);
                }
            }

            return instance;
        } catch (Exception e) {
            LOGGER.warn("Cant retrieve entity instance", e);
        }

        return null;
    }
}
