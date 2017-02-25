package com.fnklabs.draenei.orm;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.nio.ByteBuffer;

public class DateTimeCodec extends TypeCodec<DateTime> {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = ISODateTimeFormat.basicDateTime();

    public DateTimeCodec() {
        super(DataType.varchar(), DateTime.class);
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer serialize(DateTime value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null) {
            return null;
        }

        return ByteBuffer.wrap(value.toString(DATE_TIME_FORMATTER).getBytes());
    }

    /** {@inheritDoc} */
    @Override
    public DateTime deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null) {
            return null;
        }

        String dateTime = new String(Bytes.getArray(bytes));

        return DateTime.parse(dateTime, DATE_TIME_FORMATTER);
    }

    /** {@inheritDoc} */
    @Override
    public DateTime parse(String value) throws InvalidTypeException {
        return value == null ? null : DateTime.parse(value, DATE_TIME_FORMATTER);
    }

    /** {@inheritDoc} */
    @Override
    public String format(DateTime value) throws InvalidTypeException {
        return value == null ? "NULL" : value.toString(DATE_TIME_FORMATTER);
    }
}
