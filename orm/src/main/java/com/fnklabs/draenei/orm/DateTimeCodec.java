package com.fnklabs.draenei.orm;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.nio.ByteBuffer;

public class DateTimeCodec extends TypeCodec<DateTime> {
    public DateTimeCodec() {
        super(DataType.bigint(), DateTime.class);
    }

    @Override
    public ByteBuffer serialize(DateTime value, ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        }

        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(0, value.getMillis());

        return bb;
    }

    @Override
    public DateTime deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0)
            return null;

        if (bytes.remaining() != 8)
            throw new InvalidTypeException("Invalid 64-bits long value, expecting 8 bytes but got " + bytes.remaining());

        return new DateTime(bytes.getLong(), DateTimeZone.UTC);
    }

    @Override
    public DateTime parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        return new DateTime(Long.parseLong(value), DateTimeZone.UTC);

    }

    @Override
    public String format(DateTime value) {
        if (value == null)
            return "NULL";

        return Long.toString(value.getMillis());
    }
}
