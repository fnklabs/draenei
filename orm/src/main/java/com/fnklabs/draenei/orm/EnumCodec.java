package com.fnklabs.draenei.orm;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;

import java.nio.ByteBuffer;

public class EnumCodec<T extends Enum<T>> extends TypeCodec<T> {

    public EnumCodec(Class<T> enumClass) {
        super(DataType.varchar(), enumClass);
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer serialize(T value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null) {
            return null;
        }

        return ByteBuffer.wrap(value.name().getBytes());
    }

    /** {@inheritDoc} */
    @Override
    public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null) {
            return null;
        }


        String name = new String(Bytes.getArray(bytes));

        return stringToEnum(name);
    }

    /** {@inheritDoc} */
    @Override
    public T parse(String value) throws InvalidTypeException {
        return value == null ? null : stringToEnum(value);
    }

    /** {@inheritDoc} */
    @Override
    public String format(T value) throws InvalidTypeException {
        return value == null ? "NULL" : value.name();
    }

    private T stringToEnum(String name) {
        return Enum.valueOf((Class<T>) javaType.getRawType(), name);
    }
}
