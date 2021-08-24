package io.deephaven.util.codec;

import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

/**
 * ObjectCodec implementation for Maps of String to Double.
 *
 * Each map is encoded as an integer length, followed by UTF-8 encoded strings for each key and the
 * value.
 *
 * A null map is represented as an array of zero bytes.
 */
@SuppressWarnings("unused")
public class StringDoubleMapCodec extends StringKeyedMapCodec<Double> {
    public StringDoubleMapCodec(@Nullable String arguments) {
        super(arguments);
    }

    @Override
    int getValueSize() {
        return Double.BYTES;
    }

    @Override
    Double decodeValue(ByteBuffer byteBuffer) {
        final double value = byteBuffer.getDouble();
        return value == QueryConstants.NULL_DOUBLE ? null : value;
    }

    @Override
    void encodeValue(ByteBuffer scratch, Double value) {
        scratch.putDouble(value == null ? QueryConstants.NULL_DOUBLE : value);
    }
}
