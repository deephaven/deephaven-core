package io.deephaven.util.codec;

import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

/**
 * ObjectCodec implementation for Maps of String to Float.
 *
 * Each map is encoded as an integer length, followed by UTF-8 encoded strings for each key and the
 * value.
 *
 * A null map is represented as an array of zero bytes.
 */
@SuppressWarnings("unused")
public class StringFloatMapCodec extends StringKeyedMapCodec<Float> {
    public StringFloatMapCodec(@Nullable String arguments) {
        super(arguments);
    }

    @Override
    int getValueSize() {
        return Float.BYTES;
    }

    @Override
    Float decodeValue(ByteBuffer byteBuffer) {
        final float value = byteBuffer.getFloat();
        return value == QueryConstants.NULL_FLOAT ? null : value;
    }

    @Override
    void encodeValue(ByteBuffer scratch, Float value) {
        scratch.putFloat(value == null ? QueryConstants.NULL_FLOAT : value);
    }
}
