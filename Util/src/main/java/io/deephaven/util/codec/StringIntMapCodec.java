package io.deephaven.util.codec;

import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

/**
 * ObjectCodec implementation for Maps of String to Integer.
 *
 * Each map is encoded as an integer length, followed by UTF-8 encoded strings for each key and the value.
 *
 * A null map is represented as an array of zero bytes.
 */
@SuppressWarnings("unused")
public class StringIntMapCodec extends StringKeyedMapCodec<Integer> {
    public StringIntMapCodec(@Nullable String arguments) {
        super(arguments);
    }

    @Override
    int getValueSize() {
        return Integer.BYTES;
    }

    @Override
    Integer decodeValue(ByteBuffer byteBuffer) {
        final int value = byteBuffer.getInt();
        return value == QueryConstants.NULL_INT ? null : value;
    }

    @Override
    void encodeValue(ByteBuffer scratch, Integer value) {
        scratch.putInt(value == null ? QueryConstants.NULL_INT : value);
    }
}
