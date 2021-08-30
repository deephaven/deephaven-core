package io.deephaven.util.codec;

import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

/**
 * ObjectCodec implementation for Maps of String to Long.
 *
 * Each map is encoded as an integer length, followed by UTF-8 encoded strings for each key and the
 * value.
 *
 * A null map is represented as an array of zero bytes.
 */
@SuppressWarnings("unused")
public class StringLongMapCodec extends StringKeyedMapCodec<Long> {
    public StringLongMapCodec(@Nullable String arguments) {
        super(arguments);
    }

    @Override
    int getValueSize() {
        return Long.BYTES;
    }

    @Override
    Long decodeValue(ByteBuffer byteBuffer) {
        final long value = byteBuffer.getLong();
        return value == QueryConstants.NULL_LONG ? null : value;
    }

    @Override
    void encodeValue(ByteBuffer scratch, Long value) {
        scratch.putLong(value == null ? QueryConstants.NULL_LONG : value);
    }
}
