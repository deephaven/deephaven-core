package io.deephaven.util.codec;

import io.deephaven.util.EncodingUtil;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * ObjectCodec implementation for Maps of String to String.
 *
 * Each map is encoded as an integer length, followed by UTF-8 encoded strings for each key and
 * value.
 *
 * A null map is represented as an array of zero bytes.
 */
@SuppressWarnings("unused")
public class StringStringMapCodec extends StringKeyedMapCodec<String> {
    public StringStringMapCodec(@Nullable String arguments) {
        super(arguments);
    }

    @Override
    int estimateSize(Map<String, String> input) {
        int estimate = Integer.BYTES;
        for (final Map.Entry<String, String> entry : input.entrySet()) {
            estimate += entry.getKey().length() + Integer.BYTES;
            estimate += entry.getValue().length() + Integer.BYTES;
        }
        estimate *= 1.1;
        return estimate;
    }

    @Override
    int getValueSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    String decodeValue(ByteBuffer byteBuffer) {
        return EncodingUtil.getUtf8String(byteBuffer);
    }

    @Override
    void encodeValue(ByteBuffer scratch, String value) {
        EncodingUtil.putUtf8String(scratch, value);
    }
}
