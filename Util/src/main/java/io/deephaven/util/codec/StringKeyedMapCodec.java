package io.deephaven.util.codec;

import io.deephaven.util.EncodingUtil;

import java.nio.ByteBuffer;
import java.util.Map;

abstract class StringKeyedMapCodec<V> extends MapCodec<String, V> {
    StringKeyedMapCodec(String arguments) {
        super(arguments);
    }

    @Override
    int estimateSize(Map<String, V> input) {
        int estimate = 0;
        for (final String key : input.keySet()) {
            estimate += key.length() + Integer.BYTES;
        }
        estimate *= 1.1;
        estimate += Integer.BYTES + (getValueSize() * input.size());
        return estimate;
    }

    /**
     * Return the size of the values (presuming they are fixed size).
     *
     * If your values are not fixed size, then you must override the {@link #estimateSize(Map)} method and should throw
     * an UnsupportedOperationException.
     *
     * @return the size of each encoded value
     */
    abstract int getValueSize();

    @Override
    String decodeKey(ByteBuffer byteBuffer) {
        return EncodingUtil.getUtf8String(byteBuffer);
    }

    @Override
    void encodeKey(ByteBuffer scratch, String key) {
        EncodingUtil.putUtf8String(scratch, key);
    }
}
