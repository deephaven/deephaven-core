package io.deephaven.util.codec;

import io.deephaven.util.BooleanUtils;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

/**
 * ObjectCodec implementation for Maps of String to Boolean.
 *
 * Each map is encoded as an integer length, followed by UTF-8 encoded strings for each key and the
 * value.
 *
 * A null map is represented as an array of zero bytes.
 */
@SuppressWarnings("unused")
public class StringBooleanMapCodec extends StringKeyedMapCodec<Boolean> {
    public StringBooleanMapCodec(@Nullable String arguments) {
        super(arguments);
    }

    @Override
    int getValueSize() {
        return Byte.BYTES;
    }

    @Override
    Boolean decodeValue(ByteBuffer byteBuffer) {
        return BooleanUtils.byteAsBoolean(byteBuffer.get());
    }

    @Override
    void encodeValue(ByteBuffer scratch, Boolean value) {
        scratch.put(BooleanUtils.booleanAsByte(value));
    }
}
