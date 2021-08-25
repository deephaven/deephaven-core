package io.deephaven.util.codec;

import io.deephaven.datastructures.util.CollectionUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 * <p>
 * Codec for non-nullable byte arrays that does a no-op encode/decode.
 * <p>
 * One particular instance where this is useful is reading parquet 1.0 data encoded as binary as
 * "raw".
 *
 */
public class SimpleByteArrayCodec implements ObjectCodec<byte[]> {
    private final int expectedWidth;

    public SimpleByteArrayCodec(@Nullable final String arguments) {
        if (arguments == null || arguments.trim().isEmpty()) {
            expectedWidth = ObjectCodec.VARIABLE_WIDTH_SENTINEL;
            return;
        }
        final int size;
        final String[] tokens = arguments.split(",");
        if (tokens.length == 0) {
            expectedWidth = ObjectDecoder.VARIABLE_WIDTH_SENTINEL;
            return;
        }
        try {
            size = Integer.parseInt(tokens[0].trim());
            if (tokens.length > 1) {
                throw new IllegalArgumentException(
                    "Unexpected additional arguments after first: " + arguments);
            }
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Error parsing column size: " + ex.getMessage(), ex);
        }
        if (size < 1) {
            throw new IllegalArgumentException("Invalid column size: " + size);
        }
        expectedWidth = size;
    }

    @NotNull
    @Override
    public byte[] encode(@Nullable final byte[] input) {
        if (input == null) {
            throw new IllegalArgumentException(
                SimpleByteArrayCodec.class.getSimpleName() + " cannot encode nulls");
        }
        return input;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Nullable
    @Override
    public byte[] decode(@NotNull final byte[] input, final int offset, final int length) {
        if (input.length == 0) {
            return CollectionUtil.ZERO_LENGTH_BYTE_ARRAY;
        }
        if (offset == 0 && length == input.length) {
            return input;
        }
        final byte[] output = new byte[length];
        System.arraycopy(input, offset, output, 0, length);
        return output;
    }

    @Override
    public int expectedObjectWidth() {
        return expectedWidth;
    }
}
