package io.deephaven.util.codec;

import io.deephaven.datastructures.util.CollectionUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * ObjectCodec implementation for arrays of bytes.
 *
 * We support fixed and variable width encodings. Fixed width may or may not be nullable. Variable width is always
 * nullable. Nullable is true by default and can be set by an optional 2nd argument to the codec.
 * Non-nullable encoding of fixed size arrays saves one byte since we don't need a sentinel value to indicate null in that case.
 *
 */
@SuppressWarnings("unused")
public class ByteArrayCodec implements ObjectCodec<byte[]> {
    private final int expectedWidth;

    private final byte[] nullBytes;

    private final boolean nullable;

    private final byte VALID_INDICATOR = 0;
    private final byte NULL_INDICATOR = -1;

    public ByteArrayCodec(@Nullable final String arguments) {
        if (arguments == null || arguments.trim().isEmpty()) {
            expectedWidth = ObjectCodec.VARIABLE_WIDTH_SENTINEL;
            nullBytes = CollectionUtil.ZERO_LENGTH_BYTE_ARRAY;
            nullable = true;
        } else {
            final int size;
            try {
                final String[] tokens = arguments.split(",");
                size = Integer.parseInt(tokens[0].trim());
                boolean _nullable = true;
                if(tokens.length > 1) {
                    final String nullability = tokens[1].trim();
                    switch(nullability.toLowerCase()) {
                        case "nullable":
                            _nullable = true;
                            break;
                        case "notnull":
                            _nullable = false;
                            break;
                        default:
                            throw new IllegalArgumentException("Unexpected value for nullability (legal values are \"nullable\" or \"notNull\"): " + nullability);
                    }
                }
                nullable = _nullable;
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Error parsing column size: " + ex.getMessage(), ex);
            }
            if(size < 1) {
                throw new IllegalArgumentException("Invalid column size: " + size);
            }
            if(nullable) {
                expectedWidth = size + 1; // encoded byte array is a 1 byte sentinel for null followed by data
                nullBytes = new byte[expectedWidth];
                nullBytes[0] = NULL_INDICATOR;
            } else {
                expectedWidth = size;
                nullBytes = new byte[expectedWidth];
            }
        }
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @NotNull
    @Override
    public byte[] encode(@Nullable final byte[] input) {
        if (input == null) {
            if(nullable) {
                return nullBytes;
            } else {
                throw new UnsupportedOperationException("Cannot encode null, nullable=false");
            }
        }
        if(expectedWidth == ObjectCodec.VARIABLE_WIDTH_SENTINEL) {
            if (input.length == 0) {
                throw new UnsupportedOperationException("ByteArrayCodec cannot encode a zero length byte array, this is reserved to represent null");
            }
            return Arrays.copyOf(input, input.length);
        } else {
            if(nullable) {
                final byte[] v = new byte[input.length + 1];
                v[0] = VALID_INDICATOR;
                System.arraycopy(input, 0, v, 1, input.length);
                return v;
            } else {
                return Arrays.copyOf(input, input.length);
            }
        }
    }

    @Nullable
    @Override
    public byte[] decode(@NotNull final byte[] input, final int offset, final int length) {

        if(expectedWidth == ObjectCodec.VARIABLE_WIDTH_SENTINEL) {
            return length == 0 ? null : Arrays.copyOfRange(input, offset, offset + length);
        } else {
            if(nullable) {
                if (input[offset] == NULL_INDICATOR) {
                    return null;
                } else {
                    return Arrays.copyOfRange(input, offset + 1, offset + length);
                }
            } else {
                return Arrays.copyOfRange(input, offset, offset + length);
            }
        }
    }

    @Override
    public int expectedObjectWidth() {
        return expectedWidth;
    }
}
