package io.deephaven.util.codec;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;

public class SimpleStringAsByteArrayCodec implements ObjectCodec<String> {
    private final int expectedWidth;

    public SimpleStringAsByteArrayCodec(@Nullable final String arguments) {
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
                throw new IllegalArgumentException("Unexpected additional arguments after first: " + arguments);
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
    public byte[] encode(@Nullable final String input) {
        if (input == null) {
            throw new IllegalArgumentException(SimpleStringAsByteArrayCodec.class.getSimpleName() + " cannot encode nulls");
        }
        return input.getBytes(StandardCharsets.UTF_8);
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
    public String decode(@NotNull final byte[] input, final int offset, final int length) {
        return new String (input, offset, length, StandardCharsets.UTF_8);
    }

    @Override
    public int expectedObjectWidth() {
        return expectedWidth;
    }
}
