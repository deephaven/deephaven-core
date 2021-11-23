package io.deephaven.engine.table.impl;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Utilities for generated code and related classes.
 */
public class DataObjectUtils {

    /**
     * Read a String from the supplied input using {@link ObjectInput#readUTF()}, converting result Strings that match a
     * single null character ({@code "\0"}) to null.
     *
     * @param in The input
     * @return The resulting String after null conversion
     */
    @Nullable
    public static String readAdoString(@NotNull final ObjectInput in) throws IOException {
        final String rawValue = in.readUTF();
        return "\0".equals(rawValue) ? null : rawValue;
    }

    /**
     * Write a String to the supplied output using {@link ObjectOutput#writeUTF(String)}, encoding null values as
     * Strings of a single null character ({@code "\0"}).
     *
     * @param out The output
     * @param value The value to write
     */
    public static void writeAdoString(@NotNull final ObjectOutput out, @Nullable final String value) throws IOException {
        out.writeUTF(value == null ? "\0" : value);
    }
}
