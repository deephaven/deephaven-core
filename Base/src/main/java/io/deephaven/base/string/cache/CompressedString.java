/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Immutable byte[]-backed String replacement.
 */
public final class CompressedString extends AbstractCompressedString<CompressedString> {

    public CompressedString() {}

    public CompressedString(String data) {
        super(data);
    }

    public CompressedString(char[] data, int offset, int length) {
        super(data, offset, length);
    }

    public CompressedString(char[] data) {
        super(data);
    }

    public CompressedString(ByteBuffer data, int offset, int length) {
        super(data, offset, length);
    }

    public CompressedString(ByteBuffer data) {
        super(data);
    }

    public CompressedString(byte[] data, int offset, int length) {
        super(data, offset, length);
    }

    public CompressedString(byte[] data) {
        super(data);
    }

    @Override
    @NotNull
    public CompressedString toCompressedString() {
        return this;
    }

    @Override
    @NotNull
    public MappedCompressedString toMappedCompressedString() {
        return new MappedCompressedString(getData());
    }

    @Override
    protected final CompressedString convertValue(final String string) {
        return new CompressedString(string);
    }

    @Override
    protected final CompressedString convertValue(final byte[] data, final int offset, final int length) {
        return new CompressedString(data, offset, length);
    }

    /**
     * Helper to be statically imported for groovy scripting.
     * 
     * @param value The String to convert
     * @return A new CompressedString with the same contents as value, assuming ISO-8859-1 encoding
     */
    public static CompressedString compress(final String value) {
        return new CompressedString(value);
    }
}
