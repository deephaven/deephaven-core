/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import org.jetbrains.annotations.NotNull;

/**
 * See documentation in the interface and parent class for details.
 */
public class ByteArrayCharSequenceAdapterImpl extends ByteOrientedCharSequenceAdapter {

    private byte byteArray[];
    private int offset;
    private int length;

    public ByteArrayCharSequenceAdapterImpl() {}

    @NotNull
    @Override
    public CompressedString toCompressedString() {
        return new CompressedString(byteArray, offset, length);
    }

    @NotNull
    @Override
    public MappedCompressedString toMappedCompressedString() {
        return new MappedCompressedString(byteArray, offset, length);
    }

    /**
     * Set the byte array backing this CharSequenceAdapter.
     * 
     * @param byteArray A byte[] instance that contains a proto-String this adapter knows how to convert.
     * @param offset The index of the first char in byteArray that belongs to the proto-String.
     * @param length The length of the proto-String in chars.
     * @return This CharSequenceAdapter.
     */
    public final ByteArrayCharSequenceAdapterImpl set(byte byteArray[], int offset, int length) {
        this.byteArray = byteArray;
        this.offset = offset;
        this.length = length;
        cachedHashCode = 0;
        return this;
    }

    @Override
    public final ByteArrayCharSequenceAdapterImpl clear() {
        byteArray = null;
        offset = 0;
        length = 0;
        cachedHashCode = 0;
        return this;
    }

    @Override
    public final int length() {
        return length;
    }

    @Override
    public final char charAt(final int index) {
        return (char) (byteArray[offset + index] & 0xFF);
    }
}
