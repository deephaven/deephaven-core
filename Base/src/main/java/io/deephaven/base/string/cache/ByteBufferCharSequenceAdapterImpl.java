/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * See documentation in the interface and parent class for details.
 */
public class ByteBufferCharSequenceAdapterImpl extends ByteOrientedCharSequenceAdapter {

    private ByteBuffer buffer;
    private int offset;
    private int length;

    public ByteBufferCharSequenceAdapterImpl() {}

    @NotNull
    @Override
    public CompressedString toCompressedString() {
        return new CompressedString(buffer, offset, length);
    }

    @NotNull
    @Override
    public MappedCompressedString toMappedCompressedString() {
        return new MappedCompressedString(buffer, offset, length);
    }

    /**
     * Set the ByteBuffer backing this CharSequenceAdapter.
     * 
     * @param buffer A ByteBuffer instance that contains a proto-String this adapter knows how to
     *        convert.
     * @param offset The index of the first char in buffer that belongs to the proto-String.
     * @param length The length of the proto-String in chars.
     * @return This CharSequenceAdapter.
     */
    public final ByteBufferCharSequenceAdapterImpl set(ByteBuffer buffer, int offset, int length) {
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        cachedHashCode = 0;
        return this;
    }

    @Override
    public final ByteBufferCharSequenceAdapterImpl clear() {
        buffer = null;
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
        return (char) (buffer.get(offset + index) & 0xFF);
    }
}
