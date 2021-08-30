/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import io.deephaven.base.MathUtil;
import org.jetbrains.annotations.NotNull;

/**
 * StringBuilder-like CharSequenceAdapter implementation.
 */
public class CharSequenceAdapterBuilder extends CharSequenceAdapter {

    /**
     * Storage for chars that make up the CharSequence.
     */
    private char storage[] = new char[0];

    /**
     * The number of chars from storage that have been used. Also, the length of the CharSequence.
     */
    private int used;

    @Override
    protected final String makeString() {
        return new String(storage, 0, used);
    }

    @NotNull
    @Override
    public CompressedString toCompressedString() {
        return new CompressedString(storage, 0, used);
    }

    @NotNull
    @Override
    public MappedCompressedString toMappedCompressedString() {
        return new MappedCompressedString(storage, 0, used);
    }

    /**
     * Grow the internal storage of this builder to avoid repeated resizing.
     * 
     * @param needed
     */
    public final void reserveCapacity(final int needed) {
        ensureSpace(needed);
    }

    private char[] ensureSpace(final int needed) {
        if (storage.length < used + needed) {
            final char copy[] = new char[1 << MathUtil.ceilLog2(used + needed)];
            if (used > 0) {
                System.arraycopy(storage, 0, copy, 0, used);
            }
            storage = copy;
        }
        return storage;
    }

    /**
     * Append a slice of a String to this adapter.
     * 
     * @param value A String instance to append to this adapter.
     * @param offset The index of the first char in value to include in the proto-string.
     * @param length The length of the proto-String in chars.
     * @return This adapter.
     */
    public final CharSequenceAdapterBuilder append(final String value, final int offset, final int length) {
        if (length > 0) {
            value.getChars(offset, offset + length, ensureSpace(length), used);
            used += length;
            cachedHashCode = 0;
        }
        return this;
    }

    /**
     * Append a String to this adapter.
     * 
     * @param value A String instance to append to this adapter.
     * @return This adapter.
     */
    public final CharSequenceAdapterBuilder append(final String value) {
        return append(value, 0, value.length());
    }

    /**
     * Append a slice of a CharSequence to this adapter.
     * 
     * @param value A CharSequence instance to append to this adapter.
     * @param offset The index of the first char in value to include in the proto-string.
     * @param length The length of the proto-String in chars.
     * @return This adapter.
     */
    public final CharSequenceAdapterBuilder append(final CharSequence value, final int offset, final int length) {
        if (length > 0) {
            ensureSpace(length);
            cachedHashCode = 0;
            for (int ci = 0; ci < length; ++ci) {
                storage[used++] = value.charAt(offset + ci);
            }
        }
        return this;
    }

    /**
     * Append a CharSequence to this adapter.
     * 
     * @param value A CharSequence instance to append to this adapter.
     * @return This adapter.
     */
    public final CharSequenceAdapterBuilder append(final CharSequence value) {
        return append(value, 0, value.length());
    }

    public final CharSequenceAdapterBuilder append(final char[] value, final int offset, final int length) {
        if (length > 0) {
            System.arraycopy(value, offset, ensureSpace(length), used, length);
            used += length;
            cachedHashCode = 0;
        }
        return this;
    }

    public final CharSequenceAdapterBuilder append(final char[] value) {
        return append(value, 0, value.length);
    }

    /**
     * Append a char to this adapter.
     * 
     * @param value A char to append to this adapter.
     * @return This adapter.
     */
    public final CharSequenceAdapterBuilder append(final char value) {
        ensureSpace(1)[used++] = value;
        cachedHashCode = 0;
        return this;
    }

    public final CharSequenceAdapterBuilder append(final byte[] value, final int offset, final int length) {
        if (length > 0) {
            ensureSpace(length);
            for (int bi = offset; bi < offset + length; ++bi) {
                storage[used++] = (char) (value[bi] & 0xFF);
            }
            cachedHashCode = 0;
        }
        return this;
    }

    public final CharSequenceAdapterBuilder append(final byte[] value) {
        return append(value, 0, value.length);
    }

    /**
     * Append a byte (converted to a char) to this adapter.
     * 
     * @param value A byte to append to this adapter.
     * @return This adapter.
     */
    public final CharSequenceAdapterBuilder append(final byte value) {
        ensureSpace(1)[used++] = (char) (value & 0xFF);
        cachedHashCode = 0;
        return this;
    }

    @Override
    public final CharSequenceAdapterBuilder clear() {
        used = 0;
        cachedHashCode = 0;
        return this;
    }

    @Override
    public final int length() {
        return used;
    }

    @Override
    public final char charAt(final int index) {
        return storage[index];
    }
}
