/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import org.jetbrains.annotations.NotNull;

/**
 * This class provides base functionality for several implementations of CharSequenceAdapter.
 * Currently, there's one for ByteBuffers, another for byte arrays, one for sequences of chars, and
 * one for chains of Strings. If you have array-backed, writable ByteBuffers, it's probably best to
 * use the byte array implementation with the backing array, e.g. for ByteBuffer b,
 * ByteBufferCharSequenceAdapterImpl a, proto-String length l, use: a.set(b.array(), b.position() +
 * b.arrayOffset(), l);
 *
 * Note: trim() support hasn't been needed/implemented so far. Note: Only Latin-1 (ISO-8859-1)
 * characters are expected at this time. Bytes are converted to chars one-for-one with the result
 * masked by 0xFF.
 *
 * Implementations are not thread-safe. Pool them, create ThreadLocal instances, or (better)
 * instantiate them along natural concurrency boundaries. Implementations allow any proto-String
 * (that is, anything that can be expressed as a sequence of chars) to be used as a cache key (in
 * ConcurrentUnboundedStringCache instances) and a String creator.
 *
 * Note Subclasses *must* support length(), and charAt(int index).
 *
 * Note The makeString() implementation *must* be consistent with length() and charAt(int index) -
 * that is, we require that makeString().contentEquals(this).
 *
 * Note subSequence(int start, int end) is unsupported by default - no StringCache implementations
 * need it at this time.
 */
public abstract class CharSequenceAdapter implements StringCompatible {

    /**
     * Cached value of our hash code. Subclasses must be sure to zero it when appropriate.
     */
    int cachedHashCode;

    /**
     * Prepare this adapter for re-use, and eliminate references to external resources.
     */
    public abstract CharSequenceAdapter clear();

    // -------------------------------------------------------------------------------------------------------------
    // Final toString(), hashCode(), and equals(Object that) implementations, to ensure correctness.
    // -------------------------------------------------------------------------------------------------------------

    @Override
    @NotNull
    public final String toString() {
        return makeString();
    }

    /**
     * @return A new String consistent with this CharSequenceAdapter.
     */
    protected abstract String makeString();

    @Override
    public final int hashCode() {
        if (cachedHashCode == 0 && length() > 0) {
            cachedHashCode = CharSequenceUtils.hashCode(this);
        }
        return cachedHashCode;
    }

    @Override
    public final boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null) {
            return false;
        }
        // noinspection SimplifiableIfStatement
        if (getClass() != that.getClass()) {
            return false;
        }
        return CharSequenceUtils.contentEquals(this, (CharSequence) that);
    }

    // -------------------------------------------------------------------------------------------------------------
    // Default subSequence(int start, int end) implementation
    // -------------------------------------------------------------------------------------------------------------

    @Override
    public CharSequence subSequence(int start, int end) {
        throw new UnsupportedOperationException();
    }

    // -------------------------------------------------------------------------------------------------------------
    // Comparable<CharSequence> implementation
    // -------------------------------------------------------------------------------------------------------------

    @Override
    public final int compareTo(@NotNull final CharSequence that) {
        return CharSequenceUtils.CASE_SENSITIVE_COMPARATOR.compare(this, that);
    }
}
