/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Marker interface for CharSequences to be used in StringCache implementations.
 */
public interface StringCompatible extends CharSequence, Comparable<CharSequence> {

    /**
     * Convert this StringCompatible into a String. Implementations should not cache result Strings,
     * in order to avoid inadvertently allowing promotion of short-lived objects under generational
     * garbage collection.
     * 
     * @return A newly constructed String representing the same sequence of characters as this
     *         StringCompatible.
     */
    @Override
    @NotNull
    String toString();

    /**
     * Convert this StringCompatible into a CompressedString. Implementations should not cache
     * result CompressedStrings, in order to avoid inadvertently allowing promotion of short-lived
     * objects under generational garbage collection.
     * 
     * @return A newly constructed CompressedString representing the same sequence of characters as
     *         this StringCompatible (or this object, if appropriate).
     */
    @NotNull
    CompressedString toCompressedString();

    /**
     * Convert this StringCompatible into a MappedCompressedString. Implementations should not cache
     * result CompressedStrings, in order to avoid inadvertently allowing promotion of short-lived
     * objects under generational garbage collection.
     * 
     * @return A newly constructed MappedCompressedString representing the same sequence of
     *         characters as this StringCompatible (or this object, if appropriate).
     */
    @NotNull
    MappedCompressedString toMappedCompressedString();

    /**
     * Implementations MUST match the current implementation of String.hashCode().
     * 
     * @return A hashcode value for this StringCompatible that matches the value a String of the
     *         same chars.
     */
    @Override
    int hashCode();

    /**
     * @return true iff that is a StringCompatible of the same class with identical members.
     */
    @Override
    boolean equals(Object that);

    /**
     * Implementations MUST compare StringCompatibles and Strings char-by-char.
     * 
     * @return 0, <0, or >0 if that compares equal-to, less-than, or greater-than this.
     */
    @Override
    int compareTo(@NotNull final CharSequence that);

    /**
     * Compute a hash code for a CharSequence using the algorithm employed by String.hashCode().
     *
     * @param cs The CharSequence to hash
     * @return The hash code
     */
    static int hash(@Nullable final CharSequence cs) {
        if (cs == null) {
            return 0;
        }
        // NB: For these classes/markers, we know we can trust their hashCode implementation to
        // match
        // CharSequenceUtils.hashCode(CharSequence), so use hashCode() directly and allow for
        // caching.
        if (cs instanceof String || cs instanceof StringCompatible) {
            return cs.hashCode();
        }
        return CharSequenceUtils.hashCode(cs);
    }
}
