/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import org.jetbrains.annotations.NotNull;

/**
 * Generic caching interface for Strings and String-like CharSequences.
 */
public interface StringCache<STRING_LIKE_TYPE extends CharSequence> {

    int CAPACITY_UNBOUNDED = -1;

    /**
     * Get a hint about this cache's capacity and behavior.
     * 
     * @return -1 : This is an unbounded cache. 0 : This "cache" doesn't actually perform any caching. >0 : Actual
     *         capacity bound.
     */
    int capacity();

    /**
     * @return The type of the elements in this cache.
     */
    @NotNull
    Class<STRING_LIKE_TYPE> getType();

    /**
     * @return A cached STRING_LIKE_TYPE that represents the empty string.
     */
    @NotNull
    STRING_LIKE_TYPE getEmptyString();

    /**
     * @param protoString The string-like CharSequence to look up
     * @return A cached STRING_LIKE_TYPE that corresponds to the current value of the CharSequence expressed by
     *         protoString
     */
    @NotNull
    STRING_LIKE_TYPE getCachedString(@NotNull StringCompatible protoString);

    /**
     * @param string The String to look up
     * @return A cached STRING_LIKE_TYPE that corresponds to the CharSequence expressed by string
     */
    @NotNull
    STRING_LIKE_TYPE getCachedString(@NotNull String string);

    /**
     * Convenience method to allow arbitrary CharSequence references to be used in lookup.
     * 
     * @param charSequence The CharSequence to look up
     * @return A cached STRING_LIKE_TYPE that corresponds to charSequence
     */
    default @NotNull STRING_LIKE_TYPE getCachedString(@NotNull CharSequence charSequence) {
        return charSequence instanceof StringCompatible ? getCachedString((StringCompatible) charSequence)
                : getCachedString(charSequence.toString());
    }
}
