/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import org.jetbrains.annotations.NotNull;

/**
 * Abstracts type-specific functionality for use by StringCache implementations.
 */
public interface StringCacheTypeAdapter<STRING_LIKE_TYPE extends CharSequence> {

    /**
     * @return The Class of the STRING_LIKE_TYPE instances created by this adapter.
     */
    @NotNull
    Class<STRING_LIKE_TYPE> getType();

    /**
     * @return The implementation-appropriate empty STRING_LIKE_TYPE instance.
     */
    @NotNull
    STRING_LIKE_TYPE empty();

    /**
     * Note: StringCache implementations may choose not hold a lock while invoking this method.
     * 
     * @param string The input String
     * @return A newly allocated STRING_LIKE_TYPE with the same content as string.
     */
    @NotNull
    STRING_LIKE_TYPE create(@NotNull String string);

    /**
     * Note: StringCache implementations may choose not hold a lock while invoking this method.
     * 
     * @param protoString The input StringCompatible
     * @return A newly allocated STRING_LIKE_TYPE with the same content as protoString.
     */
    @NotNull
    STRING_LIKE_TYPE create(@NotNull StringCompatible protoString);

    /**
     * Compare key (Assumed to be a String *or* a StringCompatible) with value (created by this factory).
     * 
     * @param key The key
     * @param value The value
     * @return True iff key and value are equal (according to StringCompatible's implementation).
     */
    boolean areEqual(@NotNull CharSequence key, @NotNull STRING_LIKE_TYPE value);
}
