/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import org.jetbrains.annotations.NotNull;

/**
 * Type adapter for Strings.
 */
public class StringCacheTypeAdapterStringImpl implements StringCacheTypeAdapter<String> {

    public static final StringCacheTypeAdapter<String> INSTANCE = new StringCacheTypeAdapterStringImpl();

    private static final String EMPTY_VALUE = "";

    private StringCacheTypeAdapterStringImpl() {}

    @NotNull
    @Override
    public final Class<String> getType() {
        return String.class;
    }

    @Override
    @NotNull
    public final String empty() {
        return EMPTY_VALUE;
    }

    @NotNull
    @Override
    public final String create(@NotNull final String string) {
        return string;
    }

    @NotNull
    @Override
    public final String create(@NotNull final StringCompatible protoString) {
        return protoString.toString();
    }

    @Override
    public final boolean areEqual(@NotNull final CharSequence key, @NotNull final String value) {
        return CharSequenceUtils.contentEquals(key, value);
    }
}
