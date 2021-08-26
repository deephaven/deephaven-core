/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import org.jetbrains.annotations.NotNull;

/**
 * Type adapter for CompressedStrings.
 */
public class StringCacheTypeAdapterCompressedStringImpl
    implements StringCacheTypeAdapter<CompressedString> {

    public static final StringCacheTypeAdapter<CompressedString> INSTANCE =
        new StringCacheTypeAdapterCompressedStringImpl();

    private static final CompressedString EMPTY_VALUE = new CompressedString("");

    private StringCacheTypeAdapterCompressedStringImpl() {}

    @NotNull
    @Override
    public final Class<CompressedString> getType() {
        return CompressedString.class;
    }

    @Override
    @NotNull
    public final CompressedString empty() {
        return EMPTY_VALUE;
    }

    @NotNull
    @Override
    public final CompressedString create(@NotNull final String string) {
        return new CompressedString(string);
    }

    @NotNull
    @Override
    public final CompressedString create(@NotNull final StringCompatible protoString) {
        return protoString.toCompressedString();
    }

    @Override
    public final boolean areEqual(@NotNull final CharSequence key,
        @NotNull final CompressedString value) {
        return CharSequenceUtils.contentEquals(key, value);
    }
}
