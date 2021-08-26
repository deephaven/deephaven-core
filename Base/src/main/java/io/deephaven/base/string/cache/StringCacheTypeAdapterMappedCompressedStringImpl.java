/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import org.jetbrains.annotations.NotNull;

/**
 * Type adapter for MappedCompressedStrings.
 */
public class StringCacheTypeAdapterMappedCompressedStringImpl
        implements StringCacheTypeAdapter<MappedCompressedString> {

    public static final StringCacheTypeAdapter<MappedCompressedString> INSTANCE =
            new StringCacheTypeAdapterMappedCompressedStringImpl();

    private static final MappedCompressedString EMPTY_VALUE = new MappedCompressedString("");

    private StringCacheTypeAdapterMappedCompressedStringImpl() {}

    @NotNull
    @Override
    public final Class<MappedCompressedString> getType() {
        return MappedCompressedString.class;
    }

    @Override
    @NotNull
    public final MappedCompressedString empty() {
        return EMPTY_VALUE;
    }

    @NotNull
    @Override
    public final MappedCompressedString create(@NotNull final String string) {
        return new MappedCompressedString(string);
    }

    @NotNull
    @Override
    public final MappedCompressedString create(@NotNull final StringCompatible protoString) {
        return protoString.toMappedCompressedString();
    }

    @Override
    public final boolean areEqual(@NotNull final CharSequence key, @NotNull final MappedCompressedString value) {
        return CharSequenceUtils.contentEquals(key, value);
    }
}
