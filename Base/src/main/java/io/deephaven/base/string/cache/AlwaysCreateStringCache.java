/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

/**
 * Dummy StringCache implementation for code that won't benefit from caching, just wants to use the StringCompatible
 * mechanisms for efficient String creation.
 */
public class AlwaysCreateStringCache<STRING_LIKE_TYPE extends CharSequence> implements StringCache<STRING_LIKE_TYPE> {

    public static final StringCache<String> STRING_INSTANCE =
            new AlwaysCreateStringCache<>(StringCacheTypeAdapterStringImpl.INSTANCE);
    public static final StringCache<CompressedString> COMPRESSED_STRING_INSTANCE =
            new AlwaysCreateStringCache<>(StringCacheTypeAdapterCompressedStringImpl.INSTANCE);
    public static final StringCache<MappedCompressedString> MAPPED_COMPRESSED_STRING_INSTANCE =
            new AlwaysCreateStringCache<>(StringCacheTypeAdapterMappedCompressedStringImpl.INSTANCE);

    /**
     * Adapter to make and compare cache members.
     */
    private final StringCacheTypeAdapter<STRING_LIKE_TYPE> typeAdapter;

    private AlwaysCreateStringCache(final StringCacheTypeAdapter<STRING_LIKE_TYPE> typeAdapter) {
        this.typeAdapter = Require.neqNull(typeAdapter, "typeAdapter");
    }

    @Override
    public final int capacity() {
        return 0;
    }

    @Override
    @NotNull
    public final Class<STRING_LIKE_TYPE> getType() {
        return typeAdapter.getType();
    }

    @Override
    @NotNull
    public final STRING_LIKE_TYPE getEmptyString() {
        return typeAdapter.empty();
    }

    @Override
    @NotNull
    public final STRING_LIKE_TYPE getCachedString(@NotNull final StringCompatible protoString) {
        return typeAdapter.create(protoString);
    }

    @Override
    @NotNull
    public final STRING_LIKE_TYPE getCachedString(@NotNull final String string) {
        return typeAdapter.create(string);
    }
}
