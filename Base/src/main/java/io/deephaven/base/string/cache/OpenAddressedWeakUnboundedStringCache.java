/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import io.deephaven.base.cache.OpenAddressedCanonicalizationCache;
import org.jetbrains.annotations.NotNull;

/**
 * Unbounded StringCache built around a OpenAddressedCanonicalizationCache, which only enforces weak reachability on its
 * STRING_LIKE_TYPE members.
 */
public class OpenAddressedWeakUnboundedStringCache<STRING_LIKE_TYPE extends CharSequence>
        implements StringCache<STRING_LIKE_TYPE> {

    /**
     * Adapter to make and compare cache members.
     */
    private final StringCacheTypeAdapter<STRING_LIKE_TYPE> typeAdapter;

    /**
     * The cache itself.
     */
    private final OpenAddressedCanonicalizationCache cache;

    /**
     * Canonicalization cache adapter implementation for String inputs.
     */
    private class StringCanonicalizationCacheAdapter
            implements OpenAddressedCanonicalizationCache.Adapter<String, STRING_LIKE_TYPE> {

        @Override
        public boolean equals(@NotNull String inputItem, @NotNull Object cachedItem) {
            // noinspection unchecked
            return typeAdapter.getType() == cachedItem.getClass()
                    && typeAdapter.areEqual(inputItem, (STRING_LIKE_TYPE) cachedItem);
        }

        @Override
        public int hashCode(@NotNull String inputItem) {
            return inputItem.hashCode();
        }

        @Override
        public STRING_LIKE_TYPE makeCacheableItem(@NotNull String inputItem) {
            return typeAdapter.create(inputItem);
        }
    }

    /**
     * Canonicalization cache adapter for String inputs.
     */
    private final OpenAddressedCanonicalizationCache.Adapter<String, STRING_LIKE_TYPE> stringC14nAdapter;

    /**
     * Canonicalization cache adapter implementation for StringCompatible inputs.
     */
    private class StringCompatibleCanonicalizationCacheAdapter
            implements OpenAddressedCanonicalizationCache.Adapter<StringCompatible, STRING_LIKE_TYPE> {

        @Override
        public boolean equals(@NotNull StringCompatible inputItem, @NotNull Object cachedItem) {
            // noinspection unchecked
            return typeAdapter.getType() == cachedItem.getClass()
                    && typeAdapter.areEqual(inputItem, (STRING_LIKE_TYPE) cachedItem);
        }

        @Override
        public int hashCode(@NotNull StringCompatible inputItem) {
            return inputItem.hashCode();
        }

        @Override
        public STRING_LIKE_TYPE makeCacheableItem(@NotNull StringCompatible inputItem) {
            return typeAdapter.create(inputItem);
        }
    }

    /**
     * Canonicalization cache adapter for StringCompatible inputs.
     */
    private final OpenAddressedCanonicalizationCache.Adapter<StringCompatible, STRING_LIKE_TYPE> stringCompatibleC14nAdapter;

    /**
     * @param typeAdapter The type adapter for this cache
     * @param initialCapacity Initial capacity of the map backing this cache
     */
    @SuppressWarnings("unused")
    public OpenAddressedWeakUnboundedStringCache(@NotNull final StringCacheTypeAdapter<STRING_LIKE_TYPE> typeAdapter,
            final int initialCapacity) {
        this(typeAdapter, new OpenAddressedCanonicalizationCache(initialCapacity));
    }

    /**
     * @param typeAdapter The type adapter for this String cache
     * @param cache The internal canonicalization cache
     */
    public OpenAddressedWeakUnboundedStringCache(@NotNull final StringCacheTypeAdapter<STRING_LIKE_TYPE> typeAdapter,
            @NotNull final OpenAddressedCanonicalizationCache cache) {
        this.typeAdapter = typeAdapter;
        this.cache = cache;
        stringC14nAdapter = new StringCanonicalizationCacheAdapter();
        stringCompatibleC14nAdapter = new StringCompatibleCanonicalizationCacheAdapter();
    }

    @Override
    public final int capacity() {
        return CAPACITY_UNBOUNDED;
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
        return cache.getCachedItem(protoString, stringCompatibleC14nAdapter);
    }

    @Override
    @NotNull
    public final STRING_LIKE_TYPE getCachedString(@NotNull final String string) {
        return cache.getCachedItem(string, stringC14nAdapter);
    }
}
