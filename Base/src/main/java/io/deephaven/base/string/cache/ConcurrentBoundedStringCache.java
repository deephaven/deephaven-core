/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import io.deephaven.base.cache.KeyedObjectCache;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.util.Random;

/**
 * This cache follows the same design as ConcurrentUnboundedStringCache, but uses a KeyedObjectCache
 * (bounded, concurrent-get, "pseudo-random pseudo-LRU" replacement) for its internal storage.
 *
 * This implementation is thread-safe, and lock-free except for the insertion of new cached Strings
 * on a cache miss.
 */
public class ConcurrentBoundedStringCache<STRING_LIKE_TYPE extends CharSequence>
    implements StringCache<STRING_LIKE_TYPE> {

    /**
     * Adapter to make and compare cache members.
     */
    private final StringCacheTypeAdapter<STRING_LIKE_TYPE> typeAdapter;

    /**
     * The cache itself.
     */
    private final KeyedObjectCache<CharSequence, STRING_LIKE_TYPE> cache;

    /**
     * @param typeAdapter Adapter to make and compare cache members.
     * @param capacity Minimum capacity of the storage backing this cache.
     * @param collisionFactor Number of possible storage slots a given element might be stored in.
     */
    public ConcurrentBoundedStringCache(final StringCacheTypeAdapter<STRING_LIKE_TYPE> typeAdapter,
        final int capacity, final int collisionFactor) {
        this.typeAdapter = Require.neqNull(typeAdapter, "typeAdapter");
        cache =
            new KeyedObjectCache<>(capacity, collisionFactor, new KeyImpl(), null, new Random());
    }

    @Override
    public final int capacity() {
        return cache.getCapacity();
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
        final STRING_LIKE_TYPE existingValue = cache.get(protoString);
        return existingValue != null ? existingValue
            : cache.putIfAbsent(typeAdapter.create(protoString));
    }

    @Override
    @NotNull
    public final STRING_LIKE_TYPE getCachedString(@NotNull final String string) {
        final STRING_LIKE_TYPE existingValue = cache.get(string);
        return existingValue != null ? existingValue
            : cache.putIfAbsent(typeAdapter.create(string));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // KeyedObjectKey<CharSequence, SimpleReference<String>> implementation
    // -----------------------------------------------------------------------------------------------------------------

    private class KeyImpl implements KeyedObjectKey<CharSequence, STRING_LIKE_TYPE> {

        @Override
        public CharSequence getKey(STRING_LIKE_TYPE value) {
            return value;
        }

        @Override
        public int hashKey(CharSequence key) {
            return key.hashCode();
        }

        @Override
        public boolean equalKey(CharSequence key, STRING_LIKE_TYPE value) {
            return typeAdapter.areEqual(key, value);
        }
    }
}
