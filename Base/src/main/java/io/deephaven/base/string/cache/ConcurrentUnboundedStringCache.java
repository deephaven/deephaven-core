/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import io.deephaven.hash.KeyedObjectHash;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

/**
 * A very limited interface is specified, in order to decouple typeAdapter pooling and related
 * concerns from the cache itself.
 *
 * StringCompatibles or Strings used as keys (or values) when probing/populating the cache are
 * allowed to use their own hashCode() implementation. This is dangerous, because we rely on our key
 * implementation to hash CharSequences identically to a String of the same characters. An assertion
 * in the value factory should catch any cases where the built-in assumption breaks down, but we've
 * deemed that unnecessary at this time. Specify "debug" in the constructor if you need this check.
 * String.hashCode()'s implementation has been stable since JDK 1.2, and is specified in the
 * JavaDocs.
 *
 * This implementation is thread-safe, and lock-free except for the insertion of new cached Strings
 * on a cache miss. StringCompatible implementation thread-safety is a separate concern.
 */
public class ConcurrentUnboundedStringCache<STRING_LIKE_TYPE extends CharSequence>
    implements StringCache<STRING_LIKE_TYPE> {

    /**
     * Adapter to make and compare cache members.
     */
    private final StringCacheTypeAdapter<STRING_LIKE_TYPE> typeAdapter;

    /**
     * The cache itself.
     */
    private final KeyedObjectHashMap<CharSequence, STRING_LIKE_TYPE> cache;

    /**
     * ValueFactory for StringCompatible->STRING_LIKE_TYPE.
     */
    private final KeyedObjectHash.ValueFactory<CharSequence, STRING_LIKE_TYPE> stringCompatibleKeyValueFactory;

    /**
     * ValueFactory for String->STRING_LIKE_TYPE.
     */
    private final KeyedObjectHash.ValueFactory<CharSequence, STRING_LIKE_TYPE> stringKeyValueFactory;


    /**
     * @param typeAdapter The type adapter for this cache
     * @param initialCapacity Initial capacity of the map backing this cache
     * @param debug Whether constructed Strings should be checked for consistency against the
     *        StringCompatible used
     */
    public ConcurrentUnboundedStringCache(
        @NotNull final StringCacheTypeAdapter<STRING_LIKE_TYPE> typeAdapter,
        final int initialCapacity, final boolean debug) {
        this.typeAdapter = Require.neqNull(typeAdapter, "typeAdapter");
        cache = new KeyedObjectHashMap<>(initialCapacity, new KeyImpl());
        stringCompatibleKeyValueFactory = debug ? new CheckedStringCompatibleKeyValueFactory()
            : new UncheckedStringCompatibleKeyValueFactory();
        stringKeyValueFactory = new StringKeyValueFactory();
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
        // There's an inherent trade-off between the length of time we hold the cache's lock and the
        // possibility of
        // wasting constructed Strings due to optimistic construction on cache miss.
        // For now, this implementation is optimistic. Switch to the following implementation (one
        // line) if production
        // performance shows this to be a poor choice:
        // return cache.putIfAbsent(protoString, stringCompatibleKeyValueFactory);
        STRING_LIKE_TYPE existingValue = cache.get(protoString);
        if (existingValue != null) {
            return existingValue;
        }
        final STRING_LIKE_TYPE candidateValue =
            stringCompatibleKeyValueFactory.newValue(protoString);
        existingValue = cache.putIfAbsent(candidateValue, candidateValue);
        if (existingValue != null) {
            return existingValue;
        }
        return candidateValue;
    }

    @Override
    @NotNull
    public final STRING_LIKE_TYPE getCachedString(@NotNull final String string) {
        return cache.putIfAbsent(string, stringKeyValueFactory);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // KeyedObjectKey<CharSequence, String> implementation
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

    // -----------------------------------------------------------------------------------------------------------------
    // KeyedObjectHash.ValueFactory<CharSequence, String> implementations
    // -----------------------------------------------------------------------------------------------------------------

    private class UncheckedStringCompatibleKeyValueFactory
        implements KeyedObjectHash.ValueFactory<CharSequence, STRING_LIKE_TYPE> {

        @Override
        public STRING_LIKE_TYPE newValue(CharSequence key) {
            return typeAdapter.create((StringCompatible) key);
        }
    }

    private class CheckedStringCompatibleKeyValueFactory
        implements KeyedObjectHash.ValueFactory<CharSequence, STRING_LIKE_TYPE> {

        @Override
        public STRING_LIKE_TYPE newValue(final CharSequence key) {
            final STRING_LIKE_TYPE value = typeAdapter.create((StringCompatible) key);
            Assert.assertion(CharSequenceUtils.contentEquals(key, value),
                "CharSequenceUtils.contentEquals(key, value)", key, "key", value, "value");
            Assert.eq(key.hashCode(), "key.hashCode", value.hashCode(), "value.hashCode()");
            return value;
        }
    }

    private class StringKeyValueFactory
        implements KeyedObjectHash.ValueFactory<CharSequence, STRING_LIKE_TYPE> {

        @Override
        public STRING_LIKE_TYPE newValue(final CharSequence key) {
            return typeAdapter.create((String) key);
        }
    }
}
