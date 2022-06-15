/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.datastructures.hash;

import io.deephaven.hash.KeyedObjectKey;

/**
 * Base key implementation for objects that should be hashed and compared by the identity of their key.
 */
public abstract class KeyIdentityKeyedObjectKey<K, V> implements KeyedObjectKey<K, V> {

    protected KeyIdentityKeyedObjectKey() {}

    @Override
    public final int hashKey(final K key) {
        return System.identityHashCode(key);
    }

    @Override
    public final boolean equalKey(final K key, final V value) {
        return key == getKey(value);
    }
}
