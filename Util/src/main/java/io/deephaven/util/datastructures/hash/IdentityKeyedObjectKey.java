package io.deephaven.util.datastructures.hash;

import io.deephaven.hash.KeyedObjectKey;

/**
 * Key implementation for objects keyed by their own identity.
 */
public final class IdentityKeyedObjectKey<KV> implements KeyedObjectKey<KV, KV> {

    private static final KeyedObjectKey<?, ?> INSTANCE = new IdentityKeyedObjectKey<>();

    public static <KV> KeyedObjectKey<KV, KV> getInstance() {
        // noinspection unchecked
        return (KeyedObjectKey<KV, KV>) INSTANCE;
    }

    private IdentityKeyedObjectKey() {}

    @Override
    public KV getKey(final KV kv) {
        return kv;
    }

    @Override
    public int hashKey(final KV kv) {
        return System.identityHashCode(kv);
    }

    @Override
    public boolean equalKey(final KV kv1, final KV kv2) {
        return kv1 == kv2;
    }
}
