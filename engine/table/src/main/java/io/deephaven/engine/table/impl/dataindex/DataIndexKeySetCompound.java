//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.base.verify.Require;
import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.function.Consumer;

class DataIndexKeySetCompound implements DataIndexKeySet {

    private static final KeyedObjectKey<Object, Object> KEY_HASH_ADAPTER = new KeyHashAdapter();

    private static class KeyHashAdapter implements KeyedObjectKey<Object, Object> {

        private KeyHashAdapter() {}

        @Override
        public Object getKey(@NotNull final Object value) {
            return value;
        }

        @Override
        public int hashKey(@Nullable final Object key) {
            return Arrays.hashCode((Object[]) key);
        }

        @Override
        public boolean equalKey(@Nullable final Object key, @NotNull final Object value) {
            return Arrays.equals((Object[]) key, (Object[]) value);
        }
    }

    private final KeyedObjectHashSet<Object, Object> set;

    DataIndexKeySetCompound(final int initialCapacity) {
        set = new KeyedObjectHashSet<>(Require.gtZero(initialCapacity, "initialCapacity"), KEY_HASH_ADAPTER);
    }

    DataIndexKeySetCompound() {
        set = new KeyedObjectHashSet<>(KEY_HASH_ADAPTER);
    }

    @Override
    public boolean add(final Object key) {
        return set.add(key);
    }

    @Override
    public boolean remove(final Object key) {
        return set.removeValue(key);
    }

    @Override
    public boolean contains(final Object key) {
        return set.containsKey(key);
    }

    @Override
    public void forEach(@NotNull final Consumer<? super Object> action) {
        set.forEach(action);
    }

    @Override
    public Object[] toArray() {
        return set.toArray();
    }
}
