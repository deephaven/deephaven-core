//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.base.verify.Require;
import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

class DataIndexKeySetSingle implements DataIndexKeySet {

    private static final KeyedObjectKey<Object, Object> KEY_HASH_ADAPTER = new KeyHashAdapter();

    private static class KeyHashAdapter implements KeyedObjectKey<Object, Object> {

        private KeyHashAdapter() {}

        @Override
        public Object getKey(@NotNull final Object value) {
            return value;
        }

        @Override
        public int hashKey(@NotNull final Object key) {
            return key.hashCode();
        }

        @Override
        public boolean equalKey(@NotNull final Object key, @NotNull final Object value) {
            return key.equals(value);
        }
    }

    private static final Object NULL_OBJECT_KEY = new Object();

    private final KeyedObjectHashSet<Object, Object> set;

    DataIndexKeySetSingle(final int initialCapacity) {
        set = new KeyedObjectHashSet<>(Require.gtZero(initialCapacity, "initialCapacity"), KEY_HASH_ADAPTER);
    }

    DataIndexKeySetSingle() {
        set = new KeyedObjectHashSet<>(KEY_HASH_ADAPTER);
    }

    @Override
    public boolean add(final Object key) {
        return set.add(adaptKey(key));
    }

    @Override
    public boolean remove(final Object key) {
        return set.remove(adaptKey(key));
    }

    @Override
    public boolean contains(final Object key) {
        return set.containsKey(adaptKey(key));
    }

    @NotNull
    private static Object adaptKey(@Nullable final Object key) {
        return key == null ? NULL_OBJECT_KEY : key;
    }

    @Override
    public void forEach(@NotNull final Consumer<? super Object> action) {
        set.forEach(key -> action.accept(key == NULL_OBJECT_KEY ? null : key));
    }

    @Override
    public Object[] toArray() {
        final Object[] result = set.toArray();
        if (set.containsKey(NULL_OBJECT_KEY)) {
            for (int ii = 0; ii < result.length; ii++) {
                if (result[ii] == NULL_OBJECT_KEY) {
                    result[ii] = null;
                    break;
                }
            }
        }
        return result;
    }
}
