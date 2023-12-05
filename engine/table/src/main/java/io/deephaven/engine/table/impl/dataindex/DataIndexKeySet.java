package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

public class DataIndexKeySet {
    private static final KeyedObjectKey<Object, Object> INSTANCE = new KeyHashAdapter();
    private static final Object NULL_OBJECT_KEY = new Object();

    private final KeyedObjectHashSet<Object, Object> set;

    private static class KeyHashAdapter implements KeyedObjectKey<Object, Object> {
        private KeyHashAdapter() {}

        @Override
        public Object getKey(@NotNull final Object value) {
            return value;
        }

        @Override
        public int hashKey(@Nullable final Object key) {
            if (key instanceof Object[]) {
                return Arrays.hashCode((Object[]) key);
            }
            return Objects.hashCode(key);
        }

        @Override
        public boolean equalKey(@Nullable final Object key, @NotNull final Object value) {
            if (key instanceof Object[] && value instanceof Object[]) {
                return Arrays.equals((Object[]) key, (Object[]) value);
            }
            return Objects.equals(key, value);
        }
    }



    public DataIndexKeySet(final int initialCapacity) {
        set = new KeyedObjectHashSet<>(initialCapacity, INSTANCE);
    }

    public DataIndexKeySet() {
        set = new KeyedObjectHashSet<>(INSTANCE);
    }

    public boolean remove(Object o) {
        if (o == null) {
            return set.remove(NULL_OBJECT_KEY);
        }
        return set.removeValue(o);
    }

    public boolean add(Object o) {
        if (o == null) {
            return set.add(NULL_OBJECT_KEY);
        }
        return set.add(o);
    }

    public boolean contains(Object o) {
        if (o == null) {
            return set.contains(NULL_OBJECT_KEY);
        }
        return set.contains(o);
    }

    public void forEach(Consumer<? super Object> action) {
        set.forEach(key -> {
            if (key == NULL_OBJECT_KEY) {
                action.accept(null);
            } else {
                action.accept(key);
            }
        });
    }

    public Object[] toArray() {
        final Object[] result = set.toArray();
        for (int ii = 0; ii < result.length; ii++) {
            if (result[ii] == NULL_OBJECT_KEY) {
                result[ii] = null;
            }
        }
        return result;
    }

}
