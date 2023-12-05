package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * {@link KeyedObjectKey} implementation for {@link io.deephaven.engine.table.DataIndex} keys when boxed for hash-based
 * lookup.
 */
public class DataIndexBoxedKeyHashAdapter implements KeyedObjectKey<Object, Object> {

    public static final KeyedObjectKey<Object, Object> INSTANCE = new DataIndexBoxedKeyHashAdapter();

    private DataIndexBoxedKeyHashAdapter() {}

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
