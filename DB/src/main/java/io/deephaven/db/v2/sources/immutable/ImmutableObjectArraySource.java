package io.deephaven.db.v2.sources.immutable;

import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ImmutableObjectArraySource<T> extends AbstractColumnSource<T> implements ImmutableColumnSourceGetDefaults.ForObject<T> {

    private final Object[] data;

    public ImmutableObjectArraySource(@NotNull final Object[] source, @NotNull final Class<T> dataType) {
        this(source, dataType, null);
    }

    public ImmutableObjectArraySource(@NotNull final Object[] source, @NotNull final Class<T> dataType, @Nullable final Class<?> componentType) {
        super(dataType, componentType);
        this.data = source;
    }

    @Override
    public T get(long index) {
        if (index < 0 || index >= data.length) {
            return null;
        }

        //noinspection unchecked
        return (T)data[(int)index];
    }

    @Override
    public boolean isImmutable() {
        return true;
    }
}
