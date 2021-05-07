package io.deephaven.db.v2.sources.immutable;

import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;

public class ImmutableObjectArraySource<T> extends AbstractColumnSource<T> implements ImmutableColumnSourceGetDefaults.ForObject<T> {
    private final Object[] data;

    public ImmutableObjectArraySource(Object[] source, Class<T> type) {
        super(type);
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
