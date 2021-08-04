package io.deephaven.qst.type;

public abstract class ColumnTypeBase<T> implements Type<T> {

    @Override
    public final T castValue(Object value) {
        // noinspection unchecked
        return (T) value;
    }
}
