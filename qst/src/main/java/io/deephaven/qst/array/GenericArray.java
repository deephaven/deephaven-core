package io.deephaven.qst.array;

import io.deephaven.qst.AllowNulls;
import io.deephaven.qst.type.GenericType;
import org.immutables.value.Value;

import java.util.List;

/**
 * An array-like object for non-primitive types.
 *
 * @param <T> the non-primitive type
 */
@Value.Immutable
public abstract class GenericArray<T> extends ArrayBase<T> {

    public static <T> Builder<T> builder() {
        return ImmutableGenericArray.builder();
    }

    public static <T> Builder<T> builder(GenericType<T> type) {
        return ImmutableGenericArray.<T>builder().type(type);
    }

    public static <T> GenericArray<T> empty(GenericType<T> type) {
        return builder(type).build();
    }

    public static <T> GenericArray<T> of(GenericType<T> type, T... data) {
        return builder(type).add(data).build();
    }

    public static <T> GenericArray<T> of(GenericType<T> type, Iterable<T> data) {
        return builder(type).add(data).build();
    }

    public abstract GenericType<T> type();

    @AllowNulls
    public abstract List<T> values();

    @Override
    public final int size() {
        return values().size();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final T get(int index) {
        return values().get(index);
    }

    public final <O> GenericArray<O> cast(GenericType<O> type) {
        if (!type().equals(type)) {
            throw new IllegalArgumentException(
                String.format("Can't cast GenericArray with type %s to %s", type(), type));
        }
        // noinspection unchecked
        return (GenericArray<O>) this;
    }

    public abstract static class Builder<T>
        implements ArrayBuilder<T, GenericArray<T>, Builder<T>> {

        public abstract Builder<T> addValues(T item);

        public abstract Builder<T> addValues(T... item);

        public abstract Builder<T> addAllValues(Iterable<? extends T> elements);

        public abstract Builder<T> type(GenericType<T> type);

        @Override
        public abstract GenericArray<T> build();

        @Override
        public final Builder<T> add(T item) {
            return addValues(item);
        }

        @Override
        public final Builder<T> add(T... items) {
            return addValues(items);
        }

        @Override
        public final Builder<T> add(Iterable<T> items) {
            return addAllValues(items);
        }
    }
}
