package io.deephaven.qst.array;

import io.deephaven.qst.type.ShortType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * A {@link ShortType} array.
 */
public final class ShortArray extends PrimitiveArrayBase<Short> {

    public static ShortArray empty() {
        return new ShortArray(new short[0]);
    }

    public static ShortArray of(short... values) {
        return builder(values.length).add(values).build();
    }

    public static ShortArray of(Short... values) {
        return builder(values.length).add(values).build();
    }

    public static ShortArray of(Iterable<Short> values) {
        if (values instanceof Collection) {
            return of((Collection<Short>) values);
        }
        return builder(Util.DEFAULT_BUILDER_INITIAL_CAPACITY).add(values).build();
    }

    public static ShortArray of(Collection<Short> values) {
        return builder(values.size()).add(values).build();
    }

    public static ShortArray ofUnsafe(short... values) {
        return new ShortArray(values);
    }

    public static Builder builder(int initialSize) {
        return new Builder(initialSize);
    }

    private final short[] values;

    private ShortArray(short[] values) {
        this.values = Objects.requireNonNull(values);
    }

    /**
     * The raw shorts. Must not be modified.
     *
     * @return the shorts, do <b>not</b> modify
     */
    public final short[] values() {
        return values;
    }

    @Override
    public final int size() {
        return values().length;
    }

    @Override
    public final ShortType componentType() {
        return ShortType.instance();
    }

    @Override
    public final <V extends PrimitiveArray.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ShortArray that = (ShortArray) o;
        return Arrays.equals(values, that.values);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(values);
    }

    public static class Builder extends PrimitiveArrayHelper<short[]>
            implements ArrayBuilder<Short, ShortArray, Builder> {
        private Builder(int initialCapacity) {
            super(new short[initialCapacity]);
        }

        public final Builder add(short item) {
            ensureCapacity();
            array[size++] = item;
            return this;
        }

        @Override
        int length(short[] array) {
            return array.length;
        }

        @Override
        void arraycopy(short[] src, int srcPos, short[] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        short[] construct(int size) {
            return new short[size];
        }

        public final Builder add(short... items) {
            addImpl(items);
            return this;
        }

        @Override
        public final Builder add(Short item) {
            return add(Util.adapt(item));
        }

        private void addInternal(Short item) {
            array[size++] = Util.adapt(item);
        }

        @Override
        public final Builder add(Short... items) {
            ensureCapacity(items.length);
            for (Short item : items) {
                addInternal(item);
            }
            return this;
        }

        @Override
        public final Builder add(Iterable<Short> items) {
            for (Short item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public final ShortArray build() {
            return new ShortArray(takeAtSize());
        }
    }
}
