package io.deephaven.qst.array;

import io.deephaven.qst.type.ShortType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public final class ShortArray extends PrimitiveArrayBase<Short> {
    static final short NULL_REPR = Short.MIN_VALUE;

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
        return builder(16).add(values).build();
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

    private static short adapt(Short x) {
        return x == null ? NULL_REPR : x;
    }

    private static Short adapt(short x) {
        return x == NULL_REPR ? null : x;
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
    public final Short get(int index) {
        return adapt(values[index]);
    }

    @Override
    public final int size() {
        return values().length;
    }

    @Override
    public final ShortType type() {
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

    public static class Builder implements ArrayBuilder<Short, ShortArray, Builder> {

        private short[] array;
        private int size;

        private Builder(int initialCapacity) {
            this.array = new short[initialCapacity];
            this.size = 0;
        }

        public synchronized final Builder add(short item) {
            ensureCapacity();
            array[size++] = item;
            return this;
        }

        public synchronized final Builder add(short... items) {
            // todo: systemcopy
            for (short item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final Builder add(Short item) {
            return add(adapt(item));
        }

        @Override
        public synchronized final Builder add(Short... items) {
            for (Short item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final Builder add(Iterable<Short> items) {
            for (Short item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final ShortArray build() {
            return new ShortArray(takeAtSize());
        }

        private void ensureCapacity() {
            if (size == array.length) {
                short[] next = new short[array.length == 0 ? 1 : array.length * 2];
                System.arraycopy(array, 0, next, 0, array.length);
                array = next;
            }
        }

        private short[] takeAtSize() {
            if (size == array.length) {
                return array; // great case, no copying necessary :)
            }
            short[] atSize = new short[size];
            System.arraycopy(array, 0, atSize, 0, size);
            return atSize;
        }
    }
}
