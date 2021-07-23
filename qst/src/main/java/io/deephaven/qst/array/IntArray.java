package io.deephaven.qst.array;

import io.deephaven.qst.type.IntType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public final class IntArray extends PrimitiveArrayBase<Integer> {

    public static IntArray empty() {
        return new IntArray(new int[0]);
    }

    public static IntArray of(int... values) {
        return builder(values.length).add(values).build();
    }

    public static IntArray of(Integer... values) {
        return builder(values.length).add(values).build();
    }

    public static IntArray of(Collection<Integer> values) {
        return builder(values.size()).add(values).build();
    }

    public static IntArray ofUnsafe(int... values) {
        return new IntArray(values);
    }

    public static Builder builder(int initialSize) {
        return new Builder(initialSize);
    }

    private static int adapt(Integer x) {
        return x == null ? Util.NULL_INT : x;
    }

    private static Integer adapt(int x) {
        return x == Util.NULL_INT ? null : x;
    }

    private final int[] values;

    private IntArray(int[] values) {
        this.values = Objects.requireNonNull(values);
    }

    /**
     * The raw ints. Must not be modified.
     *
     * @return the ints, do <b>not</b> modify
     */
    public final int[] values() {
        return values;
    }

    @Override
    public final int size() {
        return values().length;
    }

    @Override
    public final IntType type() {
        return IntType.instance();
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
        IntArray intArray = (IntArray) o;
        return Arrays.equals(values, intArray.values);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(values);
    }

    public static class Builder extends PrimitiveArrayHelper<int[]>
        implements ArrayBuilder<Integer, IntArray, Builder> {

        private Builder(int initialCapacity) {
            super(initialCapacity, int.class);
        }

        public final Builder add(int item) {
            ensureCapacity();
            array[size++] = item;
            return this;
        }

        public final Builder add(int... items) {
            addImpl(items);
            return this;
        }

        @Override
        public final Builder add(Integer item) {
            return add(adapt(item));
        }

        @Override
        public final Builder add(Integer... items) {
            for (Integer item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public final Builder add(Iterable<Integer> items) {
            for (Integer item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public final IntArray build() {
            return new IntArray(takeAtSize());
        }
    }
}
