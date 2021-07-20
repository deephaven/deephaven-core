package io.deephaven.qst.array;

import io.deephaven.qst.type.IntType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public final class IntArray extends PrimitiveArrayBase<Integer> {
    static final int NULL_REPR = Integer.MIN_VALUE;

    public static IntArray empty() {
        return new IntArray(new int[0]);
    }

    public static IntArray of(int... values) {
        return builder(values.length).add(values).build();
    }

    public static IntArray of(Integer... values) {
        return builder(values.length).add(values).build();
    }

    public static IntArray of(Iterable<Integer> values) {
        if (values instanceof Collection) {
            return of((Collection<Integer>) values);
        }
        return builder(16).add(values).build();
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
        return x == null ? NULL_REPR : x;
    }

    private static Integer adapt(int x) {
        return x == NULL_REPR ? null : x;
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
    public final Integer get(int index) {
        return adapt(values[index]);
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

    public static class Builder implements ArrayBuilder<Integer, IntArray, Builder> {

        private int[] array;
        private int size;

        private Builder(int initialCapacity) {
            this.array = new int[initialCapacity];
            this.size = 0;
        }

        public synchronized final Builder add(int item) {
            ensureCapacity();
            array[size++] = item;
            return this;
        }

        public synchronized final Builder add(int... items) {
            // todo: systemcopy
            for (int item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final Builder add(Integer item) {
            return add(adapt(item));
        }

        @Override
        public synchronized final Builder add(Integer... items) {
            for (Integer item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final Builder add(Iterable<Integer> items) {
            for (Integer item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final IntArray build() {
            return new IntArray(takeAtSize());
        }

        private void ensureCapacity() {
            if (size == array.length) {
                int[] next = new int[array.length == 0 ? 1 : array.length * 2];
                System.arraycopy(array, 0, next, 0, array.length);
                array = next;
            }
        }

        private int[] takeAtSize() {
            if (size == array.length) {
                return array; // great case, no copying necessary :)
            }
            int[] atSize = new int[size];
            System.arraycopy(array, 0, atSize, 0, size);
            return atSize;
        }
    }
}
