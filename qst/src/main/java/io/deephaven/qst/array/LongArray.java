package io.deephaven.qst.array;

import io.deephaven.qst.type.LongType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public final class LongArray extends PrimitiveArrayBase<Long> {
    static final long NULL_REPR = Long.MIN_VALUE;

    public static LongArray empty() {
        return new LongArray(new long[0]);
    }

    public static LongArray of(long... values) {
        return builder(values.length).add(values).build();
    }

    public static LongArray of(Long... values) {
        return builder(values.length).add(values).build();
    }

    public static LongArray of(Iterable<Long> values) {
        if (values instanceof Collection) {
            return of((Collection<Long>) values);
        }
        return builder(16).add(values).build();
    }

    public static LongArray of(Collection<Long> values) {
        return builder(values.size()).add(values).build();
    }

    public static LongArray ofUnsafe(long... values) {
        return new LongArray(values);
    }

    public static Builder builder(int initialSize) {
        return new Builder(initialSize);
    }

    private static long adapt(Long x) {
        return x == null ? NULL_REPR : x;
    }

    private static Long adapt(long x) {
        return x == NULL_REPR ? null : x;
    }

    private final long[] values;

    private LongArray(long[] values) {
        this.values = Objects.requireNonNull(values);
    }

    /**
     * The raw longs. Must not be modified.
     *
     * @return the longs, do <b>not</b> modify
     */
    public final long[] values() {
        return values;
    }

    @Override
    public final Long get(int index) {
        return adapt(values[index]);
    }

    @Override
    public final int size() {
        return values().length;
    }

    @Override
    public final LongType type() {
        return LongType.instance();
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
        LongArray longArray = (LongArray) o;
        return Arrays.equals(values, longArray.values);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(values);
    }

    public static class Builder implements ArrayBuilder<Long, LongArray, Builder> {

        private long[] array;
        private int size;

        private Builder(int initialCapacity) {
            this.array = new long[initialCapacity];
            this.size = 0;
        }

        public synchronized final Builder add(long item) {
            ensureCapacity();
            array[size++] = item;
            return this;
        }

        public synchronized final Builder add(long... items) {
            // todo: systemcopy
            for (long item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final Builder add(Long item) {
            return add(adapt(item));
        }

        @Override
        public synchronized final Builder add(Long... items) {
            for (Long item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final Builder add(Iterable<Long> items) {
            for (Long item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final LongArray build() {
            return new LongArray(takeAtSize());
        }

        private void ensureCapacity() {
            if (size == array.length) {
                long[] next = new long[array.length == 0 ? 1 : array.length * 2];
                System.arraycopy(array, 0, next, 0, array.length);
                array = next;
            }
        }

        private long[] takeAtSize() {
            if (size == array.length) {
                return array; // great case, no copying necessary :)
            }
            long[] atSize = new long[size];
            System.arraycopy(array, 0, atSize, 0, size);
            return atSize;
        }
    }
}
