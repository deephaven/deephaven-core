package io.deephaven.qst.array;

import io.deephaven.qst.type.LongType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public final class LongArray extends PrimitiveArrayBase<Long> {

    public static LongArray empty() {
        return new LongArray(new long[0]);
    }

    public static LongArray of(long... values) {
        return builder(values.length).add(values).build();
    }

    public static LongArray of(Long... values) {
        return builder(values.length).add(values).build();
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

    public static class Builder extends PrimitiveArrayHelper<long[]>
        implements ArrayBuilder<Long, LongArray, Builder> {

        private Builder(int initialCapacity) {
            super(initialCapacity, long.class);
        }

        public final Builder add(long item) {
            ensureCapacity();
            array[size++] = item;
            return this;
        }

        public final Builder add(long... items) {
            addImpl(items);
            return this;
        }

        @Override
        public final Builder add(Long item) {
            return add(Util.adapt(item));
        }

        @Override
        public final Builder add(Long... items) {
            for (Long item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public final Builder add(Iterable<Long> items) {
            for (Long item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public final LongArray build() {
            return new LongArray(takeAtSize());
        }
    }
}
