package io.deephaven.qst.array;

import io.deephaven.qst.type.BooleanType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public final class BooleanArray extends PrimitiveArrayBase<Boolean> {

    public static BooleanArray empty() {
        return new BooleanArray(new byte[0]);
    }

    public static BooleanArray of(boolean... values) {
        return builder(values.length).add(values).build();
    }

    public static BooleanArray of(Boolean... values) {
        return builder(values.length).add(values).build();
    }

    public static BooleanArray of(Collection<Boolean> values) {
        return builder(values.size()).add(values).build();
    }

    public static BooleanArray ofUnsafe(byte... values) {
        return new BooleanArray(values);
    }

    public static Builder builder(int initialSize) {
        return new Builder(initialSize);
    }

    // todo: use bitset?
    private final byte[] values;

    private BooleanArray(byte[] values) {
        this.values = Objects.requireNonNull(values);
    }

    /**
     * The raw booleans, as bytes. Must not be modified.
     *
     * @return the booleans, as bytes. do <b>not</b> modify
     */
    public final byte[] values() {
        return values;
    }

    @Override
    public final int size() {
        return values().length;
    }

    @Override
    public final BooleanType type() {
        return BooleanType.instance();
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
        BooleanArray that = (BooleanArray) o;
        return Arrays.equals(values, that.values);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(values);
    }

    public static class Builder implements ArrayBuilder<Boolean, BooleanArray, Builder> {

        private byte[] array;
        private int size;

        private Builder(int initialCapacity) {
            this.array = new byte[initialCapacity];
            this.size = 0;
        }

        public final Builder add(boolean item) {
            ensureCapacity();
            array[size++] = Util.adapt(item);
            return this;
        }

        public final Builder add(boolean... items) {
            for (boolean item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public final Builder add(Boolean item) {
            ensureCapacity();
            array[size++] = Util.adapt(item);
            return this;
        }

        @Override
        public final Builder add(Boolean... items) {
            for (Boolean item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public final Builder add(Iterable<Boolean> items) {
            for (Boolean item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public final BooleanArray build() {
            return new BooleanArray(takeAtSize());
        }

        private void ensureCapacity() {
            if (size == array.length) {
                byte[] next = new byte[array.length == 0 ? 1 : array.length * 2];
                System.arraycopy(array, 0, next, 0, array.length);
                array = next;
            }
        }

        private byte[] takeAtSize() {
            if (size == array.length) {
                return array; // great case, no copying necessary :)
            }
            byte[] atSize = new byte[size];
            System.arraycopy(array, 0, atSize, 0, size);
            return atSize;
        }
    }
}
