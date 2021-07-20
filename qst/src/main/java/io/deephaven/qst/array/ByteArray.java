package io.deephaven.qst.array;

import io.deephaven.qst.type.ByteType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public final class ByteArray extends PrimitiveArrayBase<Byte> {
    static final byte NULL_REPR = Byte.MIN_VALUE;

    public static ByteArray empty() {
        return new ByteArray(new byte[0]);
    }

    public static ByteArray of(byte... values) {
        return builder(values.length).add(values).build();
    }

    public static ByteArray of(Byte... values) {
        return builder(values.length).add(values).build();
    }

    public static ByteArray of(Iterable<Byte> values) {
        if (values instanceof Collection) {
            return of((Collection<Byte>) values);
        }
        return builder(16).add(values).build();
    }

    public static ByteArray of(Collection<Byte> values) {
        return builder(values.size()).add(values).build();
    }

    public static ByteArray ofUnsafe(byte... values) {
        return new ByteArray(values);
    }

    public static Builder builder(int initialSize) {
        return new Builder(initialSize);
    }

    private static byte adapt(Byte x) {
        return x == null ? NULL_REPR : x;
    }

    private static Byte adapt(byte x) {
        return x == NULL_REPR ? null : x;
    }

    private final byte[] values;

    private ByteArray(byte[] values) {
        this.values = Objects.requireNonNull(values);
    }

    /**
     * The raw bytes. Must not be modified.
     *
     * @return the bytes, do <b>not</b> modify
     */
    public final byte[] values() {
        return values;
    }

    @Override
    public final Byte get(int index) {
        return adapt(values[index]);
    }

    @Override
    public final int size() {
        return values().length;
    }

    @Override
    public final ByteType type() {
        return ByteType.instance();
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
        ByteArray byteArray = (ByteArray) o;
        return Arrays.equals(values, byteArray.values);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(values);
    }

    public static class Builder implements ArrayBuilder<Byte, ByteArray, Builder> {

        private byte[] array;
        private int size;

        private Builder(int initialCapacity) {
            this.array = new byte[initialCapacity];
            this.size = 0;
        }

        public synchronized final Builder add(byte item) {
            ensureCapacity();
            array[size++] = item;
            return this;
        }

        public synchronized final Builder add(byte... items) {
            // todo: systemcopy
            for (byte item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final Builder add(Byte item) {
            return add(adapt(item));
        }

        @Override
        public synchronized final Builder add(Byte... items) {
            for (Byte item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final Builder add(Iterable<Byte> items) {
            for (Byte item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final ByteArray build() {
            return new ByteArray(takeAtSize());
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
