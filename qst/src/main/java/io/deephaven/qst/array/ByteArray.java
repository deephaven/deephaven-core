package io.deephaven.qst.array;

import io.deephaven.qst.type.ByteType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * A {@link ByteType} array.
 */
public final class ByteArray extends PrimitiveArrayBase<Byte> {

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
        return builder(Util.DEFAULT_BUILDER_INITIAL_CAPACITY).add(values).build();
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
    public final int size() {
        return values().length;
    }

    @Override
    public final ByteType componentType() {
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

    public static class Builder extends PrimitiveArrayHelper<byte[]>
            implements ArrayBuilder<Byte, ByteArray, Builder> {

        private Builder(int initialCapacity) {
            super(new byte[initialCapacity]);
        }

        @Override
        int length(byte[] array) {
            return array.length;
        }

        @Override
        void arraycopy(byte[] src, int srcPos, byte[] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        byte[] construct(int size) {
            return new byte[size];
        }

        public final Builder add(byte item) {
            ensureCapacity();
            array[size++] = item;
            return this;
        }

        private void addInternal(Byte item) {
            array[size++] = Util.adapt(item);
        }

        public final Builder add(byte... items) {
            addImpl(items);
            return this;
        }

        @Override
        public final Builder add(Byte item) {
            return add(Util.adapt(item));
        }

        @Override
        public final Builder add(Byte... items) {
            ensureCapacity(items.length);
            for (Byte item : items) {
                addInternal(item);
            }
            return this;
        }

        @Override
        public final Builder add(Iterable<Byte> items) {
            for (Byte item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public final ByteArray build() {
            return new ByteArray(takeAtSize());
        }
    }
}
