package io.deephaven.qst.array;

import io.deephaven.qst.type.BooleanType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * A {@link BooleanType} array.
 */
public final class BooleanArray extends PrimitiveArrayBase<Boolean> {

    public static BooleanArray empty() {
        return new BooleanArray(new byte[0]);
    }

    public static BooleanArray of(byte... values) {
        return builder(values.length).add(values).build();
    }

    public static BooleanArray of(Boolean... values) {
        return builder(values.length).add(values).build();
    }

    public static BooleanArray of(Iterable<Boolean> values) {
        if (values instanceof Collection) {
            return of((Collection<Boolean>) values);
        }
        return builder(Util.DEFAULT_BUILDER_INITIAL_CAPACITY).add(values).build();
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
    public final BooleanType componentType() {
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

    public static class Builder extends PrimitiveArrayHelper<byte[]>
            implements ArrayBuilder<Boolean, BooleanArray, Builder> {

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

        public final Builder add(byte... items) {
            addImpl(items);
            return this;
        }

        @Override
        public final Builder add(Boolean item) {
            return add(Util.adapt(item));
        }

        private void addInternal(Boolean item) {
            array[size++] = Util.adapt(item);
        }

        @Override
        public final Builder add(Boolean... items) {
            ensureCapacity(items.length);
            for (Boolean item : items) {
                addInternal(item);
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
    }
}
