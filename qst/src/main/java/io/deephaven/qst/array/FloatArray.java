package io.deephaven.qst.array;

import io.deephaven.qst.type.FloatType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * A {@link FloatType} array.
 */
public final class FloatArray extends PrimitiveArrayBase<Float> {

    public static FloatArray empty() {
        return new FloatArray(new float[0]);
    }

    public static FloatArray of(float... values) {
        return builder(values.length).add(values).build();
    }

    public static FloatArray of(Float... values) {
        return builder(values.length).add(values).build();
    }

    public static FloatArray of(Iterable<Float> values) {
        if (values instanceof Collection) {
            return of((Collection<Float>) values);
        }
        return builder(Util.DEFAULT_BUILDER_INITIAL_CAPACITY).add(values).build();
    }

    public static FloatArray of(Collection<Float> values) {
        return builder(values.size()).add(values).build();
    }

    public static FloatArray ofUnsafe(float... values) {
        return new FloatArray(values);
    }

    public static Builder builder(int initialSize) {
        return new Builder(initialSize);
    }

    private final float[] values;

    private FloatArray(float[] values) {
        this.values = Objects.requireNonNull(values);
    }

    /**
     * The raw floats. Must not be modified.
     *
     * @return the floats, do <b>not</b> modify
     */
    public final float[] values() {
        return values;
    }

    @Override
    public final int size() {
        return values().length;
    }

    @Override
    public final FloatType componentType() {
        return FloatType.instance();
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
        FloatArray that = (FloatArray) o;
        return Arrays.equals(values, that.values);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(values);
    }

    public static class Builder extends PrimitiveArrayHelper<float[]>
        implements ArrayBuilder<Float, FloatArray, Builder> {

        private Builder(int initialCapacity) {
            super(new float[initialCapacity]);
        }

        @Override
        int length(float[] array) {
            return array.length;
        }

        @Override
        void arraycopy(float[] src, int srcPos, float[] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        float[] construct(int size) {
            return new float[size];
        }

        public final Builder add(float item) {
            ensureCapacity();
            array[size++] = item;
            return this;
        }

        public final Builder add(float... items) {
            addImpl(items);
            return this;
        }

        @Override
        public final Builder add(Float item) {
            return add(Util.adapt(item));
        }

        private void addInternal(Float item) {
            array[size++] = Util.adapt(item);
        }

        @Override
        public final Builder add(Float... items) {
            ensureCapacity(items.length);
            for (Float item : items) {
                addInternal(item);
            }
            return this;
        }

        @Override
        public final Builder add(Iterable<Float> items) {
            for (Float item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public final FloatArray build() {
            return new FloatArray(takeAtSize());
        }
    }
}
