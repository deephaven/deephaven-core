package io.deephaven.qst.array;

import io.deephaven.qst.type.FloatType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public final class FloatArray extends PrimitiveArrayBase<Float> {

    static final float NULL_REPR = -Float.MAX_VALUE;

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
        return builder(16).add(values).build();
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

    private static float adapt(Float x) {
        return x == null ? NULL_REPR : x;
    }

    private static Float adapt(float x) {
        return x == NULL_REPR ? null : x;
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
    public final Float get(int index) {
        return adapt(values[index]);
    }

    @Override
    public final int size() {
        return values().length;
    }

    @Override
    public final FloatType type() {
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

    public static class Builder implements ArrayBuilder<Float, FloatArray, Builder> {

        private float[] array;
        private int size;

        private Builder(int initialCapacity) {
            this.array = new float[initialCapacity];
            this.size = 0;
        }

        public synchronized final Builder add(float item) {
            ensureCapacity();
            array[size++] = item;
            return this;
        }

        public synchronized final Builder add(float... items) {
            // todo: systemcopy
            for (float item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final Builder add(Float item) {
            return add(adapt(item));
        }

        @Override
        public synchronized final Builder add(Float... items) {
            for (Float item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final Builder add(Iterable<Float> items) {
            for (Float item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final FloatArray build() {
            return new FloatArray(takeAtSize());
        }

        private void ensureCapacity() {
            if (size == array.length) {
                float[] next = new float[array.length == 0 ? 1 : array.length * 2];
                System.arraycopy(array, 0, next, 0, array.length);
                array = next;
            }
        }

        private float[] takeAtSize() {
            if (size == array.length) {
                return array; // great case, no copying necessary :)
            }
            float[] atSize = new float[size];
            System.arraycopy(array, 0, atSize, 0, size);
            return atSize;
        }
    }
}
