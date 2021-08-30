package io.deephaven.qst.array;

import io.deephaven.qst.type.DoubleType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * A {@link DoubleType} array.
 */
public final class DoubleArray extends PrimitiveArrayBase<Double> {

    public static DoubleArray empty() {
        return new DoubleArray(new double[0]);
    }

    public static DoubleArray of(double... values) {
        return builder(values.length).add(values).build();
    }

    public static DoubleArray of(Double... values) {
        return builder(values.length).add(values).build();
    }

    public static DoubleArray of(Iterable<Double> values) {
        if (values instanceof Collection) {
            return of((Collection<Double>) values);
        }
        return builder(Util.DEFAULT_BUILDER_INITIAL_CAPACITY).add(values).build();
    }

    public static DoubleArray of(Collection<Double> values) {
        return builder(values.size()).add(values).build();
    }

    public static DoubleArray ofUnsafe(double... values) {
        return new DoubleArray(values);
    }

    public static Builder builder(int initialSize) {
        return new Builder(initialSize);
    }

    private final double[] values;

    private DoubleArray(double[] values) {
        this.values = Objects.requireNonNull(values);
    }

    /**
     * The raw doubles. Must not be modified.
     *
     * @return the doubles, do <b>not</b> modify
     */
    public final double[] values() {
        return values;
    }

    @Override
    public final int size() {
        return values().length;
    }

    @Override
    public final DoubleType componentType() {
        return DoubleType.instance();
    }

    @Override
    public <V extends PrimitiveArray.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DoubleArray that = (DoubleArray) o;
        return Arrays.equals(values, that.values);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(values);
    }

    public static class Builder extends PrimitiveArrayHelper<double[]>
            implements ArrayBuilder<Double, DoubleArray, Builder> {

        private Builder(int initialCapacity) {
            super(new double[initialCapacity]);
        }

        @Override
        int length(double[] array) {
            return array.length;
        }

        @Override
        void arraycopy(double[] src, int srcPos, double[] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        double[] construct(int size) {
            return new double[size];
        }

        public final Builder add(double item) {
            ensureCapacity();
            array[size++] = item;
            return this;
        }

        public final Builder add(double... items) {
            addImpl(items);
            return this;
        }

        @Override
        public final Builder add(Double item) {
            return add(Util.adapt(item));
        }

        private void addInternal(Double item) {
            array[size++] = Util.adapt(item);
        }

        @Override
        public final Builder add(Double... items) {
            ensureCapacity(items.length);
            for (Double item : items) {
                addInternal(item);
            }
            return this;
        }

        @Override
        public final Builder add(Iterable<Double> items) {
            for (Double item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public final DoubleArray build() {
            return new DoubleArray(takeAtSize());
        }
    }
}
