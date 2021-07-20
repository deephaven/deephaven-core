package io.deephaven.qst.array;

import io.deephaven.qst.type.DoubleType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public final class DoubleArray extends PrimitiveArrayBase<Double> {
    static final double NULL_REPR = -Double.MAX_VALUE;

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
        return builder(16).add(values).build();
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

    private static double adapt(Double x) {
        return x == null ? NULL_REPR : x;
    }

    private static Double adapt(double x) {
        return x == NULL_REPR ? null : x;
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
    public final Double get(int index) {
        return adapt(values[index]);
    }

    @Override
    public final int size() {
        return values().length;
    }

    @Override
    public final DoubleType type() {
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

    public static class Builder implements ArrayBuilder<Double, DoubleArray, Builder> {

        private double[] array;
        private int size;

        private Builder(int initialCapacity) {
            this.array = new double[initialCapacity];
            this.size = 0;
        }

        public synchronized final Builder add(double item) {
            ensureCapacity();
            array[size++] = item;
            return this;
        }

        public synchronized final Builder add(double... items) {
            // todo: systemcopy
            for (double item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final Builder add(Double item) {
            return add(adapt(item));
        }

        @Override
        public synchronized final Builder add(Double... items) {
            for (Double item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final Builder add(Iterable<Double> items) {
            for (Double item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final DoubleArray build() {
            return new DoubleArray(takeAtSize());
        }

        private void ensureCapacity() {
            if (size == array.length) {
                double[] next = new double[array.length == 0 ? 1 : array.length * 2];
                System.arraycopy(array, 0, next, 0, array.length);
                array = next;
            }
        }

        private double[] takeAtSize() {
            if (size == array.length) {
                return array; // great case, no copying necessary :)
            }
            double[] atSize = new double[size];
            System.arraycopy(array, 0, atSize, 0, size);
            return atSize;
        }
    }
}
