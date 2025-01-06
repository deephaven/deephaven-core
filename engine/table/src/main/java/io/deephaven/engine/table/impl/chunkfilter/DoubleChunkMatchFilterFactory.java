//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkMatchFilterFactory and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import gnu.trove.set.hash.TDoubleHashSet;

/**
 * Creates chunk filters for double values.
 * <p>
 * The strategy is that for one, two, or three values we have specialized classes that will do the appropriate simple
 * equality check.
 * <p>
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class DoubleChunkMatchFilterFactory {
    private DoubleChunkMatchFilterFactory() {} // static use only

    public static DoubleChunkFilter makeFilter(boolean invertMatch, double... values) {
        if (invertMatch) {
            if (values.length == 1) {
                return new InverseSingleValueDoubleChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new InverseTwoValueDoubleChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new InverseThreeValueDoubleChunkFilter(values[0], values[1], values[2]);
            }
            return new InverseMultiValueDoubleChunkFilter(values);
        } else {
            if (values.length == 1) {
                return new SingleValueDoubleChunkFilter(values[0]);
            }
            if (values.length == 2) {
                return new TwoValueDoubleChunkFilter(values[0], values[1]);
            }
            if (values.length == 3) {
                return new ThreeValueDoubleChunkFilter(values[0], values[1], values[2]);
            }
            return new MultiValueDoubleChunkFilter(values);
        }
    }

    private final static class SingleValueDoubleChunkFilter extends DoubleChunkFilter {
        private final double value;

        private SingleValueDoubleChunkFilter(double value) {
            this.value = value;
        }

        @Override
        public boolean matches(double value) {
            return value == this.value;
        }
    }

    private final static class InverseSingleValueDoubleChunkFilter extends DoubleChunkFilter {
        private final double value;

        private InverseSingleValueDoubleChunkFilter(double value) {
            this.value = value;
        }

        @Override
        public boolean matches(double value) {
            return value != this.value;
        }
    }

    private final static class TwoValueDoubleChunkFilter extends DoubleChunkFilter {
        private final double value1;
        private final double value2;

        private TwoValueDoubleChunkFilter(double value1, double value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(double value) {
            return value == value1 || value == value2;
        }
    }

    private final static class InverseTwoValueDoubleChunkFilter extends DoubleChunkFilter {
        private final double value1;
        private final double value2;

        private InverseTwoValueDoubleChunkFilter(double value1, double value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(double value) {
            return value != value1 && value != value2;
        }
    }

    private final static class ThreeValueDoubleChunkFilter extends DoubleChunkFilter {
        private final double value1;
        private final double value2;
        private final double value3;

        private ThreeValueDoubleChunkFilter(double value1, double value2, double value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(double value) {
            return value == value1 || value == value2 || value == value3;
        }
    }

    private final static class InverseThreeValueDoubleChunkFilter extends DoubleChunkFilter {
        private final double value1;
        private final double value2;
        private final double value3;

        private InverseThreeValueDoubleChunkFilter(double value1, double value2, double value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(double value) {
            return value != value1 && value != value2 && value != value3;
        }
    }

    private final static class MultiValueDoubleChunkFilter extends DoubleChunkFilter {
        private final TDoubleHashSet values;

        private MultiValueDoubleChunkFilter(double... values) {
            this.values = new TDoubleHashSet(values);
        }

        @Override
        public boolean matches(double value) {
            return this.values.contains(value);
        }
    }

    private final static class InverseMultiValueDoubleChunkFilter extends DoubleChunkFilter {
        private final TDoubleHashSet values;

        private InverseMultiValueDoubleChunkFilter(double... values) {
            this.values = new TDoubleHashSet(values);
        }

        @Override
        public boolean matches(double value) {
            return !this.values.contains(value);
        }
    }
}
