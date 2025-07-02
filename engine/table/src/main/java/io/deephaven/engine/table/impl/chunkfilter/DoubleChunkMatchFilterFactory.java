//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatChunkMatchFilterFactory and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import gnu.trove.iterator.TDoubleIterator;
import gnu.trove.set.hash.TDoubleHashSet;
import io.deephaven.util.compare.DoubleComparisons;

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
            return DoubleComparisons.eq(value, this.value);
        }

        @Override
        public boolean overlaps(double inputLower, double inputUpper) {
            return DoubleComparisons.leq(inputLower, value) && DoubleComparisons.leq(value, inputUpper);
        }
    }

    private final static class InverseSingleValueDoubleChunkFilter extends DoubleChunkFilter {
        private final double value;

        private InverseSingleValueDoubleChunkFilter(double value) {
            this.value = value;
        }

        @Override
        public boolean matches(double value) {
            return !DoubleComparisons.eq(value, this.value);
        }

        @Override
        public boolean overlaps(double inputLower, double inputUpper) {
            // Any interval wider than one point must include a double not equal to `value`, so we simply need to
            // check whether we have a single-point range [value,value] or not.
            return matches(inputLower) || matches(inputUpper);
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
            return DoubleComparisons.eq(value, value1) || DoubleComparisons.eq(value, value2);
        }

        @Override
        public boolean overlaps(double inputLower, double inputUpper) {
            return (DoubleComparisons.leq(inputLower, value1) && DoubleComparisons.leq(value1, inputUpper)) ||
                    (DoubleComparisons.leq(inputLower, value2) && DoubleComparisons.leq(value2, inputUpper));
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
            return !DoubleComparisons.eq(value, value1) && !DoubleComparisons.eq(value, value2);
        }

        @Override
        public boolean overlaps(double inputLower, double inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first three doubles in the range because at max two doubles in
            // the range are excluded (value1 and value2).
            final int maxSteps = 3;
            int steps = 0;
            // @formatter:off
            for (double value = inputLower;
                 DoubleComparisons.leq(value, inputUpper) && steps < maxSteps;
                 value = Math.nextAfter(value, Double.POSITIVE_INFINITY), steps++) {
                if (matches(value)) {
                    return true;
                }
            }
            // @formatter:on
            return false;
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
            return DoubleComparisons.eq(value, value1) ||
                    DoubleComparisons.eq(value, value2) ||
                    DoubleComparisons.eq(value, value3);
        }

        @Override
        public boolean overlaps(double inputLower, double inputUpper) {
            return (DoubleComparisons.leq(inputLower, value1) && DoubleComparisons.leq(value1, inputUpper)) ||
                    (DoubleComparisons.leq(inputLower, value2) && DoubleComparisons.leq(value2, inputUpper)) ||
                    (DoubleComparisons.leq(inputLower, value3) && DoubleComparisons.leq(value3, inputUpper));
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
            return !DoubleComparisons.eq(value, value1) &&
                    !DoubleComparisons.eq(value, value2) &&
                    !DoubleComparisons.eq(value, value3);
        }

        @Override
        public boolean overlaps(double inputLower, double inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first four doubles in the range because at max three doubles
            // in the range are excluded (value1, value2, and value3).
            final int maxSteps = 4;
            int steps = 0;
            // @formatter:off
            for (double value = inputLower;
                 DoubleComparisons.leq(value, inputUpper) && steps < maxSteps;
                 value = Math.nextAfter(value, Double.POSITIVE_INFINITY), steps++) {
                if (matches(value)) {
                    return true;
                }
            }
            // @formatter:on
            return false;
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

        @Override
        public boolean overlaps(double inputLower, double inputUpper) {
            final TDoubleIterator iterator = values.iterator();
            while (iterator.hasNext()) {
                final double value = iterator.next();
                if (DoubleComparisons.leq(inputLower, value) && DoubleComparisons.leq(value, inputUpper)) {
                    return true;
                }
            }
            return false;
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

        @Override
        public boolean overlaps(double inputLower, double inputUpper) {
            // Iterate through the range from inputLower to inputUpper, checking for any value that matches the inverse
            // condition. We only need to check the first `values.size() + 1` doubles in the range because at max
            // `values.size()` doubles in the range are excluded.
            final int maxSteps = values.size() + 1;
            int steps = 0;
            // @formatter:off
            for (double value = inputLower;
                 DoubleComparisons.leq(value, inputUpper) && steps < maxSteps;
                 value = Math.nextAfter(value, Double.POSITIVE_INFINITY), steps++) {
                if (matches(value)) {
                    return true;
                }
            }
            // @formatter:on
            return false;
        }
    }
}
