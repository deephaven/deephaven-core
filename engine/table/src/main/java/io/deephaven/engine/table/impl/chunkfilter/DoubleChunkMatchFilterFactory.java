//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatChunkMatchFilterFactory and run "./gradlew replicateChunkFilters" to regenerate
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

    /**
     * If NaN values are present, return a copy of the array with the NaN values skipped. Otherwise returns the
     * original array.
     */
    private static double[] removeNan(final double[] values) {
        // First pass: count non-NaN elements
        int count = 0;
        for (double v : values) {
            if (!Double.isNaN(v)) {
                count++;
            }
        }

        // If no NaN values, return a copy of the original
        if (count == values.length) {
            return values;
        }

        // Second pass: fill the result array
        double[] result = new double[count];
        int index = 0;
        for (double v : values) {
            if (!Double.isNaN(v)) {
                result[index++] = v;
            }
        }

        return result;
    }


    public static DoubleChunkFilter makeFilter(boolean invertMatch, double... values) {
        final double[] valuesWithoutNaN = removeNan(values);
        
        // If no NaN values were present, we can use the regular filters
        if (values.length == valuesWithoutNaN.length) {
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
        } else {
            if (invertMatch) {
                if (valuesWithoutNaN.length == 0) {
                    return new TrueDoubleChunkFilter(); // all != NaN -> True
                }
                if (valuesWithoutNaN.length == 1) {
                    return new InverseSingleValueNaNDoubleChunkFilter(valuesWithoutNaN[0]);
                }
                if (valuesWithoutNaN.length == 2) {
                    return new InverseTwoValueNaNDoubleChunkFilter(valuesWithoutNaN[0], valuesWithoutNaN[1]);
                }
                if (valuesWithoutNaN.length == 3) {
                    return new InverseThreeValueNaNDoubleChunkFilter(valuesWithoutNaN[0], valuesWithoutNaN[1], valuesWithoutNaN[2]);
                }
                return new InverseMultiValueNaNDoubleChunkFilter(valuesWithoutNaN);
            } else {
                if (valuesWithoutNaN.length == 0) {
                    return new FalseDoubleChunkFilter(); // all == NaN -> False
                }
                if (valuesWithoutNaN.length == 1) {
                    return new SingleValueNaNDoubleChunkFilter(valuesWithoutNaN[0]);
                }
                if (valuesWithoutNaN.length == 2) {
                    return new TwoValueNaNDoubleChunkFilter(valuesWithoutNaN[0], valuesWithoutNaN[1]);
                }
                if (valuesWithoutNaN.length == 3) {
                    return new ThreeValueNaNDoubleChunkFilter(valuesWithoutNaN[0], valuesWithoutNaN[1], valuesWithoutNaN[2]);
                }
                return new MultiValueNaNDoubleChunkFilter(valuesWithoutNaN);
            }
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

    private final static class TrueDoubleChunkFilter extends DoubleChunkFilter {
        @Override
        public boolean matches(double value) {
            return true;
        }
    }

    private final static class FalseDoubleChunkFilter extends DoubleChunkFilter {
        @Override
        public boolean matches(double value) {
            return false;
        }
    }

    private final static class SingleValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final double value;

        private SingleValueNaNDoubleChunkFilter(double value) {
            this.value = value;
        }

        @Override
        public boolean matches(double value) {
            return Double.isNaN(value) || value == this.value;
        }
    }

    private final static class InverseSingleValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final double value;

        private InverseSingleValueNaNDoubleChunkFilter(double value) {
            this.value = value;
        }

        @Override
        public boolean matches(double value) {
            return !Double.isNaN(value) && value != this.value;
        }
    }

    private final static class TwoValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final double value1;
        private final double value2;

        private TwoValueNaNDoubleChunkFilter(double value1, double value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(double value) {
            return Double.isNaN(value) || value == value1 || value == value2;
        }
    }

    private final static class InverseTwoValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final double value1;
        private final double value2;

        private InverseTwoValueNaNDoubleChunkFilter(double value1, double value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(double value) {
            return !Double.isNaN(value) && value != value1 && value != value2;
        }
    }

    private final static class ThreeValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final double value1;
        private final double value2;
        private final double value3;

        private ThreeValueNaNDoubleChunkFilter(double value1, double value2, double value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(double value) {
            return Double.isNaN(value) || value == value1 || value == value2 || value == value3;
        }
    }

    private final static class InverseThreeValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final double value1;
        private final double value2;
        private final double value3;

        private InverseThreeValueNaNDoubleChunkFilter(double value1, double value2, double value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(double value) {
            return !Double.isNaN(value) && value != value1 && value != value2 && value != value3;
        }
    }

    private final static class MultiValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final TDoubleHashSet values;

        private MultiValueNaNDoubleChunkFilter(double... values) {
            this.values = new TDoubleHashSet(values);
        }

        @Override
        public boolean matches(double value) {
            return Double.isNaN(value) || this.values.contains(value);
        }
    }

    private final static class InverseMultiValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final TDoubleHashSet values;

        private InverseMultiValueNaNDoubleChunkFilter(double... values) {
            this.values = new TDoubleHashSet(values);
        }

        @Override
        public boolean matches(double value) {
            return !Double.isNaN(value) && !this.values.contains(value);
        }
    }
}
