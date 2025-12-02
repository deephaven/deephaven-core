//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import gnu.trove.set.hash.TFloatHashSet;

/**
 * Creates chunk filters for float values.
 * <p>
 * The strategy is that for one, two, or three values we have specialized classes that will do the appropriate simple
 * equality check.
 * <p>
 * For more values, we use a trove set and check contains for each value in the chunk.
 */
public class FloatChunkMatchFilterFactory {
    private FloatChunkMatchFilterFactory() {} // static use only

    /**
     * If NaN values are present, return a copy of the array with the NaN values skipped. Otherwise returns the
     * original array.
     */
    private static float[] removeNan(final float[] values) {
        // First pass: count non-NaN elements
        int count = 0;
        for (float v : values) {
            if (!Float.isNaN(v)) {
                count++;
            }
        }

        // If no NaN values, return a copy of the original
        if (count == values.length) {
            return values;
        }

        // Second pass: fill the result array
        float[] result = new float[count];
        int index = 0;
        for (float v : values) {
            if (!Float.isNaN(v)) {
                result[index++] = v;
            }
        }

        return result;
    }


    public static FloatChunkFilter makeFilter(boolean invertMatch, float... values) {
        final float[] valuesWithoutNaN = removeNan(values);
        
        // If no NaN values were present, we can use the regular filters
        if (values.length == valuesWithoutNaN.length) {
            if (invertMatch) {
                if (values.length == 1) {
                    return new InverseSingleValueFloatChunkFilter(values[0]);
                }
                if (values.length == 2) {
                    return new InverseTwoValueFloatChunkFilter(values[0], values[1]);
                }
                if (values.length == 3) {
                    return new InverseThreeValueFloatChunkFilter(values[0], values[1], values[2]);
                }
                return new InverseMultiValueFloatChunkFilter(values);
            } else {
                if (values.length == 1) {
                    return new SingleValueFloatChunkFilter(values[0]);
                }
                if (values.length == 2) {
                    return new TwoValueFloatChunkFilter(values[0], values[1]);
                }
                if (values.length == 3) {
                    return new ThreeValueFloatChunkFilter(values[0], values[1], values[2]);
                }
                return new MultiValueFloatChunkFilter(values);
            }
        } else {
            if (invertMatch) {
                if (valuesWithoutNaN.length == 0) {
                    return new TrueFloatChunkFilter(); // all != NaN -> True
                }
                if (valuesWithoutNaN.length == 1) {
                    return new InverseSingleValueNaNFloatChunkFilter(valuesWithoutNaN[0]);
                }
                if (valuesWithoutNaN.length == 2) {
                    return new InverseTwoValueNaNFloatChunkFilter(valuesWithoutNaN[0], valuesWithoutNaN[1]);
                }
                if (valuesWithoutNaN.length == 3) {
                    return new InverseThreeValueNaNFloatChunkFilter(valuesWithoutNaN[0], valuesWithoutNaN[1], valuesWithoutNaN[2]);
                }
                return new InverseMultiValueNaNFloatChunkFilter(valuesWithoutNaN);
            } else {
                if (valuesWithoutNaN.length == 0) {
                    return new FalseFloatChunkFilter(); // all == NaN -> False
                }
                if (valuesWithoutNaN.length == 1) {
                    return new SingleValueNaNFloatChunkFilter(valuesWithoutNaN[0]);
                }
                if (valuesWithoutNaN.length == 2) {
                    return new TwoValueNaNFloatChunkFilter(valuesWithoutNaN[0], valuesWithoutNaN[1]);
                }
                if (valuesWithoutNaN.length == 3) {
                    return new ThreeValueNaNFloatChunkFilter(valuesWithoutNaN[0], valuesWithoutNaN[1], valuesWithoutNaN[2]);
                }
                return new MultiValueNaNFloatChunkFilter(valuesWithoutNaN);
            }
        }
    }

    private final static class SingleValueFloatChunkFilter extends FloatChunkFilter {
        private final float value;

        private SingleValueFloatChunkFilter(float value) {
            this.value = value;
        }

        @Override
        public boolean matches(float value) {
            return value == this.value;
        }
    }

    private final static class InverseSingleValueFloatChunkFilter extends FloatChunkFilter {
        private final float value;

        private InverseSingleValueFloatChunkFilter(float value) {
            this.value = value;
        }

        @Override
        public boolean matches(float value) {
            return value != this.value;
        }
    }

    private final static class TwoValueFloatChunkFilter extends FloatChunkFilter {
        private final float value1;
        private final float value2;

        private TwoValueFloatChunkFilter(float value1, float value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(float value) {
            return value == value1 || value == value2;
        }
    }

    private final static class InverseTwoValueFloatChunkFilter extends FloatChunkFilter {
        private final float value1;
        private final float value2;

        private InverseTwoValueFloatChunkFilter(float value1, float value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(float value) {
            return value != value1 && value != value2;
        }
    }

    private final static class ThreeValueFloatChunkFilter extends FloatChunkFilter {
        private final float value1;
        private final float value2;
        private final float value3;

        private ThreeValueFloatChunkFilter(float value1, float value2, float value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(float value) {
            return value == value1 || value == value2 || value == value3;
        }
    }

    private final static class InverseThreeValueFloatChunkFilter extends FloatChunkFilter {
        private final float value1;
        private final float value2;
        private final float value3;

        private InverseThreeValueFloatChunkFilter(float value1, float value2, float value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(float value) {
            return value != value1 && value != value2 && value != value3;
        }
    }

    private final static class MultiValueFloatChunkFilter extends FloatChunkFilter {
        private final TFloatHashSet values;

        private MultiValueFloatChunkFilter(float... values) {
            this.values = new TFloatHashSet(values);
        }

        @Override
        public boolean matches(float value) {
            return this.values.contains(value);
        }
    }

    private final static class InverseMultiValueFloatChunkFilter extends FloatChunkFilter {
        private final TFloatHashSet values;

        private InverseMultiValueFloatChunkFilter(float... values) {
            this.values = new TFloatHashSet(values);
        }

        @Override
        public boolean matches(float value) {
            return !this.values.contains(value);
        }
    }

    private final static class TrueFloatChunkFilter extends FloatChunkFilter {
        @Override
        public boolean matches(float value) {
            return true;
        }
    }

    private final static class FalseFloatChunkFilter extends FloatChunkFilter {
        @Override
        public boolean matches(float value) {
            return false;
        }
    }

    private final static class SingleValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final float value;

        private SingleValueNaNFloatChunkFilter(float value) {
            this.value = value;
        }

        @Override
        public boolean matches(float value) {
            return Float.isNaN(value) || value == this.value;
        }
    }

    private final static class InverseSingleValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final float value;

        private InverseSingleValueNaNFloatChunkFilter(float value) {
            this.value = value;
        }

        @Override
        public boolean matches(float value) {
            return !Float.isNaN(value) && value != this.value;
        }
    }

    private final static class TwoValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final float value1;
        private final float value2;

        private TwoValueNaNFloatChunkFilter(float value1, float value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(float value) {
            return Float.isNaN(value) || value == value1 || value == value2;
        }
    }

    private final static class InverseTwoValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final float value1;
        private final float value2;

        private InverseTwoValueNaNFloatChunkFilter(float value1, float value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public boolean matches(float value) {
            return !Float.isNaN(value) && value != value1 && value != value2;
        }
    }

    private final static class ThreeValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final float value1;
        private final float value2;
        private final float value3;

        private ThreeValueNaNFloatChunkFilter(float value1, float value2, float value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(float value) {
            return Float.isNaN(value) || value == value1 || value == value2 || value == value3;
        }
    }

    private final static class InverseThreeValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final float value1;
        private final float value2;
        private final float value3;

        private InverseThreeValueNaNFloatChunkFilter(float value1, float value2, float value3) {
            this.value1 = value1;
            this.value2 = value2;
            this.value3 = value3;
        }

        @Override
        public boolean matches(float value) {
            return !Float.isNaN(value) && value != value1 && value != value2 && value != value3;
        }
    }

    private final static class MultiValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final TFloatHashSet values;

        private MultiValueNaNFloatChunkFilter(float... values) {
            this.values = new TFloatHashSet(values);
        }

        @Override
        public boolean matches(float value) {
            return Float.isNaN(value) || this.values.contains(value);
        }
    }

    private final static class InverseMultiValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final TFloatHashSet values;

        private InverseMultiValueNaNFloatChunkFilter(float... values) {
            this.values = new TFloatHashSet(values);
        }

        @Override
        public boolean matches(float value) {
            return !Float.isNaN(value) && !this.values.contains(value);
        }
    }
}
