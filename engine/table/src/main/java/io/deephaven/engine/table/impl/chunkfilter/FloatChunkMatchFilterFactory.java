//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import io.deephaven.engine.table.MatchOptions;

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

    public static FloatChunkFilter makeFilter(final MatchOptions matchOptions, final float... values) {
        if (matchOptions.nanMatch()) {
            if (matchOptions.inverted()) {
                if (values.length == 1) {
                    return new InverseSingleValueNaNFloatChunkFilter(values[0]);
                }
                if (values.length == 2) {
                    return new InverseTwoValueNaNFloatChunkFilter(values[0], values[1]);
                }
                if (values.length == 3) {
                    return new InverseThreeValueNaNFloatChunkFilter(values[0], values[1], values[2]);
                }
                return new InverseMultiValueNaNFloatChunkFilter(values);
            } else {
                if (values.length == 1) {
                    return new SingleValueNaNFloatChunkFilter(values[0]);
                }
                if (values.length == 2) {
                    return new TwoValueNaNFloatChunkFilter(values[0], values[1]);
                }
                if (values.length == 3) {
                    return new ThreeValueNaNFloatChunkFilter(values[0], values[1], values[2]);
                }
                return new MultiValueNaNFloatChunkFilter(values);
            }
        }

        if (matchOptions.inverted()) {
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

    // A FloatOpenHashSet that canonicalizes -0.0f to +0.0f; NaN values are silently skipped (not added).
    private static final class FloatZeroCanonicalOpenHashSet {
        final FloatOpenHashSet wrapped;

        FloatZeroCanonicalOpenHashSet(float... values) {
            wrapped = new FloatOpenHashSet(values.length);
            for (final float v : values) {
                add(v);
            }
        }

        private static float canonicalize(final float k) {
            return k == 0.0f ? 0.0f : k;
        }

        public boolean add(final float k) {
            return !Float.isNaN(k) && add(canonicalize(k));
        }

        public boolean contains(final float k) {
            return !Float.isNaN(k) && contains(canonicalize(k));
        }
    }

    private final static class MultiValueFloatChunkFilter extends FloatChunkFilter {
        private final FloatZeroCanonicalOpenHashSet values;

        private MultiValueFloatChunkFilter(float... values) {
            this.values = new FloatZeroCanonicalOpenHashSet(values);
        }

        @Override
        public boolean matches(float value) {
            return this.values.contains(value);
        }
    }

    private final static class InverseMultiValueFloatChunkFilter extends FloatChunkFilter {
        private final FloatZeroCanonicalOpenHashSet values;

        private InverseMultiValueFloatChunkFilter(float... values) {
            this.values = new FloatZeroCanonicalOpenHashSet(values);
        }

        @Override
        public boolean matches(float value) {
            return !this.values.contains(value);
        }
    }

    /**
     * Gets the canonicalized bit pattern for the given value. Specifically, ensures that any NaN values have the bit
     * pattern of {@link Float#NaN}, and -0.0 has the bit pattern of 0.0.
     */
    // region getBits
    public static int getBits(float value) {
        return Float.floatToIntBits(value == 0.0f ? 0.0f : value);
    }
    // endregion getBits

    private final static class SingleValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final int valueBits;

        private SingleValueNaNFloatChunkFilter(float value) {
            valueBits = getBits(value);
        }

        @Override
        public boolean matches(float value) {
            return valueBits == getBits(value);
        }
    }

    private final static class InverseSingleValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final int valueBits;

        private InverseSingleValueNaNFloatChunkFilter(float value) {
            valueBits = getBits(value);
        }

        @Override
        public boolean matches(float value) {
            return valueBits != getBits(value);
        }
    }

    private final static class TwoValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final int valueBits1;
        private final int valueBits2;

        private TwoValueNaNFloatChunkFilter(float value1, float value2) {
            this.valueBits1 = getBits(value1);
            this.valueBits2 = getBits(value2);
        }

        @Override
        public boolean matches(float value) {
            final int valueBits = getBits(value);
            return valueBits == valueBits1 || valueBits == valueBits2;
        }
    }

    private final static class InverseTwoValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final int valueBits1;
        private final int valueBits2;

        private InverseTwoValueNaNFloatChunkFilter(float value1, float value2) {
            this.valueBits1 = getBits(value1);
            this.valueBits2 = getBits(value2);
        }

        @Override
        public boolean matches(float value) {
            final int valueBits = getBits(value);
            return valueBits != valueBits1 && valueBits != valueBits2;
        }
    }

    private final static class ThreeValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final int valueBits1;
        private final int valueBits2;
        private final int valueBits3;

        private ThreeValueNaNFloatChunkFilter(float value1, float value2, float value3) {
            this.valueBits1 = getBits(value1);
            this.valueBits2 = getBits(value2);
            this.valueBits3 = getBits(value3);
        }

        @Override
        public boolean matches(float value) {
            final int valueBits = getBits(value);
            return valueBits == valueBits1 || valueBits == valueBits2 || valueBits == valueBits3;
        }
    }

    private final static class InverseThreeValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final int valueBits1;
        private final int valueBits2;
        private final int valueBits3;

        private InverseThreeValueNaNFloatChunkFilter(float value1, float value2, float value3) {
            this.valueBits1 = getBits(value1);
            this.valueBits2 = getBits(value2);
            this.valueBits3 = getBits(value3);
        }

        @Override
        public boolean matches(float value) {
            final int valueBits = Float.floatToIntBits(value);
            return valueBits != valueBits1 && valueBits != valueBits2 && valueBits != valueBits3;
        }
    }

    private final static class MultiValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final IntSet values;

        private MultiValueNaNFloatChunkFilter(float... values) {
            this.values = new IntOpenHashSet(values.length);
            for (float v : values) {
                this.values.add(getBits(v));
            }
        }

        @Override
        public boolean matches(float value) {
            final int valueBits = getBits(value);
            return this.values.contains(valueBits);
        }
    }

    private final static class InverseMultiValueNaNFloatChunkFilter extends FloatChunkFilter {
        private final IntSet values;

        private InverseMultiValueNaNFloatChunkFilter(float... values) {
            this.values = new IntOpenHashSet(values.length);
            for (float v : values) {
                this.values.add(getBits(v));
            }
        }

        @Override
        public boolean matches(float value) {
            final int valueBits = getBits(value);
            return !values.contains(valueBits);
        }
    }
}
