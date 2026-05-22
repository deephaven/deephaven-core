//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatChunkMatchFilterFactory and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import io.deephaven.engine.table.MatchOptions;

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

    public static DoubleChunkFilter makeFilter(final MatchOptions matchOptions, final double... values) {
        if (matchOptions.nanMatch()) {
            if (matchOptions.inverted()) {
                if (values.length == 1) {
                    return new InverseSingleValueNaNDoubleChunkFilter(values[0]);
                }
                if (values.length == 2) {
                    return new InverseTwoValueNaNDoubleChunkFilter(values[0], values[1]);
                }
                if (values.length == 3) {
                    return new InverseThreeValueNaNDoubleChunkFilter(values[0], values[1], values[2]);
                }
                return new InverseMultiValueNaNDoubleChunkFilter(values);
            } else {
                if (values.length == 1) {
                    return new SingleValueNaNDoubleChunkFilter(values[0]);
                }
                if (values.length == 2) {
                    return new TwoValueNaNDoubleChunkFilter(values[0], values[1]);
                }
                if (values.length == 3) {
                    return new ThreeValueNaNDoubleChunkFilter(values[0], values[1], values[2]);
                }
                return new MultiValueNaNDoubleChunkFilter(values);
            }
        }

        if (matchOptions.inverted()) {
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

    // A DoubleOpenHashSet that canonicalizes -0.0d to +0.0d; NaN values are silently skipped (not added).
    private static final class DoubleZeroCanonicalOpenHashSet {
        final DoubleOpenHashSet wrapped;

        DoubleZeroCanonicalOpenHashSet(double... values) {
            wrapped = new DoubleOpenHashSet(values.length);
            for (final double v : values) {
                add(v);
            }
        }

        private static double canonicalize(final double k) {
            return k == 0.0d ? 0.0d : k;
        }

        public boolean add(final double k) {
            return !Double.isNaN(k) && add(canonicalize(k));
        }

        public boolean contains(final double k) {
            return !Double.isNaN(k) && contains(canonicalize(k));
        }
    }

    private final static class MultiValueDoubleChunkFilter extends DoubleChunkFilter {
        private final DoubleZeroCanonicalOpenHashSet values;

        private MultiValueDoubleChunkFilter(double... values) {
            this.values = new DoubleZeroCanonicalOpenHashSet(values);
        }

        @Override
        public boolean matches(double value) {
            return this.values.contains(value);
        }
    }

    private final static class InverseMultiValueDoubleChunkFilter extends DoubleChunkFilter {
        private final DoubleZeroCanonicalOpenHashSet values;

        private InverseMultiValueDoubleChunkFilter(double... values) {
            this.values = new DoubleZeroCanonicalOpenHashSet(values);
        }

        @Override
        public boolean matches(double value) {
            return !this.values.contains(value);
        }
    }

    /**
     * Gets the canonicalized bit pattern for the given value. Specifically, ensures that any NaN values have the bit
     * pattern of {@link Double#NaN}, and -0.0 has the bit pattern of 0.0.
     */
    // region getBits
    public static long getBits(double value) {
        return Double.doubleToLongBits(value == 0.0d ? 0.0d : value);
    }

    // endregion getBits

    private final static class SingleValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final long valueBits;

        private SingleValueNaNDoubleChunkFilter(double value) {
            valueBits = getBits(value);
        }

        @Override
        public boolean matches(double value) {
            return valueBits == getBits(value);
        }
    }

    private final static class InverseSingleValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final long valueBits;

        private InverseSingleValueNaNDoubleChunkFilter(double value) {
            valueBits = getBits(value);
        }

        @Override
        public boolean matches(double value) {
            return valueBits != getBits(value);
        }
    }

    private final static class TwoValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final long valueBits1;
        private final long valueBits2;

        private TwoValueNaNDoubleChunkFilter(double value1, double value2) {
            this.valueBits1 = getBits(value1);
            this.valueBits2 = getBits(value2);
        }

        @Override
        public boolean matches(double value) {
            final long valueBits = getBits(value);
            return valueBits == valueBits1 || valueBits == valueBits2;
        }
    }

    private final static class InverseTwoValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final long valueBits1;
        private final long valueBits2;

        private InverseTwoValueNaNDoubleChunkFilter(double value1, double value2) {
            this.valueBits1 = getBits(value1);
            this.valueBits2 = getBits(value2);
        }

        @Override
        public boolean matches(double value) {
            final long valueBits = getBits(value);
            return valueBits != valueBits1 && valueBits != valueBits2;
        }
    }

    private final static class ThreeValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final long valueBits1;
        private final long valueBits2;
        private final long valueBits3;

        private ThreeValueNaNDoubleChunkFilter(double value1, double value2, double value3) {
            this.valueBits1 = getBits(value1);
            this.valueBits2 = getBits(value2);
            this.valueBits3 = getBits(value3);
        }

        @Override
        public boolean matches(double value) {
            final long valueBits = getBits(value);
            return valueBits == valueBits1 || valueBits == valueBits2 || valueBits == valueBits3;
        }
    }

    private final static class InverseThreeValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final long valueBits1;
        private final long valueBits2;
        private final long valueBits3;

        private InverseThreeValueNaNDoubleChunkFilter(double value1, double value2, double value3) {
            this.valueBits1 = getBits(value1);
            this.valueBits2 = getBits(value2);
            this.valueBits3 = getBits(value3);
        }

        @Override
        public boolean matches(double value) {
            final long valueBits = Double.doubleToLongBits(value);
            return valueBits != valueBits1 && valueBits != valueBits2 && valueBits != valueBits3;
        }
    }

    private final static class MultiValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final LongSet values;

        private MultiValueNaNDoubleChunkFilter(double... values) {
            this.values = new LongOpenHashSet(values.length);
            for (double v : values) {
                this.values.add(getBits(v));
            }
        }

        @Override
        public boolean matches(double value) {
            final long valueBits = getBits(value);
            return this.values.contains(valueBits);
        }
    }

    private final static class InverseMultiValueNaNDoubleChunkFilter extends DoubleChunkFilter {
        private final LongSet values;

        private InverseMultiValueNaNDoubleChunkFilter(double... values) {
            this.values = new LongOpenHashSet(values.length);
            for (double v : values) {
                this.values.add(getBits(v));
            }
        }

        @Override
        public boolean matches(double value) {
            final long valueBits = getBits(value);
            return !values.contains(valueBits);
        }
    }
}
