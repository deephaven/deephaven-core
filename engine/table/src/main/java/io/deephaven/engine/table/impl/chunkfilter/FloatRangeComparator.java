//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.FloatComparisons;

public class FloatRangeComparator {
    private FloatRangeComparator() {} // static use only

    private abstract static class FloatFloatFilter extends FloatChunkFilter {
        final float lower;
        final float upper;

        FloatFloatFilter(float lower, float upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    private final static class FloatFloatInclusiveInclusiveFilter extends FloatFloatFilter {
        private FloatFloatInclusiveInclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(float value) {
            return FloatComparisons.geq(value, lower) && FloatComparisons.leq(value, upper);
        }

        @Override
        public boolean overlaps(float inputLower, float inputUpper) {
            return FloatComparisons.geq(inputUpper, lower) && FloatComparisons.geq(upper, inputLower);
        }
    }

    private final static class FloatFloatInclusiveExclusiveFilter extends FloatFloatFilter {
        private FloatFloatInclusiveExclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(float value) {
            return FloatComparisons.geq(value, lower) && FloatComparisons.lt(value, upper);
        }

        @Override
        public boolean overlaps(float inputLower, float inputUpper) {
            return FloatComparisons.geq(inputUpper, lower) && FloatComparisons.gt(upper, inputLower);
        }
    }

    private final static class FloatFloatExclusiveInclusiveFilter extends FloatFloatFilter {
        private FloatFloatExclusiveInclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(float value) {
            return FloatComparisons.gt(value, lower) && FloatComparisons.leq(value, upper);
        }

        @Override
        public boolean overlaps(float inputLower, float inputUpper) {
            return FloatComparisons.gt(inputUpper, lower) && FloatComparisons.geq(upper, inputLower);
        }
    }

    private final static class FloatFloatExclusiveExclusiveFilter extends FloatFloatFilter {
        private FloatFloatExclusiveExclusiveFilter(float lower, float upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(float value) {
            return FloatComparisons.gt(value, lower) && FloatComparisons.lt(value, upper);
        }

        @Override
        public boolean overlaps(float inputLower, float inputUpper) {
            return FloatComparisons.gt(inputUpper, lower) && FloatComparisons.gt(upper, inputLower);
        }
    }

    public static FloatChunkFilter makeFloatFilter(float lower, float upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new FloatFloatInclusiveInclusiveFilter(lower, upper);
            } else {
                return new FloatFloatInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new FloatFloatExclusiveInclusiveFilter(lower, upper);
            } else {
                return new FloatFloatExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
