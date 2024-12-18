//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.DoubleComparisons;

public class DoubleRangeComparator {
    private DoubleRangeComparator() {} // static use only

    private abstract static class DoubleDoubleFilter extends DoubleChunkFilter {
        final double lower;
        final double upper;

        DoubleDoubleFilter(double lower, double upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    private final static class DoubleDoubleInclusiveInclusiveFilter extends DoubleDoubleFilter {
        private DoubleDoubleInclusiveInclusiveFilter(double lower, double upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(double value) {
            return DoubleComparisons.geq(value, lower) && DoubleComparisons.leq(value, upper);
        }
    }

    private final static class DoubleDoubleInclusiveExclusiveFilter extends DoubleDoubleFilter {
        private DoubleDoubleInclusiveExclusiveFilter(double lower, double upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(double value) {
            return DoubleComparisons.geq(value, lower) && DoubleComparisons.lt(value, upper);
        }
    }

    private final static class DoubleDoubleExclusiveInclusiveFilter extends DoubleDoubleFilter {
        private DoubleDoubleExclusiveInclusiveFilter(double lower, double upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(double value) {
            return DoubleComparisons.gt(value, lower) && DoubleComparisons.leq(value, upper);
        }
    }

    private final static class DoubleDoubleExclusiveExclusiveFilter extends DoubleDoubleFilter {
        private DoubleDoubleExclusiveExclusiveFilter(double lower, double upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(double value) {
            return DoubleComparisons.gt(value, lower) && DoubleComparisons.lt(value, upper);
        }
    }

    public static DoubleChunkFilter makeDoubleFilter(double lower, double upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new DoubleDoubleInclusiveInclusiveFilter(lower, upper);
            } else {
                return new DoubleDoubleInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new DoubleDoubleExclusiveInclusiveFilter(lower, upper);
            } else {
                return new DoubleDoubleExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
