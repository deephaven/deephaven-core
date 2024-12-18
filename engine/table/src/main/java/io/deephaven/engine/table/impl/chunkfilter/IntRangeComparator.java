//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.IntComparisons;

public class IntRangeComparator {
    private IntRangeComparator() {} // static use only

    private abstract static class IntIntFilter extends IntChunkFilter {
        final int lower;
        final int upper;

        IntIntFilter(int lower, int upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    final static class IntIntInclusiveInclusiveFilter extends IntIntFilter {
        private IntIntInclusiveInclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.geq(value, lower) && IntComparisons.leq(value, upper);
        }
    }

    final static class IntIntInclusiveExclusiveFilter extends IntIntFilter {
        private IntIntInclusiveExclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.geq(value, lower) && IntComparisons.lt(value, upper);
        }
    }

    final static class IntIntExclusiveInclusiveFilter extends IntIntFilter {
        private IntIntExclusiveInclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.gt(value, lower) && IntComparisons.leq(value, upper);
        }
    }

    final static class IntIntExclusiveExclusiveFilter extends IntIntFilter {
        private IntIntExclusiveExclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(int value) {
            return IntComparisons.gt(value, lower) && IntComparisons.lt(value, upper);
        }
    }

    public static IntChunkFilter makeIntFilter(int lower, int upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new IntIntInclusiveInclusiveFilter(lower, upper);
            } else {
                return new IntIntInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new IntIntExclusiveInclusiveFilter(lower, upper);
            } else {
                return new IntIntExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
