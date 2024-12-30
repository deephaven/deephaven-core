//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.LongComparisons;

public class LongRangeComparator {
    private LongRangeComparator() {} // static use only

    private abstract static class LongLongFilter extends LongChunkFilter {
        final long lower;
        final long upper;

        LongLongFilter(long lower, long upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    final static class LongLongInclusiveInclusiveFilter extends LongLongFilter {
        private LongLongInclusiveInclusiveFilter(long lower, long upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(long value) {
            return LongComparisons.geq(value, lower) && LongComparisons.leq(value, upper);
        }
    }

    final static class LongLongInclusiveExclusiveFilter extends LongLongFilter {
        private LongLongInclusiveExclusiveFilter(long lower, long upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(long value) {
            return LongComparisons.geq(value, lower) && LongComparisons.lt(value, upper);
        }
    }

    final static class LongLongExclusiveInclusiveFilter extends LongLongFilter {
        private LongLongExclusiveInclusiveFilter(long lower, long upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(long value) {
            return LongComparisons.gt(value, lower) && LongComparisons.leq(value, upper);
        }
    }

    final static class LongLongExclusiveExclusiveFilter extends LongLongFilter {
        private LongLongExclusiveExclusiveFilter(long lower, long upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(long value) {
            return LongComparisons.gt(value, lower) && LongComparisons.lt(value, upper);
        }
    }

    public static LongChunkFilter makeLongFilter(long lower, long upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new LongLongInclusiveInclusiveFilter(lower, upper);
            } else {
                return new LongLongInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new LongLongExclusiveInclusiveFilter(lower, upper);
            } else {
                return new LongLongExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
