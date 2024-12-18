//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.ShortComparisons;

public class ShortRangeComparator {
    private ShortRangeComparator() {} // static use only

    private abstract static class ShortShortFilter extends ShortChunkFilter {
        final short lower;
        final short upper;

        ShortShortFilter(short lower, short upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    final static class ShortShortInclusiveInclusiveFilter extends ShortShortFilter {
        private ShortShortInclusiveInclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.geq(value, lower) && ShortComparisons.leq(value, upper);
        }
    }

    final static class ShortShortInclusiveExclusiveFilter extends ShortShortFilter {
        private ShortShortInclusiveExclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.geq(value, lower) && ShortComparisons.lt(value, upper);
        }
    }

    final static class ShortShortExclusiveInclusiveFilter extends ShortShortFilter {
        private ShortShortExclusiveInclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.gt(value, lower) && ShortComparisons.leq(value, upper);
        }
    }

    final static class ShortShortExclusiveExclusiveFilter extends ShortShortFilter {
        private ShortShortExclusiveExclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.gt(value, lower) && ShortComparisons.lt(value, upper);
        }
    }

    public static ShortChunkFilter makeShortFilter(short lower, short upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new ShortShortInclusiveInclusiveFilter(lower, upper);
            } else {
                return new ShortShortInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new ShortShortExclusiveInclusiveFilter(lower, upper);
            } else {
                return new ShortShortExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
