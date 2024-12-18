//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.CharComparisons;

public class CharRangeComparator {
    private CharRangeComparator() {} // static use only

    private abstract static class CharCharFilter extends CharChunkFilter {
        final char lower;
        final char upper;

        CharCharFilter(char lower, char upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    final static class CharCharInclusiveInclusiveFilter extends CharCharFilter {
        private CharCharInclusiveInclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(char value) {
            return CharComparisons.geq(value, lower) && CharComparisons.leq(value, upper);
        }
    }

    final static class CharCharInclusiveExclusiveFilter extends CharCharFilter {
        private CharCharInclusiveExclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(char value) {
            return CharComparisons.geq(value, lower) && CharComparisons.lt(value, upper);
        }
    }

    final static class CharCharExclusiveInclusiveFilter extends CharCharFilter {
        private CharCharExclusiveInclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(char value) {
            return CharComparisons.gt(value, lower) && CharComparisons.leq(value, upper);
        }
    }

    final static class CharCharExclusiveExclusiveFilter extends CharCharFilter {
        private CharCharExclusiveExclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(char value) {
            return CharComparisons.gt(value, lower) && CharComparisons.lt(value, upper);
        }
    }

    public static CharChunkFilter makeCharFilter(char lower, char upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new CharCharInclusiveInclusiveFilter(lower, upper);
            } else {
                return new CharCharInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new CharCharExclusiveInclusiveFilter(lower, upper);
            } else {
                return new CharCharExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
