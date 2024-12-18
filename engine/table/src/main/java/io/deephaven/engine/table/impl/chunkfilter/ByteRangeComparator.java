//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.ByteComparisons;

public class ByteRangeComparator {
    private ByteRangeComparator() {} // static use only

    private abstract static class ByteByteFilter extends ByteChunkFilter {
        final byte lower;
        final byte upper;

        ByteByteFilter(byte lower, byte upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    final static class ByteByteInclusiveInclusiveFilter extends ByteByteFilter {
        private ByteByteInclusiveInclusiveFilter(byte lower, byte upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(byte value) {
            return ByteComparisons.geq(value, lower) && ByteComparisons.leq(value, upper);
        }
    }

    final static class ByteByteInclusiveExclusiveFilter extends ByteByteFilter {
        private ByteByteInclusiveExclusiveFilter(byte lower, byte upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(byte value) {
            return ByteComparisons.geq(value, lower) && ByteComparisons.lt(value, upper);
        }
    }

    final static class ByteByteExclusiveInclusiveFilter extends ByteByteFilter {
        private ByteByteExclusiveInclusiveFilter(byte lower, byte upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(byte value) {
            return ByteComparisons.gt(value, lower) && ByteComparisons.leq(value, upper);
        }
    }

    final static class ByteByteExclusiveExclusiveFilter extends ByteByteFilter {
        private ByteByteExclusiveExclusiveFilter(byte lower, byte upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(byte value) {
            return ByteComparisons.gt(value, lower) && ByteComparisons.lt(value, upper);
        }
    }

    public static ByteChunkFilter makeByteFilter(byte lower, byte upper, boolean lowerInclusive,
            boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new ByteByteInclusiveInclusiveFilter(lower, upper);
            } else {
                return new ByteByteInclusiveExclusiveFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new ByteByteExclusiveInclusiveFilter(lower, upper);
            } else {
                return new ByteByteExclusiveExclusiveFilter(lower, upper);
            }
        }
    }
}
