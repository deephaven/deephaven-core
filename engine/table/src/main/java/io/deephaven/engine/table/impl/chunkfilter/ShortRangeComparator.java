//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.ShortComparisons;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;

public class ShortRangeComparator {
    private ShortRangeComparator() {} // static use only

    private abstract static class ShortShortFilter implements ChunkFilter.ShortChunkFilter {
        final short lower;
        final short upper;

        ShortShortFilter(short lower, short upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    static class ShortShortInclusiveInclusiveFilter extends ShortShortFilter {
        private ShortShortInclusiveInclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.geq(value, lower) && ShortComparisons.leq(value, upper);
        }

        /*
         * NOTE: this method is identically repeated for every class below. This is to allow a single virtual lookup
         * per filtered chunk, rather than making virtual calls to matches() for every value in the chunk. This
         * is a performance optimization that helps at least on JVM <= 21. It may not be always necessary on newer JVMs.
         */
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(shortChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ShortShortInclusiveExclusiveFilter extends ShortShortFilter {
        private ShortShortInclusiveExclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.geq(value, lower) && ShortComparisons.lt(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(shortChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ShortShortExclusiveInclusiveFilter extends ShortShortFilter {
        private ShortShortExclusiveInclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.gt(value, lower) && ShortComparisons.leq(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(shortChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ShortShortExclusiveExclusiveFilter extends ShortShortFilter {
        private ShortShortExclusiveExclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(short value) {
            return ShortComparisons.gt(value, lower) && ShortComparisons.lt(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(shortChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    public static ChunkFilter.ShortChunkFilter makeShortFilter(short lower, short upper, boolean lowerInclusive,
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
