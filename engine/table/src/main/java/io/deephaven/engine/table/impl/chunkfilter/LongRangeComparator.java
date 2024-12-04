//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.LongComparisons;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;

public class LongRangeComparator {
    private LongRangeComparator() {} // static use only

    private abstract static class LongLongFilter implements ChunkFilter.LongChunkFilter {
        final long lower;
        final long upper;

        LongLongFilter(long lower, long upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    static class LongLongInclusiveInclusiveFilter extends LongLongFilter {
        private LongLongInclusiveInclusiveFilter(long lower, long upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(long value) {
            return LongComparisons.geq(value, lower) && LongComparisons.leq(value, upper);
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
            final LongChunk<? extends Values> longChunk = values.asLongChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(longChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class LongLongInclusiveExclusiveFilter extends LongLongFilter {
        private LongLongInclusiveExclusiveFilter(long lower, long upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(long value) {
            return LongComparisons.geq(value, lower) && LongComparisons.lt(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final LongChunk<? extends Values> longChunk = values.asLongChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(longChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class LongLongExclusiveInclusiveFilter extends LongLongFilter {
        private LongLongExclusiveInclusiveFilter(long lower, long upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(long value) {
            return LongComparisons.gt(value, lower) && LongComparisons.leq(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final LongChunk<? extends Values> longChunk = values.asLongChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(longChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class LongLongExclusiveExclusiveFilter extends LongLongFilter {
        private LongLongExclusiveExclusiveFilter(long lower, long upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(long value) {
            return LongComparisons.gt(value, lower) && LongComparisons.lt(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final LongChunk<? extends Values> longChunk = values.asLongChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(longChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    public static ChunkFilter.LongChunkFilter makeLongFilter(long lower, long upper, boolean lowerInclusive,
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
