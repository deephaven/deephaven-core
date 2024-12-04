//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRangeComparator and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.ByteComparisons;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;

public class ByteRangeComparator {
    private ByteRangeComparator() {} // static use only

    private abstract static class ByteByteFilter implements ChunkFilter.ByteChunkFilter {
        final byte lower;
        final byte upper;

        ByteByteFilter(byte lower, byte upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    static class ByteByteInclusiveInclusiveFilter extends ByteByteFilter {
        private ByteByteInclusiveInclusiveFilter(byte lower, byte upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(byte value) {
            return ByteComparisons.geq(value, lower) && ByteComparisons.leq(value, upper);
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
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(byteChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ByteByteInclusiveExclusiveFilter extends ByteByteFilter {
        private ByteByteInclusiveExclusiveFilter(byte lower, byte upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(byte value) {
            return ByteComparisons.geq(value, lower) && ByteComparisons.lt(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(byteChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ByteByteExclusiveInclusiveFilter extends ByteByteFilter {
        private ByteByteExclusiveInclusiveFilter(byte lower, byte upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(byte value) {
            return ByteComparisons.gt(value, lower) && ByteComparisons.leq(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(byteChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ByteByteExclusiveExclusiveFilter extends ByteByteFilter {
        private ByteByteExclusiveExclusiveFilter(byte lower, byte upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(byte value) {
            return ByteComparisons.gt(value, lower) && ByteComparisons.lt(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(byteChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    public static ChunkFilter.ByteChunkFilter makeByteFilter(byte lower, byte upper, boolean lowerInclusive,
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
