//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.compare.CharComparisons;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;

public class CharRangeComparator {
    private CharRangeComparator() {} // static use only

    private abstract static class CharCharFilter implements ChunkFilter.CharChunkFilter {
        final char lower;
        final char upper;

        CharCharFilter(char lower, char upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    static class CharCharInclusiveInclusiveFilter extends CharCharFilter {
        private CharCharInclusiveInclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(char value) {
            return CharComparisons.geq(value, lower) && CharComparisons.leq(value, upper);
        }

        /*
         * NOTE: this method is identically repeated for every class below. This is to allow a single virtual lookup per
         * filtered chunk, rather than making virtual calls to matches() for every value in the chunk. This is a
         * performance optimization that helps at least on JVM <= 21. It may not be always necessary on newer JVMs.
         */
        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class CharCharInclusiveExclusiveFilter extends CharCharFilter {
        private CharCharInclusiveExclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(char value) {
            return CharComparisons.geq(value, lower) && CharComparisons.lt(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class CharCharExclusiveInclusiveFilter extends CharCharFilter {
        private CharCharExclusiveInclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(char value) {
            return CharComparisons.gt(value, lower) && CharComparisons.leq(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class CharCharExclusiveExclusiveFilter extends CharCharFilter {
        private CharCharExclusiveExclusiveFilter(char lower, char upper) {
            super(lower, upper);
        }

        @Override
        public boolean matches(char value) {
            return CharComparisons.gt(value, lower) && CharComparisons.lt(value, upper);
        }

        @Override
        public void filter(
                final Chunk<? extends Values> values,
                final LongChunk<OrderedRowKeys> keys,
                final WritableLongChunk<OrderedRowKeys> results) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                if (matches(charChunk.get(ii))) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    public static ChunkFilter.CharChunkFilter makeCharFilter(char lower, char upper, boolean lowerInclusive,
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
