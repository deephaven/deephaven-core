/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRangeComparator and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.select.chunkfilters;

import io.deephaven.engine.util.DhIntComparisons;
import io.deephaven.engine.v2.select.ChunkFilter;
import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.OrderedRowKeys;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;

public class IntRangeComparator {
    private IntRangeComparator() {} // static use only

    private abstract static class IntIntFilter implements ChunkFilter.IntChunkFilter {
        final int lower;
        final int upper;

        IntIntFilter(int lower, int upper) {
            this.lower = lower;
            this.upper = upper;
        }

        abstract public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results);
    }

    static class IntIntInclusiveInclusiveFilter extends IntIntFilter {
        private IntIntInclusiveInclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final int value = values.get(ii);
                if (DhIntComparisons.geq(value, lower) && DhIntComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class IntIntInclusiveExclusiveFilter extends IntIntFilter {
        private IntIntInclusiveExclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final int value = values.get(ii);
                if (DhIntComparisons.geq(value, lower) && DhIntComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class IntIntExclusiveInclusiveFilter extends IntIntFilter {
        private IntIntExclusiveInclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<Attributes.OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final int value = values.get(ii);
                if (DhIntComparisons.gt(value, lower) && DhIntComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class IntIntExclusiveExclusiveFilter extends IntIntFilter {
        private IntIntExclusiveExclusiveFilter(int lower, int upper) {
            super(lower, upper);
        }

        public void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final int value = values.get(ii);
                if (DhIntComparisons.gt(value, lower) && DhIntComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    public static ChunkFilter.IntChunkFilter makeIntFilter(int lower, int upper, boolean lowerInclusive, boolean upperInclusive) {
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
