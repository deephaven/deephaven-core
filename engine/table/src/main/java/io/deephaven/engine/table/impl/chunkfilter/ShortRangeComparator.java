/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRangeComparator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
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

        abstract public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results);
    }

    static class ShortShortInclusiveInclusiveFilter extends ShortShortFilter {
        private ShortShortInclusiveInclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final short value = values.get(ii);
                if (ShortComparisons.geq(value, lower) && ShortComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ShortShortInclusiveExclusiveFilter extends ShortShortFilter {
        private ShortShortInclusiveExclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final short value = values.get(ii);
                if (ShortComparisons.geq(value, lower) && ShortComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ShortShortExclusiveInclusiveFilter extends ShortShortFilter {
        private ShortShortExclusiveInclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final short value = values.get(ii);
                if (ShortComparisons.gt(value, lower) && ShortComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ShortShortExclusiveExclusiveFilter extends ShortShortFilter {
        private ShortShortExclusiveExclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final short value = values.get(ii);
                if (ShortComparisons.gt(value, lower) && ShortComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    public static ChunkFilter.ShortChunkFilter makeShortFilter(short lower, short upper, boolean lowerInclusive, boolean upperInclusive) {
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
