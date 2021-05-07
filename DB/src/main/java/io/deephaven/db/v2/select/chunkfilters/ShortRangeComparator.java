/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRangeComparator and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.select.chunkfilters;

import io.deephaven.db.util.DhShortComparisons;
import io.deephaven.db.v2.select.ChunkFilter;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;

public class ShortRangeComparator {
    private ShortRangeComparator() {} // static use only

    private abstract static class ShortShortFilter implements ChunkFilter.ShortChunkFilter {
        final short lower;
        final short upper;

        ShortShortFilter(short lower, short upper) {
            this.lower = lower;
            this.upper = upper;
        }

        abstract public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys, WritableLongChunk<OrderedKeyIndices> results);
    }

    static class ShortShortInclusiveInclusiveFilter extends ShortShortFilter {
        private ShortShortInclusiveInclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys, WritableLongChunk<OrderedKeyIndices> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final short value = values.get(ii);
                if (DhShortComparisons.geq(value, lower) && DhShortComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ShortShortInclusiveExclusiveFilter extends ShortShortFilter {
        private ShortShortInclusiveExclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys, WritableLongChunk<OrderedKeyIndices> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final short value = values.get(ii);
                if (DhShortComparisons.geq(value, lower) && DhShortComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ShortShortExclusiveInclusiveFilter extends ShortShortFilter {
        private ShortShortExclusiveInclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys, WritableLongChunk<OrderedKeyIndices> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final short value = values.get(ii);
                if (DhShortComparisons.gt(value, lower) && DhShortComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ShortShortExclusiveExclusiveFilter extends ShortShortFilter {
        private ShortShortExclusiveExclusiveFilter(short lower, short upper) {
            super(lower, upper);
        }

        public void filter(ShortChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys, WritableLongChunk<OrderedKeyIndices> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final short value = values.get(ii);
                if (DhShortComparisons.gt(value, lower) && DhShortComparisons.lt(value, upper)) {
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
