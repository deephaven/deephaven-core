/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRangeComparator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
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

        abstract public void filter(ByteChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results);
    }

    static class ByteByteInclusiveInclusiveFilter extends ByteByteFilter {
        private ByteByteInclusiveInclusiveFilter(byte lower, byte upper) {
            super(lower, upper);
        }

        public void filter(ByteChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final byte value = values.get(ii);
                if (ByteComparisons.geq(value, lower) && ByteComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ByteByteInclusiveExclusiveFilter extends ByteByteFilter {
        private ByteByteInclusiveExclusiveFilter(byte lower, byte upper) {
            super(lower, upper);
        }

        public void filter(ByteChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final byte value = values.get(ii);
                if (ByteComparisons.geq(value, lower) && ByteComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ByteByteExclusiveInclusiveFilter extends ByteByteFilter {
        private ByteByteExclusiveInclusiveFilter(byte lower, byte upper) {
            super(lower, upper);
        }

        public void filter(ByteChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final byte value = values.get(ii);
                if (ByteComparisons.gt(value, lower) && ByteComparisons.leq(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    static class ByteByteExclusiveExclusiveFilter extends ByteByteFilter {
        private ByteByteExclusiveExclusiveFilter(byte lower, byte upper) {
            super(lower, upper);
        }

        public void filter(ByteChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys, WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ii = 0; ii < values.size(); ++ii) {
                final byte value = values.get(ii);
                if (ByteComparisons.gt(value, lower) && ByteComparisons.lt(value, upper)) {
                    results.add(keys.get(ii));
                }
            }
        }
    }

    public static ChunkFilter.ByteChunkFilter makeByteFilter(byte lower, byte upper, boolean lowerInclusive, boolean upperInclusive) {
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
