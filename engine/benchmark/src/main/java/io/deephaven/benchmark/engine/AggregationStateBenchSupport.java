//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper;
import io.deephaven.engine.table.impl.by.IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones;

/**
 * Shared pieces of {@link AggregationBuildBenchmark} and {@link AggregationIncrementalBenchmark}.
 */
final class AggregationStateBenchSupport {
    private AggregationStateBenchSupport() {}

    /**
     * Select how refreshing aggregations reclaim the states of removed keys: {@code none} keeps every state, and
     * {@code blocks} releases whole blocks of empty states in place.
     *
     * @param reclaim the reclaim mode
     */
    static void setReclaimMode(final String reclaim) {
        switch (reclaim) {
            case "none":
                ChunkedOperatorAggregationHelper.RECLAIM_STATES = false;
                break;
            case "blocks":
                ChunkedOperatorAggregationHelper.RECLAIM_STATES = true;
                break;
            default:
                throw new IllegalArgumentException("Unknown reclaim mode " + reclaim);
        }
    }

    /**
     * Set the fraction free at which blocks of output positions are collapsed; 1 disables collapsing.
     *
     * @param collapseFreeFraction the fraction free at which a block is collapsed
     */
    static void setCollapseFreeFraction(final double collapseFreeFraction) {
        ChunkedOperatorAggregationHelper.COLLAPSE_FREE_FRACTION = collapseFreeFraction;
    }

    /**
     * @return the number of hash table rehashes begun so far by aggregations that reclaim states
     */
    static long rehashCount() {
        return IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones.REHASH_COUNT.sum();
    }

    /**
     * A long column whose value is a pure function of the row key, so that adding or removing rows costs nothing in the
     * column itself. The value at {@code rowKey} is {@code rowKey / rowsPerValue}, reduced modulo {@code valueSpace}
     * when {@code valueSpace} is positive.
     */
    static final class ComputedLongSource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private final long rowsPerValue;
        private final long valueSpace;

        ComputedLongSource(final long rowsPerValue, final long valueSpace) {
            super(long.class);
            this.rowsPerValue = rowsPerValue;
            this.valueSpace = valueSpace;
        }

        @Override
        public long getLong(final long rowKey) {
            final long value = rowKey / rowsPerValue;
            return valueSpace > 0 ? value % valueSpace : value;
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return getLong(rowKey);
        }

        @Override
        public boolean isImmutable() {
            return false;
        }

        @Override
        public boolean isStateless() {
            return true;
        }
    }
}
