//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper;

import java.lang.reflect.Field;

/**
 * Shared pieces of {@link AggregationBuildBenchmark} and {@link AggregationIncrementalBenchmark}.
 */
final class AggregationStateBenchSupport {
    private AggregationStateBenchSupport() {}

    /**
     * Select whether refreshing aggregations reclaim the states of removed keys.
     *
     * <p>
     * {@code ChunkedOperatorAggregationHelper.RECLAIM_STATES} is read reflectively so that these benchmarks also
     * compile and run against a build that predates it; such a build supports only {@code reclaimStates == false}.
     * </p>
     *
     * @param reclaimStates whether to reclaim removed states
     */
    static void setReclaimStates(final boolean reclaimStates) {
        final Field field;
        try {
            field = ChunkedOperatorAggregationHelper.class.getField("RECLAIM_STATES");
        } catch (NoSuchFieldException e) {
            if (reclaimStates) {
                throw new IllegalStateException("This build cannot reclaim aggregation states", e);
            }
            return;
        }
        try {
            field.setBoolean(null, reclaimStates);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
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
