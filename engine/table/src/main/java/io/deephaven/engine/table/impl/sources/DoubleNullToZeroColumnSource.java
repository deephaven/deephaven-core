package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.util.QueryConstants;

/**
 * If you want to expose the internal state of an aggregation and compare it, then the new tables might have nulls where
 * the old tables have zero.  This wrapper prevents that spurious comparison failure.
 */
@SuppressWarnings("unused")
public class DoubleNullToZeroColumnSource extends AbstractColumnSource<Double> implements MutableColumnSourceGetDefaults.ForDouble {
    private final DoubleArraySource column;

    private DoubleNullToZeroColumnSource(DoubleArraySource column) {
        super(double.class);
        this.column = column;
    }

    @Override
    public void startTrackingPrevValues() {
        // Nothing to do.
    }

    @Override
    public double getDouble(long index) {
        final double value = column.getDouble(index);
        return nullToZero(value);
    }

    @Override
    public double getPrevDouble(long index) {
        final double value = column.getPrevDouble(index);
        return nullToZero(value);
    }

    private static double nullToZero(double value) {
        return value == QueryConstants.NULL_DOUBLE ? 0 : value;
    }
}
