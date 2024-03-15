//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSetResult and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmminmax;

import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.ssms.DoubleSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleSetResult implements SsmChunkedMinMaxOperator.SetResult {
    private final boolean minimum;
    private final DoubleArraySource resultColumn;

    public DoubleSetResult(boolean minimum, WritableColumnSource resultColumn) {
        this.minimum = minimum;
        this.resultColumn = (DoubleArraySource) resultColumn;
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssm, long destination) {
        final double newResult;
        if (ssm.size() == 0) {
            newResult = NULL_DOUBLE;
        } else {
            final DoubleSegmentedSortedMultiset doubleSsm = (DoubleSegmentedSortedMultiset) ssm;
            newResult = minimum ? doubleSsm.getMinDouble() : doubleSsm.getMaxDouble();
        }
        return setResult(destination, newResult);
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, NULL_DOUBLE);
    }

    private boolean setResult(long destination, double newResult) {
        final double oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }
}
