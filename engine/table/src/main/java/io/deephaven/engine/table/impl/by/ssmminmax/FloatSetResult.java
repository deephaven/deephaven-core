//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSetResult and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmminmax;

import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.engine.table.impl.ssms.FloatSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatSetResult implements SsmChunkedMinMaxOperator.SetResult {
    private final boolean minimum;
    private final FloatArraySource resultColumn;

    public FloatSetResult(boolean minimum, WritableColumnSource resultColumn) {
        this.minimum = minimum;
        this.resultColumn = (FloatArraySource) resultColumn;
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssm, long destination) {
        final float newResult;
        if (ssm.size() == 0) {
            newResult = NULL_FLOAT;
        } else {
            final FloatSegmentedSortedMultiset floatSsm = (FloatSegmentedSortedMultiset) ssm;
            newResult = minimum ? floatSsm.getMinFloat() : floatSsm.getMaxFloat();
        }
        return setResult(destination, newResult);
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, NULL_FLOAT);
    }

    private boolean setResult(long destination, float newResult) {
        final float oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }
}
