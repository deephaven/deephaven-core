//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSetResult and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmminmax;

import java.util.Objects;
import io.deephaven.util.compare.ObjectComparisons;

import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.ssms.ObjectSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;


public class ObjectSetResult implements SsmChunkedMinMaxOperator.SetResult {
    private final boolean minimum;
    private final ObjectArraySource resultColumn;

    public ObjectSetResult(boolean minimum, WritableColumnSource resultColumn) {
        this.minimum = minimum;
        this.resultColumn = (ObjectArraySource) resultColumn;
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssm, long destination) {
        final Object newResult;
        if (ssm.size() == 0) {
            newResult = null;
        } else {
            final ObjectSegmentedSortedMultiset ObjectSsm = (ObjectSegmentedSortedMultiset) ssm;
            newResult = minimum ? ObjectSsm.getMinObject() : ObjectSsm.getMaxObject();
        }
        return setResult(destination, newResult);
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, null);
    }

    private boolean setResult(long destination, Object newResult) {
        final Object oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }
}
