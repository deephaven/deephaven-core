//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSetResult and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmminmax;

import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.ByteArraySource;
import io.deephaven.engine.table.impl.ssms.ByteSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

public class ByteSetResult implements SsmChunkedMinMaxOperator.SetResult {
    private final boolean minimum;
    private final ByteArraySource resultColumn;

    public ByteSetResult(boolean minimum, WritableColumnSource resultColumn) {
        this.minimum = minimum;
        this.resultColumn = (ByteArraySource) resultColumn;
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssm, long destination) {
        final byte newResult;
        if (ssm.size() == 0) {
            newResult = NULL_BYTE;
        } else {
            final ByteSegmentedSortedMultiset byteSsm = (ByteSegmentedSortedMultiset) ssm;
            newResult = minimum ? byteSsm.getMinByte() : byteSsm.getMaxByte();
        }
        return setResult(destination, newResult);
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, NULL_BYTE);
    }

    private boolean setResult(long destination, byte newResult) {
        final byte oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }
}
