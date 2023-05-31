/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

public class WritableSingleValueRowRedirection extends SingleValueRowRedirection {

    private long prevValue;
    private long updatedClockTick = 0;

    public WritableSingleValueRowRedirection(final long value) {
        super(value);
    }

    @Override
    public synchronized long get(long outerRowKey) {
        return value;
    }

    @Override
    public synchronized long getPrev(long outerRowKey) {
        if (updatedClockTick > 0) {
            if (updatedClockTick == ExecutionContext.getContext().getUpdateGraph().clock().currentStep()) {
                return prevValue;
            }
        }
        return value;
    }

    public synchronized void setValue(long newValue) {
        final long currentStep = ExecutionContext.getContext().getUpdateGraph().clock().currentStep();
        if (updatedClockTick > 0 && updatedClockTick != currentStep) {
            prevValue = value;
            updatedClockTick = currentStep;
        }
        value = newValue;
    }

    @Override
    public long getValue() {
        return value;
    }

    public void startTrackingPrevValues() {
        prevValue = value;
        updatedClockTick = ExecutionContext.getContext().getUpdateGraph().clock().currentStep();
    }

    @Override
    public String toString() {
        return "SingleValueRowRedirectionImpl{" + value + "}";
    }

    @Override
    public void fillPrevChunk(
            @NotNull FillContext fillContext,
            @NotNull WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull RowSequence outerRowKeys) {
        final long fillValue = (updatedClockTick > 0 &&
                updatedClockTick == ExecutionContext.getContext().getUpdateGraph().clock().currentStep()) ? prevValue
                        : value;
        final int sz = outerRowKeys.intSize();
        innerRowKeys.setSize(sz);
        innerRowKeys.asWritableLongChunk().fillWithValue(0, sz, fillValue);
    }
}
