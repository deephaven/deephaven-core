//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.updategraph.UpdateGraph;
import org.jetbrains.annotations.NotNull;

public class WritableSingleValueRowRedirection extends SingleValueRowRedirection {

    private long prevValue;
    private long updatedClockTick = 0;

    private final UpdateGraph updateGraph;

    public WritableSingleValueRowRedirection(final long value) {
        super(value);
        updateGraph = ExecutionContext.getContext().getUpdateGraph();
    }

    @Override
    public synchronized long get(long outerRowKey) {
        return value;
    }

    @Override
    public synchronized long getPrev(long outerRowKey) {
        if (updatedClockTick > 0) {
            if (updatedClockTick == updateGraph.clock().currentStep()) {
                return prevValue;
            }
        }
        return value;
    }

    public synchronized void setValue(long newValue) {
        final long currentStep = updateGraph.clock().currentStep();
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
        updatedClockTick = updateGraph.clock().currentStep();
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
        final long fillValue = (updatedClockTick > 0 && updatedClockTick == updateGraph.clock().currentStep())
                ? prevValue
                : value;
        final int sz = outerRowKeys.intSize();
        innerRowKeys.setSize(sz);
        innerRowKeys.asWritableLongChunk().fillWithValue(0, sz, fillValue);
    }
}
