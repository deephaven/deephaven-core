/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.sources.LogicalClock;
import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.ChunkSource;
import io.deephaven.engine.v2.sources.chunk.WritableLongChunk;
import org.jetbrains.annotations.NotNull;

public class MutableSingleValueRowRedirection extends SingleValueRowRedirection {

    private long prevValue;
    private long updatedClockTick = 0;

    public MutableSingleValueRowRedirection(final long value) {
        super(value);
    }

    @Override
    public synchronized long get(long outerRowKey) {
        return value;
    }

    @Override
    public synchronized long getPrev(long outerRowKey) {
        if (updatedClockTick > 0 && updatedClockTick == LogicalClock.DEFAULT.currentStep()) {
            return prevValue;
        }
        return value;
    }

    public synchronized void setValue(long newValue) {
        final long currentStep = LogicalClock.DEFAULT.currentStep();
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
        updatedClockTick = LogicalClock.DEFAULT.currentStep();
    }

    @Override
    public String toString() {
        return "SingleValueRowRedirectionImpl{" + value + "}";
    }


    @Override
    public void fillPrevChunk(
            @NotNull ChunkSource.FillContext fillContext,
            @NotNull WritableLongChunk<? extends Attributes.RowKeys> innerRowKeys,
            @NotNull RowSequence outerRowKeys) {
        final long fillValue =
                (updatedClockTick > 0 && updatedClockTick == LogicalClock.DEFAULT.currentStep()) ? prevValue : value;
        final int sz = outerRowKeys.intSize();
        innerRowKeys.setSize(sz);
        innerRowKeys.fillWithValue(0, sz, fillValue);
    }
}
