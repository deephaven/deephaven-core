/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import org.jetbrains.annotations.NotNull;

public class TickingSingleValueRedirectionIndexImpl implements SingleValueRedirectionIndex {
    private long value;
    private long prevValue;
    private long updatedClockTick = 0;

    public TickingSingleValueRedirectionIndexImpl(final long value) {
        this.value = value;
    }

    @Override
    public synchronized long put(long key, long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized long get(long key) {
        return value;
    }

    @Override
    public synchronized long getPrev(long key) {
        if (updatedClockTick > 0 && updatedClockTick == LogicalClock.DEFAULT.currentStep()) {
            return prevValue;
        }
        return value;
    }

    @Override
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

    @Override
    public void startTrackingPrevValues() {
        prevValue = value;
        updatedClockTick = LogicalClock.DEFAULT.currentStep();
    }

    @Override
    public synchronized long remove(long leftIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "SingleValueRedirectionIndexImpl{" + value + "}";
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableLongChunk<Attributes.KeyIndices> mappedKeysOut,
            @NotNull final OrderedKeys keysToMap) {
        final int sz = keysToMap.intSize();
        mappedKeysOut.setSize(sz);
        mappedKeysOut.fillWithValue(0, sz, value);
    }

    @Override
    public void fillPrevChunk(
            @NotNull FillContext fillContext,
            @NotNull WritableLongChunk<Attributes.KeyIndices> mappedKeysOut,
            @NotNull OrderedKeys keysToMap) {
        final long fillValue =
                (updatedClockTick > 0 && updatedClockTick == LogicalClock.DEFAULT.currentStep()) ? prevValue : value;
        final int sz = keysToMap.intSize();
        mappedKeysOut.setSize(sz);
        mappedKeysOut.fillWithValue(0, sz, fillValue);
    }
}
