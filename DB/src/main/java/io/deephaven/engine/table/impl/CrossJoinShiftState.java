/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.updategraph.LogicalClock;

/**
 * Shift state used by the {@link io.deephaven.engine.table.impl.sources.BitShiftingColumnSource}.
 */
public class CrossJoinShiftState {
    private int numShiftBits;
    private int prevNumShiftBits;
    private long mask;
    private long prevMask;
    private long updatedClockTick = 0;

    public CrossJoinShiftState(final int numInitialShiftBits) {
        setNumShiftBits(numInitialShiftBits);
    }

    void setNumShiftBits(final int newNumShiftBits) {
        Assert.lt(newNumShiftBits, "newNumShiftBits", 63, "63");
        Assert.gt(newNumShiftBits, "newNumShiftBits", 0, "0");
        this.numShiftBits = newNumShiftBits;
        this.mask = (1L << newNumShiftBits) - 1;
    }

    void setNumShiftBitsAndUpdatePrev(final int newNumShiftBits) {
        Assert.lt(newNumShiftBits, "newNumShiftBits", 63, "63");
        Assert.gt(newNumShiftBits, "newNumShiftBits", 0, "0");

        final long currentStep = LogicalClock.DEFAULT.currentStep();
        if (updatedClockTick != currentStep) {
            prevMask = mask;
            prevNumShiftBits = numShiftBits;
            updatedClockTick = currentStep;
        }
        this.numShiftBits = newNumShiftBits;
        this.mask = (1L << newNumShiftBits) - 1;
    }

    public int getNumShiftBits() {
        return numShiftBits;
    }

    public int getPrevNumShiftBits() {
        if (updatedClockTick > 0) {
            if (updatedClockTick == LogicalClock.DEFAULT.currentStep()) {
                return prevNumShiftBits;
            }
            updatedClockTick = 0;
        }
        return numShiftBits;
    }

    public long getShifted(long index) {
        return index >> getNumShiftBits();
    }

    public long getPrevShifted(long index) {
        return index >> getPrevNumShiftBits();
    }

    public long getMasked(long index) {
        return index & getMask();
    }

    public long getPrevMasked(long index) {
        return index & getPrevMask();
    }

    private long getMask() {
        return mask;
    }

    private long getPrevMask() {
        if (updatedClockTick > 0) {
            if (updatedClockTick == LogicalClock.DEFAULT.currentStep()) {
                return prevMask;
            }
            updatedClockTick = 0;
        }
        return mask;
    }

    static int getMinBits(final QueryTable table) {
        return getMinBits(table.getRowSet().lastRowKey());
    }

    static int getMinBits(final long lastKey) {
        return 64 - Long.numberOfLeadingZeros(Math.max(0, lastKey));
    }
}
