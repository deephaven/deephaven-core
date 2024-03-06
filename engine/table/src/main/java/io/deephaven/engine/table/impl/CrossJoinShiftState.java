//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.LogicalClock;

/**
 * Shift state used by the {@link io.deephaven.engine.table.impl.sources.BitShiftingColumnSource}.
 */
public class CrossJoinShiftState {
    private final LogicalClock clock;
    private final boolean leftOuterJoin;
    private int numShiftBits;
    private int prevNumShiftBits;
    private long mask;
    private long prevMask;
    private long updatedClockTick = 0;

    public CrossJoinShiftState(final int numInitialShiftBits, final boolean leftOuterJoin) {
        setNumShiftBits(numInitialShiftBits);
        this.clock = ExecutionContext.getContext().getUpdateGraph().clock();
        this.leftOuterJoin = leftOuterJoin;
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

        final long currentStep = clock.currentStep();
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
            if (updatedClockTick == clock.currentStep()) {
                return prevNumShiftBits;
            }
            updatedClockTick = 0;
        }
        return numShiftBits;
    }

    public boolean leftOuterJoin() {
        return leftOuterJoin;
    }

    public long getShifted(long rowKey) {
        return rowKey >> getNumShiftBits();
    }

    public long getPrevShifted(long rowKey) {
        return rowKey >> getPrevNumShiftBits();
    }

    public long getMasked(long rowKey) {
        return rowKey & getMask();
    }

    public long getPrevMasked(long rowKey) {
        return rowKey & getPrevMask();
    }

    private long getMask() {
        return mask;
    }

    private long getPrevMask() {
        if (updatedClockTick > 0) {
            if (updatedClockTick == clock.currentStep()) {
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
