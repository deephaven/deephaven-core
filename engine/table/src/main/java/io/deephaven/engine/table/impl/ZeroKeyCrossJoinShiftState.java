package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.updategraph.LogicalClock;

public class ZeroKeyCrossJoinShiftState extends CrossJoinShiftState {
    private boolean rightEmpty;
    private volatile boolean prevRightEmpty;
    private volatile long emptyChangeStep = -1;
    private boolean isTrackingPrev = false;

    public ZeroKeyCrossJoinShiftState(int numInitialShiftBits, boolean allowRightSideNulls) {
        super(numInitialShiftBits, allowRightSideNulls);
    }

    void startTrackingPrevious() {
        isTrackingPrev = true;
    }

    void setRightEmpty(boolean rightEmpty) {
        if (isTrackingPrev) {
            this.prevRightEmpty = this.rightEmpty;
            final long currentStep = LogicalClock.DEFAULT.currentStep();
            Assert.lt(emptyChangeStep, "emptyChangeStep", currentStep, "currentStep");
            this.emptyChangeStep = currentStep;
        }
        this.rightEmpty = rightEmpty;
    }

    public boolean rightEmpty() {
        return rightEmpty;
    }

    public boolean rightEmptyPrev() {
        if (emptyChangeStep != -1 && emptyChangeStep == LogicalClock.DEFAULT.currentStep()) {
            return prevRightEmpty;
        }
        return rightEmpty;
    }
}
