//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;

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
            final long currentStep = ExecutionContext.getContext().getUpdateGraph().clock().currentStep();
            Assert.lt(emptyChangeStep, "emptyChangeStep", currentStep, "currentStep");
            this.emptyChangeStep = currentStep;
        }
        this.rightEmpty = rightEmpty;
    }

    public boolean rightEmpty() {
        return rightEmpty;
    }

    public boolean rightEmptyPrev() {
        if (emptyChangeStep != -1
                && emptyChangeStep == ExecutionContext.getContext().getUpdateGraph().clock().currentStep()) {
            return prevRightEmpty;
        }
        return rightEmpty;
    }
}
