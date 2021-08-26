package io.deephaven.db.v2.by;

import io.deephaven.db.tables.utils.TableToolsShowControl;
import io.deephaven.db.v2.sources.LogicalClock;
import org.jetbrains.annotations.NotNull;

@TableToolsShowControl(getWidth = 40)
class AvgState implements PreviousStateProvider<AvgState> {
    AvgState(boolean previous) {
        if (!previous) {
            prevValue = createPrev();
            changeTime = -1;
        } else {
            prevValue = null;
            changeTime = -1;
        }
    }

    @NotNull
    AvgState createPrev() {
        return new AvgState(true);
    }

    // only used in the current state
    private long changeTime;
    private final AvgState prevValue;

    protected double runningSum;
    protected long nonNullCount;

    double currentValue() {
        return runningSum / nonNullCount;
    }

    @Override
    public AvgState prev() {
        return prevValue;
    }

    void checkUpdates() {
        final long currentStep = LogicalClock.DEFAULT.currentStep();
        if (changeTime != currentStep) {
            savePrevious();
            changeTime = currentStep;
        }
    }

    void savePrevious() {
        prev().runningSum = runningSum;
        prev().nonNullCount = nonNullCount;
    }

    @Override
    public String toString() {
        return "Avg{" +
                "sum=" + runningSum +
                ", nonNull=" + nonNullCount +
                '}';
    }
}
