package io.deephaven.db.v2.by;

import io.deephaven.db.tables.utils.TableToolsShowControl;
import io.deephaven.db.v2.sources.LogicalClock;
import org.jetbrains.annotations.NotNull;

@TableToolsShowControl(getWidth = 40)
class VarState implements PreviousStateProvider<VarState> {
    VarState(boolean previous) {
        if (!previous) {
            prevValue = createPrev();
            changeTime = -1;
        } else {
            prevValue = null;
            changeTime = -1;
        }
    }

    @NotNull
    VarState createPrev() {
        return new VarState(true);
    }

    // only used in the current state
    private long changeTime;
    private final VarState prevValue;

    protected double sum;
    protected double sum2;
    protected long nonNullCount;

    double currentValue() {
        return sum2 / (nonNullCount - 1) - sum * sum / nonNullCount / (nonNullCount - 1);
    }

    @Override
    public VarState prev() {
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
        prev().sum = sum;
        prev().sum2 = sum2;
        prev().nonNullCount = nonNullCount;
    }

    @Override
    public String toString() {
        return "Var{" +
                "sum=" + sum +
                ", sum2=" + sum2 +
                ", nonNull=" + nonNullCount +
                '}';
    }
}
