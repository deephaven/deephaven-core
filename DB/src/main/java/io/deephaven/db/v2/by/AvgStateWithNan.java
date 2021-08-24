package io.deephaven.db.v2.by;

import io.deephaven.db.tables.utils.TableToolsShowControl;
import org.jetbrains.annotations.NotNull;

@TableToolsShowControl(getWidth = 40)
class AvgStateWithNan extends AvgState {
    protected long nanCount;

    AvgStateWithNan(boolean previous) {
        super(previous);
    }

    @NotNull
    @Override
    AvgStateWithNan createPrev() {
        return new AvgStateWithNan(true);
    }

    double currentValue() {
        if (nanCount > 0) {
            return Double.NaN;
        }
        return runningSum / nonNullCount;
    }

    @Override
    void savePrevious() {
        super.savePrevious();
        ((AvgStateWithNan) prev()).nanCount = nanCount;
    }

    @Override
    public String toString() {
        return "Avg{" +
            "sum=" + runningSum +
            ", nonNull=" + nonNullCount +
            ", nan=" + nanCount +
            '}';
    }
}
