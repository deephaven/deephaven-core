package io.deephaven.db.v2.by;

import io.deephaven.db.tables.utils.TableToolsShowControl;
import org.jetbrains.annotations.NotNull;

@TableToolsShowControl(getWidth = 40)
class StdStateWithNan extends StdState {
    protected long nanCount;

    StdStateWithNan(boolean previous) {
        super(previous);
    }

    @NotNull
    @Override
    StdStateWithNan createPrev() {
        return new StdStateWithNan(true);
    }

    double currentValue() {
        if (nanCount > 0) {
            return Double.NaN;
        }
        return Math.sqrt(sum2 / (nonNullCount - 1) - sum * sum / nonNullCount / (nonNullCount - 1));
    }

    @Override
    void savePrevious() {
        super.savePrevious();
        ((StdStateWithNan) prev()).nanCount = nanCount;
    }

    @Override
    public String toString() {
        return "Std{" + "sum=" + sum + ", sum2=" + sum2 + ", nan=" + nanCount + ", nonNull=" + nonNullCount + '}';
    }
}
