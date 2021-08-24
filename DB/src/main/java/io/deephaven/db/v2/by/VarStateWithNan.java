package io.deephaven.db.v2.by;

import io.deephaven.db.tables.utils.TableToolsShowControl;
import org.jetbrains.annotations.NotNull;

@TableToolsShowControl(getWidth = 40)
class VarStateWithNan extends VarState {
    protected long nanCount;

    VarStateWithNan(boolean previous) {
        super(previous);
    }

    @NotNull
    @Override
    VarStateWithNan createPrev() {
        return new VarStateWithNan(true);
    }

    double currentValue() {
        if (nanCount > 0) {
            return Double.NaN;
        }
        return sum2 / (nonNullCount - 1) - sum * sum / nonNullCount / (nonNullCount - 1);
    }

    @Override
    void savePrevious() {
        super.savePrevious();
        ((VarStateWithNan) prev()).nanCount = nanCount;
    }

    @Override
    public String toString() {
        return "Var{" + "sum=" + sum + ", sum2=" + sum2 + ", nan=" + nanCount + ", nonNull="
            + nonNullCount + '}';
    }
}
