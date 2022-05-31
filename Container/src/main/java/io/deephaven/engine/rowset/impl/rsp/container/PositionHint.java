package io.deephaven.engine.rowset.impl.rsp.container;

public final class PositionHint extends MutableInteger {
    public PositionHint() {
        super(-1);
    }

    public PositionHint(final int v) {
        super(v);
    }

    public boolean isValid() {
        return value >= 0;
    }

    public void reset() {
        value = -1;
    }

    public static void resetIfNotNull(final PositionHint positionHint) {
        if (positionHint == null) {
            return;
        }
        positionHint.reset();
    }
}
