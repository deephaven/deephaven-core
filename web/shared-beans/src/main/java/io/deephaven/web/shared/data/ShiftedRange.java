//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.data;

import java.io.Serializable;

public class ShiftedRange implements Serializable {
    private Range range;
    private long delta;

    public ShiftedRange() {}

    public ShiftedRange(Range range, long delta) {
        setRange(range);
        setDelta(delta);
    }

    public Range getRange() {
        return range;
    }

    public void setRange(final Range range) {
        this.range = range;
    }

    public long getDelta() {
        return delta;
    }

    public void setDelta(final long delta) {
        this.delta = delta;
    }

    public Range getResultRange() {
        return range.shift(delta);
    }

    @Override
    public String toString() {
        return "ShiftedRange{" +
                "range=" + range +
                ", delta=" + delta +
                '}';
    }
}
