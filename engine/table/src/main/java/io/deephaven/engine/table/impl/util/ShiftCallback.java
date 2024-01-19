package io.deephaven.engine.table.impl.util;

public interface ShiftCallback {
    void shift(long start, long end, long offset);
}
