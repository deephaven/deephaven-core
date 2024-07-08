//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

public interface ShiftCallback {
    void shift(long start, long end, long offset);
}
