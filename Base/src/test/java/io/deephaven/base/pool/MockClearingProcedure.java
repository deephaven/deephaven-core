/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.pool;

import io.deephaven.base.Procedure;
import io.deephaven.base.testing.RecordingMockObject;

// --------------------------------------------------------------------
/**
 * Mock clearing procedure.
 */
public class MockClearingProcedure<T> extends RecordingMockObject implements Procedure.Unary<T> {

    // ----------------------------------------------------------------
    @Override // from Procedure.Unary
    public void call(T arg) {
        recordActivity("call(" + arg + ")");
    }
}
