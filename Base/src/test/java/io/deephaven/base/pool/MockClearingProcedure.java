//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.pool;

import io.deephaven.base.testing.RecordingMockObject;

import java.util.function.Consumer;

// --------------------------------------------------------------------
/**
 * Mock clearing procedure.
 */
public class MockClearingProcedure<T> extends RecordingMockObject implements Consumer<T> {

    // ----------------------------------------------------------------
    @Override
    public void accept(T arg) {
        recordActivity("accept(" + arg + ")");
    }
}
