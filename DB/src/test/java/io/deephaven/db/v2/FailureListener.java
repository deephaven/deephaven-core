package io.deephaven.db.v2;

import junit.framework.TestCase;

public class FailureListener extends InstrumentedShiftAwareListener {
    public FailureListener() {
        super("Fuzzer Failure Listener");
    }

    @Override
    public void onUpdate(final io.deephaven.db.v2.ShiftAwareListener.Update upstream) {}

    @Override
    public void onFailureInternal(Throwable originalException,
        io.deephaven.db.v2.utils.UpdatePerformanceTracker.Entry sourceEntry) {
        originalException.printStackTrace();
        TestCase.fail(originalException.getMessage());
    }
}
