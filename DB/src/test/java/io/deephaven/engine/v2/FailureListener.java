package io.deephaven.engine.v2;

import junit.framework.TestCase;

public class FailureListener extends InstrumentedListener {
    public FailureListener() {
        super("Fuzzer Failure ShiftObliviousListener");
    }

    @Override
    public void onUpdate(final Listener.Update upstream) {}

    @Override
    public void onFailureInternal(Throwable originalException,
            io.deephaven.engine.v2.utils.UpdatePerformanceTracker.Entry sourceEntry) {
        originalException.printStackTrace();
        TestCase.fail(originalException.getMessage());
    }
}
