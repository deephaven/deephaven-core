package io.deephaven.db.v2;

import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import junit.framework.TestCase;

class ErrorListener extends InstrumentedShiftAwareListenerAdapter {
    Throwable originalException;

    ErrorListener(DynamicTable table) {
        super("Error Checker", table, false);
    }

    @Override
    public void onUpdate(final Update upstream) {
        TestCase.fail("Should not have gotten an update!");
    }

    @Override
    public void onFailureInternal(Throwable originalException,
        UpdatePerformanceTracker.Entry sourceEntry) {
        this.originalException = originalException;
    }
}
