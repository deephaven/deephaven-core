package io.deephaven.engine.v2;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import junit.framework.TestCase;

public class ErrorListener extends InstrumentedTableUpdateListenerAdapter {
    Throwable originalException;

    ErrorListener(Table table) {
        super("Error Checker", table, false);
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {
        TestCase.fail("Should not have gotten an update!");
    }

    @Override
    public void onFailureInternal(Throwable originalException, UpdatePerformanceTracker.Entry sourceEntry) {
        this.originalException = originalException;
    }
}
