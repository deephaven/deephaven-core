//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * A listener that records every failure delivered to it, so a test can assert that a result failed exactly once and
 * with the error that actually occurred upstream. Updates are ignored.
 * <p>
 * Tables hold their listeners weakly, so the test must keep a strong reference to this listener for as long as it
 * expects deliveries.
 */
public final class FailureRecordingListener extends InstrumentedTableUpdateListenerAdapter {

    private final List<Throwable> failures = new ArrayList<>();

    public FailureRecordingListener(@NotNull final Table result) {
        super("failure recorder", result, false);
        result.addUpdateListener(this);
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {}

    @Override
    public void onFailureInternal(final Throwable originalException, final Entry sourceEntry) {
        failures.add(originalException);
    }

    /**
     * @return How many failures have been delivered so far
     */
    public int failureCount() {
        return failures.size();
    }

    /**
     * Assert that exactly one failure was delivered, and that it was {@code expected} itself rather than a wrapper or
     * an unrelated error raised while propagating it.
     */
    public void assertFailedOnceWith(@NotNull final Throwable expected) {
        assertEquals("failures delivered: " + failures, 1, failures.size());
        assertSame(expected, failures.get(0));
    }
}
