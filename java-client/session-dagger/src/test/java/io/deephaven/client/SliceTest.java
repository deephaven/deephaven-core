//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class SliceTest extends DeephavenSessionTestBase {

    private static final TableSpec STATIC_BASE = TableSpec.empty(100).view("I=ii");
    private static final TableSpec TICKING_BASE = TimeTable.of(Duration.ofMillis(100)).view("I=ii");

    @Test
    public void bothNonNegativeStartBeforeEnd() throws InterruptedException, TableHandleException {
        allow(0, 50);
        allow(25, 75);
    }

    @Test
    public void bothNonNegativeStartAfterEnd() throws IllegalArgumentException {
        disallow(50, 0);
    }

    @Test
    public void bothNegativeStartBeforeEnd() throws InterruptedException, TableHandleException {
        allow(-50, -25);
    }

    @Test
    public void bothNegativeStartAfterEnd() throws IllegalArgumentException {
        disallow(-25, -50);
    }

    @Test
    public void diffSignStartBeforeEnd() throws InterruptedException, TableHandleException {
        allow(-25, 25);
    }

    @Test
    public void diffSignStartAfterEnd() throws InterruptedException, TableHandleException {
        allow(25, -25);
    }

    @Test
    public void startZeroEndNegative() throws InterruptedException, TableHandleException {
        allow(0, -25);
    }

    private void allow(long start, long end)
            throws InterruptedException, TableHandleException {
        try (final TableHandle handle = session.batch().execute(STATIC_BASE.slice(start, end))) {
            assertThat(handle.isSuccessful()).isTrue();
        }
        try (final TableHandle handle = session.batch().execute(TICKING_BASE.slice(start, end))) {
            assertThat(handle.isSuccessful()).isTrue();
        }
    }

    private void disallow(long start, long end) throws IllegalArgumentException {
        try {
            STATIC_BASE.slice(start, end);
            failBecauseExceptionWasNotThrown(TableHandle.TableHandleException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            TICKING_BASE.slice(start, end);
            failBecauseExceptionWasNotThrown(TableHandle.TableHandleException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

}
