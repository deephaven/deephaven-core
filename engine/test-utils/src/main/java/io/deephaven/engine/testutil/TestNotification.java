//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil;

import io.deephaven.engine.updategraph.AbstractNotification;

import static org.junit.Assert.*;

public class TestNotification extends AbstractNotification {

    private boolean invoked = false;

    public TestNotification() {
        super(false);
    }

    @Override
    public boolean canExecute(final long step) {
        return true;
    }

    @Override
    public void run() {
        assertNotInvoked();
        invoked = true;
    }

    public void reset() {
        invoked = false;
    }

    public void assertInvoked() {
        assertTrue(invoked);
    }

    public void assertNotInvoked() {
        assertFalse(invoked);
    }
}
