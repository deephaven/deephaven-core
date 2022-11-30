package io.deephaven.engine.testutil;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.AbstractNotification;
import junit.framework.TestCase;

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
        TestCase.assertTrue(invoked);
    }

    public void assertNotInvoked() {
        TestCase.assertFalse(invoked);
    }

    @Override
    public ExecutionContext getExecutionContext() {
        return null;
    }
}
