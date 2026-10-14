//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests that a failure preparing an evaluation is reported the same way a script error is, and that the script does not
 * run.
 */
public class TestScriptSessionPreparation {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private final RuntimeException preparationFailure = new IllegalStateException("preparation failed");

    private boolean failPreparation;
    private boolean evaluated;
    private AbstractScriptSession<AbstractScriptSession.Snapshot> session;

    @Before
    public void setup() {
        final ExecutionContext context = ExecutionContext.getContext();
        session = new NoLanguageDeephavenSession(context.getUpdateGraph(), context.getOperationInitializer()) {
            @Override
            protected void prepareForEvaluation() {
                if (failPreparation) {
                    throw preparationFailure;
                }
            }

            @Override
            protected void evaluate(final String command, @Nullable final String scriptName) {
                evaluated = true;
            }

            @Override
            protected Snapshot emptySnapshot() {
                return () -> {
                };
            }

            @Override
            protected Snapshot takeSnapshot() {
                return () -> {
                };
            }

            @Override
            protected Changes createDiff(final Snapshot from, final Snapshot to, final RuntimeException e) {
                final Changes diff = new Changes();
                diff.error = e;
                return diff;
            }
        };
    }

    @After
    public void teardown() {
        session.cleanup();
    }

    @Test
    public void testPreparationFailureIsReportedAndSkipsTheScript() {
        failPreparation = true;
        final ScriptSession.Changes failed = session.evaluateScript("ignored", null);
        assertSame("The preparation failure should be reported like a script error", preparationFailure,
                failed.error);
        assertFalse("The script should not run when preparation fails", evaluated);

        failPreparation = false;
        final ScriptSession.Changes succeeded = session.evaluateScript("ignored", null);
        assertNull(succeeded.error);
        assertTrue(evaluated);
    }
}
