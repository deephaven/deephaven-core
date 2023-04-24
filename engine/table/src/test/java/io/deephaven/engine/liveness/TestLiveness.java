/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.liveness;

import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateContext;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for liveness code.
 */
public class TestLiveness {

    private boolean oldCheckUgp;
    private LivenessScope scope;
    private SafeCloseable executionContext;

    @Before
    public void setUp() throws Exception {
        UpdateContext.updateGraphProcessor().enableUnitTestMode();
        UpdateContext.updateGraphProcessor().resetForUnitTests(false);
        oldCheckUgp = UpdateContext.updateGraphProcessor().setCheckTableOperations(false);
        scope = new LivenessScope();
        LivenessScopeStack.push(scope);
        executionContext = TestExecutionContext.createForUnitTests().open();
    }

    @After
    public void tearDown() throws Exception {
        LivenessScopeStack.pop(scope);
        scope.release();
        UpdateContext.updateGraphProcessor().setCheckTableOperations(oldCheckUgp);
        UpdateContext.updateGraphProcessor().resetForUnitTests(true);
        executionContext.close();
    }

    @Test
    public void testRecursion() {
        // noinspection AutoBoxing
        final Table input = TstUtils.testRefreshingTable(
                TstUtils.i(2, 3, 6, 7, 8, 10, 12, 15, 16).toTracking(),
                TableTools.col("GroupedInts", 1, 1, 2, 2, 2, 3, 3, 3, 3));
        Table result = null;
        for (int ii = 0; ii < 4096; ++ii) {
            if (result == null) {
                result = input;
            } else {
                result = TableTools.merge(result, input).updateView("GroupedInts=GroupedInts+1")
                        .updateView("GroupedInts=GroupedInts-1");
            }
        }
    }

    @Test
    public void testTryManageFailure() {
        final LivenessArtifact a1;
        final LivenessArtifact a2;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            a1 = new LivenessArtifact();
            a2 = new LivenessArtifact();
        }
        try {
            a1.manage(a2);
            TestCase.fail("Expected exception");
        } catch (LivenessStateException expected) {
            expected.printStackTrace();
        }

        final LivenessArtifact a3;
        final LivenessArtifact a4 = new LivenessArtifact();
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            a3 = new LivenessArtifact();
        }
        try {
            a3.manage(a4);
            TestCase.fail("Expected exception");
        } catch (LivenessStateException expected) {
            expected.printStackTrace();
        }

        final LivenessArtifact a5 = new LivenessArtifact();
        final LivenessArtifact a6;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            a6 = new LivenessArtifact();
        }
        try {
            a5.manage(a6);
            TestCase.fail("Expected exception");
        } catch (LivenessStateException expected) {
            expected.printStackTrace();
        }
    }
}
