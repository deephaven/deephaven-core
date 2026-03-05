//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.liveness;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for liveness code.
 */
public class TestLiveness {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

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

    private static class NamedLivenessArtifact extends LivenessArtifact {
        public String name;

        public NamedLivenessArtifact(String name, boolean strong) {
            super(strong);
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @Test
    public void testMultipleReferences() {
        testMultipleReferences(false);
        testMultipleReferences(true);
    }

    private void testMultipleReferences(boolean strong) {
        final LivenessArtifact a1, a2, a3;
        final LivenessScope scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            a1 = new NamedLivenessArtifact("a1", strong);
            a2 = new NamedLivenessArtifact("a2", strong);
            a3 = new NamedLivenessArtifact("a3", strong);
            a1.manage(a3);
            a1.manage(a2);
            scope.manage(a1);
        }

        final LivenessArtifact a4;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            a4 = new NamedLivenessArtifact("a4", strong);
            a4.manage(a1);
            a4.manage(a2);
            a4.manage(a3);
        }

        if (a4.tryManage(a1)) {
            TestCase.fail("Expected not to manage a1");
        }

        scope.release();

        final LivenessArtifact a5;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            a5 = new NamedLivenessArtifact("a5", strong);
            if (a5.tryManage(a1)) {
                TestCase.fail("Expected not to manage a1");
            }
            if (a5.tryManage(a2)) {
                TestCase.fail("Expected not to manage a2");
            }
            if (a5.tryManage(a3)) {
                TestCase.fail("Expected not to manage a3");
            }
        }
    }

    @Test
    public void testSingletonLivenessManager() {
        testSingletonLivenessManager(false);
        testSingletonLivenessManager(true);
    }

    private void testSingletonLivenessManager(final boolean strong) {
        final LivenessArtifact a1, a2, a3;
        final SingletonLivenessManager slm;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            a1 = new NamedLivenessArtifact("a1", strong);
            a2 = new NamedLivenessArtifact("a2", strong);
            a3 = new NamedLivenessArtifact("a3", strong);
            a2.manage(a1);
            a3.manage(a1);
            a3.manage(a2);
            slm = new SingletonLivenessManager(a3);
        }

        assertTrue(a1.tryRetainReference());
        a1.dropReference();
        assertTrue(a2.tryRetainReference());
        a2.dropReference();
        assertTrue(a3.tryRetainReference());
        a3.dropReference();

        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final LivenessArtifact a4 = new NamedLivenessArtifact("a4", strong);
            a4.manage(a3);
        }

        slm.release();

        assertFalse(a1.tryRetainReference());
        assertFalse(a2.tryRetainReference());
        assertFalse(a3.tryRetainReference());
    }
}
