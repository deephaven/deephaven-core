//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
}
