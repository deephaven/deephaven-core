//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.TableAlreadyFailedException;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * An exception raised while an as-of join is built or updated (here, by a stamp formula that throws for one value) must
 * fail the operation without leaving pooled chunks or contexts unreleased. The engine test harness tracks pooled chunk
 * release, and each test checks it explicitly once the failure has been delivered.
 */
@Category(OutOfBandTest.class)
public class QueryTableAjFailureCleanupTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private static final String THROWING_STAMP = "Stamp = S0 == 99 ? Integer.parseInt(`notAnInt`) : S0";

    private static QueryTable makeLeft(final boolean refreshing, final boolean withBadRow) {
        return withBadRow
                ? makeTable(refreshing, i(10, 20, 30, 40).toTracking(), intCol("Key", 1, 1, 2, 1),
                        intCol("S0", 1, 5, 3, 99))
                : makeTable(refreshing, i(10, 20, 30).toTracking(), intCol("Key", 1, 1, 2), intCol("S0", 1, 5, 3));
    }

    private static QueryTable makeRight(final boolean refreshing) {
        return makeTable(refreshing, i(10, 20, 30).toTracking(), intCol("Key", 1, 1, 2), intCol("S0", 0, 4, 2),
                intCol("Val", 100, 200, 300));
    }

    private static QueryTable makeTable(final boolean refreshing,
            final TrackingRowSet rowSet,
            final ColumnHolder<?>... columns) {
        return refreshing ? TstUtils.testRefreshingTable(rowSet, columns) : TstUtils.testTable(rowSet, columns);
    }

    private static Table doAj(final Table left, final Table right, final boolean keyed) {
        final Table leftView = left.view("Key", THROWING_STAMP);
        final Table rightView = right.view("Key", THROWING_STAMP, "Val");
        return leftView.aj(rightView, keyed ? "Key,Stamp" : "Stamp", "Val");
    }

    private static void addStampRow(final QueryTable table, final int stamp) {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            if (table.hasColumns("Val")) {
                addToTable(table, i(40), intCol("Key", 1), intCol("S0", stamp), intCol("Val", 400));
            } else {
                addToTable(table, i(40), intCol("Key", 1), intCol("S0", stamp));
            }
            table.notifyListeners(i(40), i(), i());
        });
    }

    /**
     * Both sides refresh and the join is keyed, so the BucketedChunkedAjMergedListener processes the update. A stamp
     * value of 99 on the ticking side makes the stamp fill throw from process().
     */
    private void checkBucketedUpdateThrows(final boolean failLeft) {
        final QueryTable left = makeLeft(true, false);
        final QueryTable right = makeRight(true);
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final Table result = doAj(left, right, true);
            base.setExpectError(true);
            try {
                final ErrorListener errorListener = new ErrorListener(result);
                result.addUpdateListener(errorListener);
                addStampRow(failLeft ? left : right, 99);
                assertTrue(result.isFailed());
                assertNotNull(errorListener.originalException());
            } finally {
                base.setExpectError(false);
            }
        }
        assertFalse(left.hasListeners());
        assertFalse(right.hasListeners());
        ChunkPoolReleaseTracking.check();
    }

    /**
     * A left-side exception in BucketedChunkedAjMergedListener.process() releases the left fill context, left stamp
     * chunks and the sort kernel.
     */
    @Test
    public void testBucketedLeftUpdateFailureReleasesChunks() {
        checkBucketedUpdateThrows(true);
    }

    /**
     * A right-side exception in BucketedChunkedAjMergedListener.process() releases the sort kernel.
     */
    @Test
    public void testBucketedRightUpdateFailureReleasesChunks() {
        checkBucketedUpdateThrows(false);
    }

    /**
     * Zero-key aj of a static left against a refreshing right releases its left stamp chunks when the initial left
     * stamp fill throws, before any listener owns them.
     */
    @Test
    public void testZeroKeyRightIncrementalInitialFailureReleasesChunks() {
        final QueryTable left = makeLeft(false, true);
        final QueryTable right = makeRight(true);
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            doAj(left, right, false);
            fail("expected the join to throw");
        } catch (final RuntimeException expected) {
            // the left stamp formula throws while the join is built
        }
        ChunkPoolReleaseTracking.check();
    }

    /**
     * Zero-key aj of a refreshing left against a static right with duplicate stamps copies the compacted right stamps
     * into chunks the left listener owns. When the listener can not be registered (here, because the left table has
     * already failed), the listener's destroy() releases those chunks exactly once.
     */
    @Test
    public void testZeroKeyRightStaticListenerFailureReleasesCompactedChunks() {
        final QueryTable left = makeTable(true, i(10, 20, 30).toTracking(), intCol("S0", 1, 5, 3));
        final QueryTable right = makeTable(false, i(10, 20, 30).toTracking(), intCol("S0", 0, 2, 2),
                intCol("Val", 100, 200, 300));
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> left.notifyListenersOnError(new RuntimeException("failed"), null));
        assertTrue(left.isFailed());
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            left.aj(right, "S0", "Val");
            fail("expected the join to throw");
        } catch (final TableAlreadyFailedException expected) {
            // the left listener can not be registered on a failed table
        }
        ChunkPoolReleaseTracking.check();
    }
}
