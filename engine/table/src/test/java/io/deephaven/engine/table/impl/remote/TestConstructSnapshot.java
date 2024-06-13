//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.remote;

import io.deephaven.base.SleepUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.FunctionalColumn;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.thread.NamingThreadFactory;
import io.deephaven.util.mutable.MutableLong;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.stringCol;

public class TestConstructSnapshot extends RefreshingTableTestCase {

    public void testClockChange() throws InterruptedException {
        final MutableLong changed = new MutableLong(0);
        final ConstructSnapshot.SnapshotControl control = new ConstructSnapshot.SnapshotControl() {

            @Override
            public Boolean usePreviousValues(long beforeClockValue) {
                // noinspection AutoBoxing
                return LogicalClock.getState(beforeClockValue) == LogicalClock.State.Updating;
            }

            @Override
            public boolean snapshotConsistent(final long currentClockValue, final boolean usingPreviousValues) {
                return true;
            }

            @Override
            public UpdateGraph getUpdateGraph() {
                return ExecutionContext.getContext().getUpdateGraph();
            }
        };
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final Runnable snapshot_test = () -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                ConstructSnapshot.callDataSnapshotFunction("snapshot test", control, (usePrev, beforeClock) -> {
                    SleepUtil.sleep(1000);
                    if (ConstructSnapshot.concurrentAttemptInconsistent()) {
                        changed.increment();
                    }
                    return true;
                });
            }
        };

        changed.set(0);
        final Thread t = new Thread(snapshot_test);
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().startCycleForUnitTests();
        t.start();
        t.join();
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().completeCycleForUnitTests();
        assertEquals(0, changed.get());

        changed.set(0);
        final Thread t2 = new Thread(snapshot_test);
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().startCycleForUnitTests();
        t2.start();
        SleepUtil.sleep(100);
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().completeCycleForUnitTests();
        t2.join();
        assertEquals(1, changed.get());
    }
}

