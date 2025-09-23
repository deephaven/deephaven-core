//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.BooleanGenerator;
import io.deephaven.engine.testutil.generator.IncreasingSortedLongGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.junit.Assert.*;

public class TestLeaderTableFilter {
    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void testSimple() {
        final QueryTable leader =
                TstUtils.testRefreshingTable(col("Key", "a", "a"), longCol("A", 2, 3), longCol("B", 0, 4));
        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 1, 2, 2, 3, 3),
                intCol("Sentinel", 101, 102, 103, 104, 105, 106), col("Key", "a", "a", "a", "a", "a", "a"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 0, 0, 2, 2, 4, 4),
                intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Key", "a", "a", "a", "a", "a", "a"));

        final LeaderTableFilter.TableBuilder builder = new LeaderTableFilter.TableBuilder(leader.assertAddOnly());
        builder.addTable("a", a.assertAddOnly(), "A=ID");
        builder.addTable("b", b.assertAddOnly(), "B=ID");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final LeaderTableFilter.Results<Table> results = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", LeaderTableFilter.DEFAULT_LEADER_NAME)), results.keySet());

        final Table fl = results.get(LeaderTableFilter.DEFAULT_LEADER_NAME);
        final Table fa = results.get("a");
        final Table fb = results.get("b");

        TableTools.show(fl);
        TableTools.show(fa);
        TableTools.show(fb);

        final Table ex1l = newTable(col("Key", "a"), longCol("A", 3), longCol("B", 4));
        final Table ex1a = newTable(longCol("ID", 3, 3), intCol("Sentinel", 105, 106), col("Key", "a", "a"));
        final Table ex1b = newTable(longCol("ID", 4, 4), intCol("Sentinel", 205, 206), col("Key", "a", "a"));

        assertTableEquals(ex1l, fl);
        assertTableEquals(ex1a, fa);
        assertTableEquals(ex1b, fb);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(10, 11), longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
            a.notifyListeners(i(10, 11), i(), i());
        });

        assertTableEquals(ex1l, fl);
        assertTableEquals(ex1a, fa);
        assertTableEquals(ex1b, fb);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(leader, i(10), longCol("A", 5), longCol("B", 4), col("Key", "b"));
            leader.notifyListeners(i(10), i(), i());
        });

        TableTools.showWithRowSet(fl);
        TableTools.showWithRowSet(fa);
        TableTools.showWithRowSet(fb);

        final Table ex2l = newTable(col("Key", "b"), longCol("A", 5), longCol("B", 4));
        final Table ex2a = newTable(longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
        final Table ex2b = newTable(longCol("ID", 4, 4), intCol("Sentinel", 205, 206), col("Key", "a", "a"));

        assertTableEquals(ex2l, fl);
        assertTableEquals(ex2a, fa);
        assertTableEquals(ex2b, fb);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(leader, i(11), longCol("A", 5), longCol("B", NULL_LONG), col("Key", "b"));
            leader.notifyListeners(i(11), i(), i());
        });

        TableTools.showWithRowSet(fl);
        TableTools.showWithRowSet(fa);
        TableTools.showWithRowSet(fb);

        final Table ex3l = newTable(col("Key", "b"), longCol("A", 5), longCol("B", NULL_LONG));
        final Table ex3a = newTable(longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
        final Table ex3b = ex2b.head(0);

        assertTableEquals(ex3l, fl);
        assertTableEquals(ex3a, fa);
        assertTableEquals(ex3b, fb);

        // everyone updates at once

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(leader, i(12), longCol("A", 6), longCol("B", 5), col("Key", "b"));
            leader.notifyListeners(i(12), i(), i());

            TstUtils.addToTable(a, i(20, 21), longCol("ID", 6, 6), intCol("Sentinel", 110, 111), col("Key", "b", "b"));
            a.notifyListeners(i(20, 21), i(), i());

            TstUtils.addToTable(b, i(10, 11, 12), longCol("ID", 5, 5, 5), intCol("Sentinel", 210, 211, 212),
                    col("Key", "b", "b", "b"));
            b.notifyListeners(i(10, 11, 12), i(), i());
        });

        TableTools.showWithRowSet(fl);
        TableTools.showWithRowSet(fa);
        TableTools.showWithRowSet(fb);

        final Table ex4l = newTable(col("Key", "b"), longCol("A", 6), longCol("B", 5));
        final Table ex4a = newTable(longCol("ID", 6, 6), intCol("Sentinel", 110, 111), col("Key", "b", "b"));
        final Table ex4b =
                newTable(longCol("ID", 5, 5, 5), intCol("Sentinel", 210, 211, 212), col("Key", "b", "b", "b"));

        assertTableEquals(ex4l, fl);
        assertTableEquals(ex4a, fa);
        assertTableEquals(ex4b, fb);
    }

    @Test
    public void testLiveness() {
        final QueryTable leader =
                TstUtils.testRefreshingTable(col("Key", "a", "a"), longCol("A", 2, 3), longCol("B", 0, 4));
        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 1, 2, 2, 3, 3),
                intCol("Sentinel", 101, 102, 103, 104, 105, 106), col("Key", "a", "a", "a", "a", "a", "a"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 0, 0, 2, 2, 4, 4),
                intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Key", "a", "a", "a", "a", "a", "a"));

        final LeaderTableFilter.TableBuilder builder = new LeaderTableFilter.TableBuilder(leader.assertAddOnly());
        builder.addTable("a", a.assertAddOnly(), "A=ID");
        builder.addTable("b", b.assertAddOnly(), "B=ID");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final LeaderTableFilter.Results<Table> results;
        final Table leaderResult;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            results = updateGraph.sharedLock().computeLocked(builder::build);
            leaderResult = results.getLeader();
            Assert.assertTrue(results.tryRetainReference());
            results.dropReference();
            Assert.assertTrue(leaderResult.tryRetainReference());
            leaderResult.dropReference();
        }
        Assert.assertFalse(results.tryRetainReference());
        Assert.assertFalse(leaderResult.tryRetainReference());
    }

    @Test
    public void testStatic() {
        final QueryTable leader =
                TstUtils.testTable(col("Key", "a", "a"), longCol("A", 2, 3), longCol("B", 0, 4));
        final QueryTable a = TstUtils.testTable(longCol("ID", 1, 1, 2, 2, 3, 3),
                intCol("Sentinel", 101, 102, 103, 104, 105, 106), col("Key", "a", "a", "a", "a", "a", "a"));
        final QueryTable b = TstUtils.testTable(longCol("ID", 0, 0, 2, 2, 4, 4),
                intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Key", "a", "a", "a", "a", "a", "a"));

        assertFalse(leader.isRefreshing());
        assertFalse(a.isRefreshing());
        assertFalse(b.isRefreshing());

        final LeaderTableFilter.TableBuilder builder = new LeaderTableFilter.TableBuilder(leader.assertAddOnly());
        builder.addTable("a", a.assertAddOnly(), "A=ID");
        builder.addTable("b", b.assertAddOnly(), "B=ID");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final LeaderTableFilter.Results<Table> results = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", LeaderTableFilter.DEFAULT_LEADER_NAME)), results.keySet());

        final Table fl = results.get(LeaderTableFilter.DEFAULT_LEADER_NAME);
        final Table fa = results.get("a");
        final Table fb = results.get("b");

        TableTools.show(fl);
        TableTools.show(fa);
        TableTools.show(fb);

        final Table ex1l = newTable(col("Key", "a"), longCol("A", 3), longCol("B", 4));
        final Table ex1a = newTable(longCol("ID", 3, 3), intCol("Sentinel", 105, 106), col("Key", "a", "a"));
        final Table ex1b = newTable(longCol("ID", 4, 4), intCol("Sentinel", 205, 206), col("Key", "a", "a"));

        assertTableEquals(ex1l, fl);
        assertTableEquals(ex1a, fa);
        assertTableEquals(ex1b, fb);

        assertFalse(fl.isRefreshing());
        assertFalse(fa.isRefreshing());
        assertFalse(fb.isRefreshing());
    }

    @Test
    public void testStaticLeader() {
        final QueryTable leader =
                TstUtils.testTable(col("Key", "a", "a", "a"), longCol("A", 2, 3, 5), longCol("B", 0, 4, 4));
        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 1, 2, 2, 3, 3),
                intCol("Sentinel", 101, 102, 103, 104, 105, 106), col("Key", "a", "a", "a", "a", "a", "a"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 0, 0, 2, 2, 4, 4),
                intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Key", "a", "a", "a", "a", "a", "a"));

        final LeaderTableFilter.TableBuilder builder = new LeaderTableFilter.TableBuilder(leader);
        builder.addTable("a", a.assertAddOnly(), "A=ID");
        builder.addTable("b", b.assertAddOnly(), "B=ID");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final LeaderTableFilter.Results<Table> results = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", LeaderTableFilter.DEFAULT_LEADER_NAME)), results.keySet());

        final Table fl = results.get(LeaderTableFilter.DEFAULT_LEADER_NAME);
        final Table fa = results.get("a");
        final Table fb = results.get("b");

        assertTrue(fl.isRefreshing());
        assertTrue(fa.isRefreshing());
        assertTrue(fb.isRefreshing());

        final Table ex1l = newTable(col("Key", "a"), longCol("A", 3), longCol("B", 4));
        final Table ex1a = newTable(longCol("ID", 3, 3), intCol("Sentinel", 105, 106), col("Key", "a", "a"));
        final Table ex1b = newTable(longCol("ID", 4, 4), intCol("Sentinel", 205, 206), col("Key", "a", "a"));

        assertTableEquals(ex1l, fl);
        assertTableEquals(ex1a, fa);
        assertTableEquals(ex1b, fb);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(10, 11), longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
            a.notifyListeners(i(10, 11), i(), i());
        });

        final Table ex2l = newTable(col("Key", "a"), longCol("A", 5), longCol("B", 4));
        final Table ex2a = newTable(longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
        final Table ex2b = newTable(longCol("ID", 4, 4), intCol("Sentinel", 205, 206), col("Key", "a", "a"));

        assertTableEquals(ex2l, fl);
        assertTableEquals(ex2a, fa);
        assertTableEquals(ex2b, fb);
    }

    @Test
    public void testStaticFollower() {
        final QueryTable leader =
                TstUtils.testRefreshingTable(col("Key", "a", "a"), longCol("A", 2, 3), longCol("B", 0, 4));
        final QueryTable a = TstUtils.testTable(longCol("ID", 1, 1, 2, 2, 3, 3, 5, 5),
                intCol("Sentinel", 101, 102, 103, 104, 105, 106, 107, 108),
                col("Key", "a", "a", "a", "a", "a", "a", "a", "a"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 0, 0, 2, 2, 4, 4),
                intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Key", "a", "a", "a", "a", "a", "a"));

        final LeaderTableFilter.TableBuilder builder = new LeaderTableFilter.TableBuilder(leader.assertAddOnly());
        builder.addTable("a", a.assertAddOnly(), "A=ID");
        builder.addTable("b", b.assertAddOnly(), "B=ID");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final LeaderTableFilter.Results<Table> results = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", LeaderTableFilter.DEFAULT_LEADER_NAME)), results.keySet());

        final Table fl = results.getLeader();
        final Table fa = results.get("a");
        final Table fb = results.get("b");

        assertTrue(fl.isRefreshing());
        assertTrue(fa.isRefreshing());
        assertTrue(fb.isRefreshing());

        TableTools.show(fl);
        TableTools.show(fa);
        TableTools.show(fb);

        final Table ex1l = newTable(col("Key", "a"), longCol("A", 3), longCol("B", 4));
        final Table ex1a = newTable(longCol("ID", 3, 3), intCol("Sentinel", 105, 106), col("Key", "a", "a"));
        final Table ex1b = newTable(longCol("ID", 4, 4), intCol("Sentinel", 205, 206), col("Key", "a", "a"));

        assertTableEquals(ex1l, fl);
        assertTableEquals(ex1a, fa);
        assertTableEquals(ex1b, fb);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(leader, i(10), longCol("A", 5), longCol("B", 4), col("Key", "b"));
            leader.notifyListeners(i(10), i(), i());
        });

        final Table ex2l = newTable(col("Key", "b"), longCol("A", 5), longCol("B", 4));
        final Table ex2a = newTable(longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "a", "a"));
        final Table ex2b = newTable(longCol("ID", 4, 4), intCol("Sentinel", 205, 206), col("Key", "a", "a"));

        assertTableEquals(ex2l, fl);
        assertTableEquals(ex2a, fa);
        assertTableEquals(ex2b, fb);
    }

    @Test
    public void testSimpleKeyed() {
        final QueryTable leader =
                TstUtils.testRefreshingTable(col("Key", "a", "b"), longCol("A", 1, 2), longCol("B", 101, 102));

        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 2, 3), intCol("Sentinel", 101, 102, 103),
                col("Key", "a", "b", "c"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 100, 100, 100),
                intCol("Sentinel", 201, 202, 203), col("Key", "a", "b", "c"));

        final String leaderName = "POTUS";

        final LeaderTableFilter.TableBuilder builder =
                new LeaderTableFilter.TableBuilder(leader.assertAddOnly(), "Key").setLeaderName(leaderName);
        builder.addTable("a", a.assertAddOnly(), "A=ID", "Key");
        builder.addTable("b", b.assertAddOnly(), "B=ID", "Key");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final LeaderTableFilter.Results<Table> results = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", leaderName)), results.keySet());

        final Table fl = results.get(leaderName);
        final Table fa = results.get("a");
        final Table fb = results.get("b");

        dumpTables(leader, a, b, fl, fa, fb);

        assertTableEquals(leader.head(0), fl);
        assertTableEquals(a.head(0), fa);
        assertTableEquals(b.head(0), fb);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(10, 11, 12), longCol("ID", 5, 4, 4), intCol("Sentinel", 104, 105, 106),
                    col("Key", "a", "a", "a"));
            a.notifyListeners(i(10, 11, 12), i(), i());
        });

        // we still haven't done anything but release more data; we will need to sort and compact the IDs in this case
        // as well
        assertTableEquals(leader.head(0), fl);
        assertTableEquals(a.head(0), fa);
        assertTableEquals(b.head(0), fb);


        // add to b so that we can see some rows
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(10, 11), longCol("ID", 101, 102), intCol("Sentinel", 210, 211),
                    col("Key", "a", "b"));
            b.notifyListeners(i(10, 11), i(), i());
        });

        dumpTables(leader, a, b, fl, fa, fb);

        assertTableEquals(leader.head(2), fl);
        assertTableEquals(a.where("Sentinel in 101, 102"), fa);
        assertTableEquals(b.where("Sentinel in 210, 211"), fb);

        // now some of that saved up A data we have for ID 4
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(leader, i(12), longCol("A", 4), longCol("B", 101), col("Key", "a"));
            leader.notifyListeners(i(12), i(), i());
        });

        final Table mostRecentLeader = updateGraph.sharedLock()
                .computeLocked(() -> leader.update("K=k").lastBy("Key").sort("K").dropColumns("K"));

        dumpTables(leader, a, b, fl, fa, fb);
        assertTableEquals(mostRecentLeader, fl);
        assertTableEquals(a.where("Sentinel in 105, 106, 102"), fa);
        assertTableEquals(b.where("Sentinel in 210, 211"), fb);

        // and we can drop A, keep B

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(leader, i(13), longCol("A", NULL_LONG), longCol("B", 101), col("Key", "a"));
            leader.notifyListeners(i(13), i(), i());
        });

        dumpTables(leader, a, b, fl, fa, fb);
        assertTableEquals(mostRecentLeader, fl);
        assertTableEquals(a.where("Sentinel in 102"), fa);
        assertTableEquals(b.where("Sentinel in 210, 211"), fb);

        // and now we add some data back to A and keep B; we shouldn't give a darn about old data getting added to A
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(13), longCol("ID", 3), intCol("Sentinel", 110), col("Key", "a"));
            a.notifyListeners(i(13), i(), i());

            TstUtils.addToTable(leader, i(14), longCol("A", 5), longCol("B", 101), col("Key", "a"));
            leader.notifyListeners(i(14), i(), i());
        });
        dumpTables(leader, a, b, fl, fa, fb);
        assertTableEquals(mostRecentLeader, fl);
        assertTableEquals(a.where("Sentinel in 104, 102"), fa);
        assertTableEquals(b.where("Sentinel in 210, 211"), fb);

        // now let's introduce a new key to the leader (which has been hanging out in a and b all this time)
        // we're also setting up "d" as a new key in the A follower
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(20), longCol("ID", 3), intCol("Sentinel", 120), col("Key", "d"));
            a.notifyListeners(i(20), i(), i());

            TstUtils.addToTable(leader, i(15), longCol("A", 3), longCol("B", 100), col("Key", "c"));
            leader.notifyListeners(i(15), i(), i());
        });
        assertTableEquals(mostRecentLeader, fl);
        assertTableEquals(a.where("Sentinel in 104, 102, 103"), fa);
        assertTableEquals(b.where("Sentinel in 210, 211, 203"), fb);

        // and we can instantiate d at this point
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(20, 21), longCol("ID", -7, -7), intCol("Sentinel", 220, 221),
                    col("Key", "d", "d"));
            b.notifyListeners(i(20, 21), i(), i());

            TstUtils.addToTable(leader, i(16), longCol("A", 3), longCol("B", -7), col("Key", "d"));
            leader.notifyListeners(i(16), i(), i());
        });
        assertTableEquals(mostRecentLeader, fl);
        assertTableEquals(a.where("Sentinel in 104, 102, 103, 120"), fa);
        assertTableEquals(b.where("Sentinel in 210, 211, 203, 220, 221"), fb);

        // we want to even better test compaction
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(30, 31, 32, 33), longCol("ID", 3, 2, 1, 2), intCol("Sentinel", 230, 231, 232, 233),
                    col("Key", "e", "e", "e", "e"));
            b.notifyListeners(i(30, 31, 32, 33), i(), i());

            TstUtils.addToTable(a, i(30, 31, 32, 33), longCol("ID", 7, 9, 7, 7), intCol("Sentinel", 130, 131, 132, 133),
                    col("Key", "e", "e", "e", "e"));
            a.notifyListeners(i(30, 31, 32, 33), i(), i());
        });
        assertTableEquals(mostRecentLeader, fl);
        assertTableEquals(a.where("Sentinel in 104, 102, 103, 120"), fa);
        assertTableEquals(b.where("Sentinel in 210, 211, 203, 220, 221"), fb);

        // now get IDs 2 and 8 which are in the middle to be activated
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(leader, i(17, 18, 19), longCol("A", 7, 8, 8), longCol("B", 2, 2, 3),
                    col("Key", "e", "e", "e"));
            leader.notifyListeners(i(17, 18, 19), i(), i());
        });

        dumpTables(leader, a, b, fl, fa, fb);

        final Table notQuiteMostRecentLeader = updateGraph.sharedLock().computeLocked(
                () -> leader.slice(0, -2).withAttributes(Map.of(BaseTable.TEST_SOURCE_TABLE_ATTRIBUTE, true))
                        .update("K=k").lastBy("Key").sort("K").dropColumns("K"));

        assertTableEquals(notQuiteMostRecentLeader, fl);
        assertTableEquals(a.where("Sentinel in 104, 102, 103, 120, 130, 132, 133"), fa);
        assertTableEquals(b.where("Sentinel in 210, 211, 203, 220, 221, 231, 233"), fb);

        // add some data to a current key
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(34), longCol("ID", 7), intCol("Sentinel", 134), col("Key", "e"));
            a.notifyListeners(i(34), i(), i());
        });

        assertTableEquals(notQuiteMostRecentLeader, fl);
        assertTableEquals(a.where("Sentinel in 104, 102, 103, 120, 130, 132, 133, 134"), fa);
        assertTableEquals(b.where("Sentinel in 210, 211, 203, 220, 221, 231, 233"), fb);

        // add some data to the null ID, and an old ID
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(35, 36), longCol("ID", NULL_LONG, -1), intCol("Sentinel", 135, 136),
                    col("Key", "e", "e"));
            a.notifyListeners(i(35, 36), i(), i());
        });

        assertTableEquals(notQuiteMostRecentLeader, fl);
        assertTableEquals(a.where("Sentinel in 104, 102, 103, 120, 130, 132, 133, 134"), fa);
        assertTableEquals(b.where("Sentinel in 210, 211, 203, 220, 221, 231, 233"), fb);
    }

    @Test
    public void testRepeatedMatch() {
        final QueryTable leader =
                TstUtils.testRefreshingTable(col("Key", "a", "a"), longCol("A", 2, 3), longCol("B", 4, 4));
        final QueryTable a =
                TstUtils.testRefreshingTable(longCol("ID", 2, 2), intCol("Sentinel", 101, 102), col("Key", "a", "a"));
        final QueryTable b =
                TstUtils.testRefreshingTable(longCol("ID", 4, 4), intCol("Sentinel", 201, 202), col("Key", "a", "a"));

        final LeaderTableFilter.TableBuilder builder = new LeaderTableFilter.TableBuilder(leader.assertAddOnly());
        builder.addTable("a", a.assertAddOnly(), "A=ID");
        builder.addTable("b", b.assertAddOnly(), "B=ID");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final LeaderTableFilter.Results<Table> results = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", LeaderTableFilter.DEFAULT_LEADER_NAME)), results.keySet());

        final Table fl = results.get(LeaderTableFilter.DEFAULT_LEADER_NAME);
        final Table fa = results.get("a");
        final Table fb = results.get("b");

        TableTools.show(fl);
        TableTools.show(fa);
        TableTools.show(fb);

        final Table ex1l = newTable(col("Key", "a"), longCol("A", 2), longCol("B", 4));
        final Table ex1a = newTable(longCol("ID", 2, 2), intCol("Sentinel", 101, 102), col("Key", "a", "a"));
        final Table ex1b = newTable(longCol("ID", 4, 4), intCol("Sentinel", 201, 202), col("Key", "a", "a"));

        assertTableEquals(ex1l, fl);
        assertTableEquals(ex1a, fa);
        assertTableEquals(ex1b, fb);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(10, 11), longCol("ID", 3, 3), intCol("Sentinel", 103, 104), col("Key", "a", "a"));
            TstUtils.addToTable(b, i(10, 11), longCol("ID", 4, 4), intCol("Sentinel", 203, 204), col("Key", "a", "a"));
            a.notifyListeners(i(10, 11), i(), i());
            b.notifyListeners(i(10, 11), i(), i());
        });

        TableTools.showWithRowSet(fl);
        TableTools.showWithRowSet(fa);
        TableTools.showWithRowSet(fb);

        final Table ex2l = newTable(col("Key", "a"), longCol("A", 3), longCol("B", 4));
        final Table ex2a = newTable(longCol("ID", 3, 3), intCol("Sentinel", 103, 104), col("Key", "a", "a"));
        final Table ex2b = newTable(longCol("ID", 4, 4, 4, 4), intCol("Sentinel", 201, 202, 203, 204),
                col("Key", "a", "a", "a", "a"));

        assertTableEquals(ex2l, fl);
        assertTableEquals(ex2a, fa);
        assertTableEquals(ex2b, fb);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(leader, i(2), longCol("A", 5), longCol("B", 4), col("Key", "a"));
            TstUtils.addToTable(a, i(20), longCol("ID", 5), intCol("Sentinel", 105), col("Key", "a"));
            TstUtils.addToTable(b, i(20), longCol("ID", 4), intCol("Sentinel", 205), col("Key", "a"));
            leader.notifyListeners(i(2), i(), i());
            a.notifyListeners(i(20), i(), i());
            b.notifyListeners(i(20), i(), i());
        });

        TableTools.showWithRowSet(fl);
        TableTools.showWithRowSet(fa);
        TableTools.showWithRowSet(fb);

        final Table ex3l = newTable(col("Key", "a"), longCol("A", 5), longCol("B", 4));
        final Table ex3a = newTable(longCol("ID", 5), intCol("Sentinel", 105), col("Key", "a"));
        final Table ex3b = newTable(longCol("ID", 4, 4, 4, 4, 4), intCol("Sentinel", 201, 202, 203, 204, 205),
                col("Key", "a", "a", "a", "a", "a"));

        assertTableEquals(ex3l, fl);
        assertTableEquals(ex3a, fa);
        assertTableEquals(ex3b, fb);
    }

    @Test
    public void testNullFollower() {
        final QueryTable leader = TstUtils.testRefreshingTable(col("Key", "a", "b", "b"), longCol("A", 1, 2, 3),
                longCol("B", 101, NULL_LONG, 110), intCol("Sentinel", 300, 301, 302));

        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 2, 3), intCol("Sentinel", 101, 102, 103),
                col("Key", "a", "b", "c"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 100), intCol("Sentinel", 201), col("Key", "a"));

        final String leaderName = "POTUS";

        final LeaderTableFilter.TableBuilder builder =
                new LeaderTableFilter.TableBuilder(leader.assertAddOnly(), "Key").setLeaderName(leaderName);
        builder.addTable("a", a.assertAddOnly(), "A=ID", "Key");
        builder.addTable("b", b.assertAddOnly(), "B=ID", "Key");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final LeaderTableFilter.Results<Table> results = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", leaderName)), results.keySet());

        final Table fl = results.get(leaderName);
        final Table fa = results.get("a");
        final Table fb = results.get("b");

        dumpTables(leader, a, b, fl, fa, fb);

        assertTableEquals(leader.where("Sentinel in 301"), fl);
        assertTableEquals(a.where("ID in 2"), fa);
        assertTableEquals(b.head(0), fb);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(10), longCol("ID", 101), intCol("Sentinel", 202), col("Key", "a"));
            b.notifyListeners(i(10), i(), i());
        });

        dumpTables(leader, a, b, fl, fa, fb);

        assertTableEquals(leader.where("Sentinel in 300, 301"), fl);
        assertTableEquals(a.where("ID in 1, 2"), fa);
        assertTableEquals(b.where("ID in 101"), fb);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(10, 11), longCol("ID", 3, 4), intCol("Sentinel", 105, 106), col("Key", "b", "b"));
            a.notifyListeners(i(10, 11), i(), i());

            TstUtils.addToTable(b, i(11), longCol("ID", 110), intCol("Sentinel", 202), col("Key", "b"));
            b.notifyListeners(i(11), i(), i());

            TstUtils.addToTable(leader, i(13), longCol("A", 4), longCol("B", 110), col("Key", "b"),
                    intCol("Sentinel", 304));
            leader.notifyListeners(i(13), i(), i());
        });

        dumpTables(leader, a, b, fl, fa, fb);

        assertTableEquals(leader.where("Sentinel in 300, 304"), fl);
        assertTableEquals(a.where("ID in 1, 4"), fa);
        assertTableEquals(b.where("ID in 101, 110"), fb);
    }

    @Test
    public void testAllNullFollowers() {
        final QueryTable leader = TstUtils.testRefreshingTable(col("Key", "a", "b"), longCol("A", 1, NULL_LONG),
                longCol("B", 101, NULL_LONG), intCol("Sentinel", 300, 301));

        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 2, 3), intCol("Sentinel", 101, 102, 103),
                col("Key", "a", "a", "a"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 100), intCol("Sentinel", 201), col("Key", "a"));

        final String leaderName = "POTUS";

        final LeaderTableFilter.TableBuilder builder =
                new LeaderTableFilter.TableBuilder(leader.assertAddOnly(), "Key").setLeaderName(leaderName);
        builder.addTable("a", a.assertAddOnly(), "A=ID", "Key");
        builder.addTable("b", b.assertAddOnly(), "B=ID", "Key");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final LeaderTableFilter.Results<Table> results = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", leaderName)), results.keySet());

        final Table fl = results.get(leaderName);
        final Table fa = results.get("a");
        final Table fb = results.get("b");

        dumpTables(leader, a, b, fl, fa, fb);

        assertTableEquals(leader.where("Sentinel in 301"), fl);
        assertTableEquals(a.head(0), fa);
        assertTableEquals(b.head(0), fb);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(leader, i(10), longCol("A", NULL_LONG), longCol("B", NULL_LONG),
                    intCol("Sentinel", 302), col("Key", "b"));
            leader.notifyListeners(i(10), i(), i());
        });

        dumpTables(leader, a, b, fl, fa, fb);

        assertTableEquals(leader.where("Sentinel in 302"), fl);
        assertTableEquals(a.head(0), fa);
        assertTableEquals(b.head(0), fb);

        // let's allow "a" to exist now

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(11), longCol("ID", 101), intCol("Sentinel", 202), col("Key", "a"));
            b.notifyListeners(i(11), i(), i());
        });

        dumpTables(leader, a, b, fl, fa, fb);

        assertTableEquals(leader.where("Sentinel in 302, 300"), fl);
        assertTableEquals(a.where("ID in 1"), fa);
        assertTableEquals(b.where("ID in 101"), fb);

        // and create a b in both followers
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(12), longCol("ID", 1000), intCol("Sentinel", 203), col("Key", "b"));
            b.notifyListeners(i(12), i(), i());

            TstUtils.addToTable(a, i(12), longCol("ID", 500), intCol("Sentinel", 203), col("Key", "b"));
            a.notifyListeners(i(12), i(), i());
        });

        assertTableEquals(leader.where("Sentinel in 302, 300"), fl);
        assertTableEquals(a.where("ID in 1"), fa);
        assertTableEquals(b.where("ID in 101"), fb);

        // now add that to the leader

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(leader, i(11), longCol("A", 500), longCol("B", 1000), intCol("Sentinel", 303),
                    col("Key", "b"));
            leader.notifyListeners(i(11), i(), i());
        });

        assertTableEquals(leader.where("Sentinel in 303, 300"), fl);
        assertTableEquals(a.where("ID in 1, 500"), fa);
        assertTableEquals(b.where("ID in 101, 1000"), fb);

        // let's make it null again
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(leader, i(12), longCol("A", NULL_LONG), longCol("B", NULL_LONG),
                    intCol("Sentinel", 304), col("Key", "b"));
            leader.notifyListeners(i(12), i(), i());
        });

        assertTableEquals(leader.where("Sentinel in 304, 300"), fl);
        assertTableEquals(a.where("ID in 1"), fa);
        assertTableEquals(b.where("ID in 101"), fb);
    }

    @Test
    public void testMissingFollowerRows() {
        final QueryTable leader = TstUtils.testRefreshingTable(col("Key", "b", "b", "b"), longCol("A", 1, 2, 3),
                longCol("B", 101, 102, 103), intCol("Sentinel", 300, 301, 302));

        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 2, 3), intCol("Sentinel", 101, 102, 103),
                col("Key", "b", "b", "b"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 101), intCol("Sentinel", 201), col("Key", "b"));

        final LeaderTableFilter.TableBuilder builder =
                new LeaderTableFilter.TableBuilder(leader.assertAddOnly(), "Key");
        builder.addTable("a", a.assertAddOnly(), "A=ID", "Key");
        builder.addTable("b", b.assertAddOnly(), "B=ID", "Key");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final LeaderTableFilter.Results<Table> results = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", LeaderTableFilter.DEFAULT_LEADER_NAME)), results.keySet());

        final Table fl = results.get(LeaderTableFilter.DEFAULT_LEADER_NAME);
        final Table fa = results.get("a");
        final Table fb = results.get("b");

        dumpTables(leader, a, b, fl, fa, fb);

        assertTableEquals(leader.where("Sentinel in 300"), fl);
        assertTableEquals(a.where("Sentinel in 101"), fa);
        assertTableEquals(b.where("Sentinel in 201"), fb);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(10), longCol("ID", 103), intCol("Sentinel", 202), col("Key", "b"));
            b.notifyListeners(i(10), i(), i());
        });

        dumpTables(leader, a, b, fl, fa, fb);

        assertTableEquals(leader.where("Sentinel in 302"), fl);
        assertTableEquals(a.where("Sentinel in 103"), fa);
        assertTableEquals(b.where("Sentinel in 202"), fb);
    }

    @Test
    public void testFollowerFirst() {
        final QueryTable leader = TstUtils.testRefreshingTable(col("Key", "a"), longCol("A", 1), longCol("B", 101),
                intCol("Sentinel", 300));

        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 101), intCol("Sentinel", 101), col("Key", "b"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 102), intCol("Sentinel", 201), col("Key", "b"));

        final LeaderTableFilter.TableBuilder builder =
                new LeaderTableFilter.TableBuilder(leader.assertAddOnly(), "Key");
        builder.addTable("a", a.assertAddOnly(), "A=ID", "Key");
        builder.addTable("b", b.assertAddOnly(), "B=ID", "Key");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final LeaderTableFilter.Results<Table> results = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", LeaderTableFilter.DEFAULT_LEADER_NAME)), results.keySet());

        final Table fl = results.get(LeaderTableFilter.DEFAULT_LEADER_NAME);
        final Table fa = results.get("a");
        final Table fb = results.get("b");

        assertTableEquals(leader.head(0), fl);
        assertTableEquals(a.head(0), fa);
        assertTableEquals(b.head(0), fb);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(10), longCol("ID", 103), intCol("Sentinel", 202), col("Key", "b"));
            b.notifyListeners(i(10), i(), i());
        });

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(5), longCol("ID", 104), intCol("Sentinel", 204), col("Key", "b"));
            b.notifyListeners(i(5), i(), i());
        });

        assertTableEquals(leader.head(0), fl);
        assertTableEquals(a.head(0), fa);
        assertTableEquals(b.head(0), fb);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(leader, i(1), longCol("B", 104), longCol("A", 101), intCol("Sentinel", 301),
                    col("Key", "b"));
            leader.notifyListeners(i(1), i(), i());
        });

        dumpTables(leader, a, b, fl, fa, fb);

        assertTableEquals(leader.where("Sentinel in 301"), fl);
        assertTableEquals(a.where("Sentinel in 101"), fa);
        assertTableEquals(b.where("Sentinel in 204"), fb);
    }

    private void dumpTables(QueryTable leader, QueryTable a, QueryTable b, Table fl, Table fa, Table fb) {
        System.out.println("Raw Leader:");
        TableTools.showWithRowSet(leader);
        System.out.println("Raw A:");
        TableTools.showWithRowSet(a);
        System.out.println("Raw B:");
        TableTools.showWithRowSet(b);
        System.out.println("Filtered Leader:");
        TableTools.showWithRowSet(fl);
        System.out.println("Filtered A:");
        TableTools.showWithRowSet(fa);
        System.out.println("Filtered B:");
        TableTools.showWithRowSet(fb);
    }

    @Test
    public void testBuilderErrors() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // Test leader table without key columns
        try {
            new LeaderTableFilter.TableBuilder(emptyTable(1).update("A=1", "B=2", "D=3"), "A", "B", "C");
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("Leader table does not have all key columns [C]", iae.getMessage());
        }

        // Test follower table without key columns
        try {
            new LeaderTableFilter.TableBuilder(emptyTable(1).update("A=1", "B=2", "C=(long)3"), "A", "B")
                    .addTable("c", emptyTable(0).updateView("A=1", "B=2", "C=3"), "C", "A", "B", "D");
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("Table \"c\" has missing key columns [D]", iae.getMessage());
        }

        // Test follower table without ID column
        try {
            new LeaderTableFilter.TableBuilder(emptyTable(1).update("A=1", "B=2", "C=(long)3"), "A", "B")
                    .addTable("c", emptyTable(0).updateView("A=1", "B=2", "C=3"), "C=D", "A", "B");
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("Table \"c\" does not have ID column \"D\"", iae.getMessage());
        }

        // Test leader table w/o ID for follower
        try {
            new LeaderTableFilter.TableBuilder(emptyTable(1).update("A=1", "B=2", "C=(long)3"), "A", "B")
                    .addTable("c", emptyTable(0).updateView("A=1", "B=2", "E=3"), "D=E", "A", "B");
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("Leader table does not have ID column D", iae.getMessage());
        }

        // Test duplicate name
        try {
            new LeaderTableFilter.TableBuilder(emptyTable(1).update("A=1", "B=2", "C=(long)3"), "A", "B")
                    .addTable("c", emptyTable(0).updateView("A=1", "B=2", "E=3"), "C=E", "A", "B")
                    .addTable("c", emptyTable(0).updateView("A=1", "B=2", "E=3"), "C=E", "A", "B");
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("Duplicate follower table name \"c\"", iae.getMessage());
        }

        // Test leader name
        try {
            new LeaderTableFilter.TableBuilder(emptyTable(1).update("A=1", "B=2", "C=(long)3"), "A", "B")
                    .setLeaderName("simon")
                    .addTable("simon", emptyTable(0).updateView("A=1", "B=2", "E=3"), "C=E", "A", "B");
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("Conflict with leader name \"simon\"", iae.getMessage());
        }

        // no followers
        try {
            new LeaderTableFilter.TableBuilder(emptyTable(1).update("A=1", "B=2", "C=(long)3"), "A", "B").build();
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("You must specify follower tables as parameters to the LeaderTableFilter.Builder",
                    iae.getMessage());
        }

        // key compatibility
        try {
            new LeaderTableFilter.TableBuilder(emptyTable(1).update("A=(byte)1", "B=2", "C=(long)3"), "A", "B")
                    .addTable("simon", emptyTable(0).updateView("A=1", "B=`2`", "E=3"), "C=E", "A", "B").build();
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("Key sources are not compatible for simon (int, String) and leader (byte, int)",
                    iae.getMessage());
        }

        try {
            new LeaderTableFilter.TableBuilder(emptyTable(1).update("A=(byte)1", "B=2", "C=(long)3"), "A", "B")
                    .addTable("simon", emptyTable(0).updateView("A=1", "E=3"), "C=E", "A").build();
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("Key sources are not compatible for simon (int) and leader (byte, int)", iae.getMessage());
        }

        // not add only leader
        try {
            new LeaderTableFilter.TableBuilder(TstUtils.testRefreshingTable(longCol("ID", 1)))
                    .addTable("c", emptyTable(0).updateView("ID=1"), "ID").build();
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("Leader table must be add only!", iae.getMessage());
        }

        // not add only follower
        try {
            updateGraph.sharedLock()
                    .computeLocked(() -> new LeaderTableFilter.TableBuilder(emptyTable(0).updateView("ID=1"))
                            .addTable("c", TstUtils.testRefreshingTable(longCol("ID", 1)), "ID").build());
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("All follower tables must be add only! Table c is not add only.", iae.getMessage());
        }

        // too many followers
        final LeaderTableFilter.TableBuilder builder =
                new LeaderTableFilter.TableBuilder(emptyTable(0).updateView("ID=(long)1"));
        for (int ii = 0; ii < 33; ++ii) {
            builder.addTable("x" + ii, emptyTable(0).updateView("ID=(long)1"), "ID");
        }
        try {
            updateGraph.sharedLock().computeLocked(builder::build);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("Only 32 follower tables are supported!", iae.getMessage());
        }

        // wrong leader id type
        final LeaderTableFilter.TableBuilder builder2 =
                new LeaderTableFilter.TableBuilder(emptyTable(0).updateView("ID=1"))
                        .addTable("x", emptyTable(0).updateView("ID=(long)1"), "ID");
        try {
            updateGraph.sharedLock().computeLocked(builder2::build);
            fail("expected exception");
        } catch (ClassCastException cce) {
            assertEquals("Cannot convert ColumnSource[ID] of type int to type long", cce.getMessage());
        }
        // wrong follower id type
        final LeaderTableFilter.TableBuilder builder3 =
                new LeaderTableFilter.TableBuilder(emptyTable(0).updateView("ID=(long)1"))
                        .addTable("x", emptyTable(0).updateView("ID=1"), "ID");
        try {
            updateGraph.sharedLock().computeLocked(builder3::build);
            fail("expected exception");
        } catch (ClassCastException cce) {
            assertEquals("Cannot convert ColumnSource[ID] of type int to type long", cce.getMessage());
        }

    }

    private static class ErrorListener extends io.deephaven.engine.table.impl.ErrorListener {
        public ErrorListener(Table table) {
            super(table);
        }

        @Override
        public void onUpdate(TableUpdate upstream) {
            TestCase.fail("Expected exception.");
        }
    }

    @Test
    public void testLeaderErrorPropagation() {
        testErrorPropagation(true);
    }

    @Test
    public void testFollowerErrorPropagation() {
        testErrorPropagation(false);
    }

    private void testErrorPropagation(boolean leaderError) {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final Table leader = TstUtils.testRefreshingTable(longCol("ID"), intCol("Sentinel")).assertAddOnly();
        final Table a = TstUtils.testRefreshingTable(longCol("ID"), intCol("Sentinel")).assertAddOnly();

        final LeaderTableFilter.TableBuilder builder = new LeaderTableFilter.TableBuilder(leader);
        builder.addTable("a", a, "ID");
        final LeaderTableFilter.Results<Table> results = updateGraph.sharedLock().computeLocked(builder::build);

        final Table fl = results.get(LeaderTableFilter.DEFAULT_LEADER_NAME);
        final Table fa = results.get("a");

        final ErrorListener la = new ErrorListener(fa);
        fa.addUpdateListener(la);
        final ErrorListener ll = new ErrorListener(fl);
        fl.addUpdateListener(ll);

        framework.allowingError(() -> {
            updateGraph.startCycleForUnitTests();
            if (leaderError) {
                ((BaseTable) leader).notifyListenersOnError(new RuntimeException("Yo!"), null);
            } else {
                ((BaseTable) a).notifyListenersOnError(new RuntimeException("Yo!"), null);
            }
            updateGraph.completeCycleForUnitTests();
        }, errs -> {
            assertEquals(1, errs.size());
            final Throwable throwable = errs.get(0);
            assertEquals(RuntimeException.class, throwable.getClass());
            assertEquals("Yo!", throwable.getMessage());
            return true;
        });

        Assert.assertNotNull(la.originalException());
        Assert.assertNotNull(ll.originalException());
        assertEquals("Yo!", la.originalException().getMessage());
        assertEquals("Yo!", ll.originalException().getMessage());
    }

    @Test
    public void testPartitioned() {
        final QueryTable leader = TstUtils.testRefreshingTable(RowSetFactory.flat(10).toTracking(),
                col("Partition", "A", "A", "B", "B", "C", "C", "C", "D", "D", "D"),
                longCol("ID", 1, 2, /* B */ 1, 2, /* C */ 1, 2, 2, /* D */ 1, 2, 2),
                intCol("Sentinel", 101, 102, 103, 104, 105, 106, 107, 108, 109, 110));

        final QueryTable follower = TstUtils.testRefreshingTable(RowSetFactory.flat(5).toTracking(),
                col("Division", "A", "A", "B", "C", "C"),
                longCol("ID", 2, 3, 1, 2, 2),
                intCol("Sentinel", 201, 202, 203, 204, 205));

        final PartitionedTable leaderMap = leader.assertAddOnly().updateView("SK1=k").partitionBy("Partition");
        final PartitionedTable followerMap = follower.assertAddOnly().updateView("SK2=k").partitionBy("Division");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final LeaderTableFilter.Results<Table> bykey = updateGraph.sharedLock()
                .computeLocked(() -> new LeaderTableFilter.TableBuilder(leader.assertAddOnly(), "Partition")
                        .addTable("source1", follower.assertAddOnly(), "ID", "Division").build());
        final Table lf = bykey.get(LeaderTableFilter.DEFAULT_LEADER_NAME);
        final Table s1f = bykey.get("source1");
        TableTools.showWithRowSet(lf);
        TableTools.showWithRowSet(s1f);

        final LeaderTableFilter.Results<PartitionedTable> filteredByPartition =
                updateGraph.sharedLock().computeLocked(() -> new LeaderTableFilter.PartitionedTableBuilder(leaderMap)
                        .addPartitionedTable("source1", followerMap, "ID").build());
        assertEquals(new HashSet<>(Arrays.asList(LeaderTableFilter.DEFAULT_LEADER_NAME, "source1")),
                filteredByPartition.keySet());

        final Table leaderMerged = filteredByPartition.get(LeaderTableFilter.DEFAULT_LEADER_NAME).merge();
        final Table s1merged = filteredByPartition.get("source1").merge();
        final Table leaderMergedSorted = leaderMerged.sort("SK1").dropColumns("SK1");
        final Table s1mergedSorted = s1merged.sort("SK2").dropColumns("SK2");

        assertTableEquals(lf, leaderMergedSorted);
        assertTableEquals(s1f, s1mergedSorted);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(follower, i(10, 11), col("Division", "D", "B"), longCol("ID", 2, 2),
                    intCol("Sentinel", 206, 207));
            follower.notifyListeners(i(10, 11), i(), i());
        });

        assertTableEquals(lf, leaderMergedSorted);
        assertTableEquals(s1f, s1mergedSorted);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(follower, i(12, 13), col("Division", "D", "E"), longCol("ID", 3, 3),
                    intCol("Sentinel", 208, 209));
            follower.notifyListeners(i(12, 13), i(), i());
            addToTable(leader, i(10, 11, 12), col("Partition", "D", "D", "E"), longCol("ID", 3, 4, 3),
                    intCol("Sentinel", 111, 112, 113));
            leader.notifyListeners(i(10, 11, 12), i(), i());
        });

        assertTableEquals(lf, leaderMergedSorted);
        assertTableEquals(s1f, s1mergedSorted);
    }

    @Test
    public void testPartitionedRandomized() {
        for (int seed = 0; seed < 10; ++seed) {
            System.out.println("Seed = " + seed);
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                ChunkPoolReleaseTracking.enableStrict();
                testPartitionedRandomized(seed, 10, 10, 100);
                ChunkPoolReleaseTracking.checkAndDisable();
            }
        }
    }

    @Test
    public void testPartitionedRandomizedLarge() {
        for (int seed = 0; seed < 1; ++seed) {
            System.out.println("Seed = " + seed);
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                ChunkPoolReleaseTracking.enableStrict();
                testPartitionedRandomized(seed, 1000, 100, 10);
                ChunkPoolReleaseTracking.checkAndDisable();
            }
        }
    }

    private void testPartitionedRandomized(int seed, int size, int stepSize, int maxSteps) {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final Random random = new Random(seed);
        final ColumnInfo[] columnInfo1;
        final ColumnInfo[] columnInfo2;
        final ColumnInfo[] columnInfoSet1;
        final ColumnInfo[] columnInfoSet2;
        final QueryTable source1Unfiltered = getTable(size, random,
                columnInfo1 = initColumnInfos(new String[] {"Partition", "ID", "Sentinel", "Truthy"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IncreasingSortedLongGenerator(2, 1000),
                        new IntGenerator(0, 1000000),
                        new BooleanGenerator()));

        final QueryTable source2Unfiltered = getTable(size, random,
                columnInfo2 = initColumnInfos(new String[] {"Partition", "ID", "Sentinel", "Truthy"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IncreasingSortedLongGenerator(2, 1000),
                        new IntGenerator(0, 1000000),
                        new BooleanGenerator()));

        final QueryTable filterSet1 = getTable(1, random, columnInfoSet1 =
                initColumnInfos(new String[] {"Partition"}, new SetGenerator<>("a", "b", "c", "d", "e")));
        final QueryTable filterSet2 = getTable(1, random, columnInfoSet2 =
                initColumnInfos(new String[] {"Partition"}, new SetGenerator<>("a", "b", "c", "d", "e")));

        final Table dummy = TableTools.newTable(col("Partition", "A"), longCol("ID", 0), intCol("Sentinel", 12345678),
                col("Truthy", true));

        final Table source1 =
                updateGraph.sharedLock()
                        .computeLocked(() -> TableTools.merge(dummy,
                                source1Unfiltered.whereIn(filterSet1, "Partition").update("Truthy=!!Truthy")))
                        .assertAddOnly();
        final Table source2 =
                updateGraph.sharedLock()
                        .computeLocked(() -> TableTools.merge(dummy,
                                source2Unfiltered.whereIn(filterSet2, "Partition").update("Truthy=!!Truthy")))
                        .assertAddOnly();

        final PartitionedTable sm1 = source1.updateView("SK1=k").partitionBy("Partition");
        final PartitionedTable sm2 = source2.updateView("SK2=k").partitionBy("Partition");

        final LeaderTableFilter.Results<Table> bykey =
                updateGraph.sharedLock().computeLocked(() -> new LeaderTableFilter.TableBuilder(source1, "Partition")
                        .setLeaderName("source1").addTable("source2", source2, "ID").build());
        final Table s1f = bykey.get("source1");
        final Table s2f = bykey.get("source2");

        final LeaderTableFilter.Results<Table> bykey2 = updateGraph.sharedLock()
                .computeLocked(() -> new LeaderTableFilter.TableBuilder(source1, "Partition", "Truthy")
                        .setLeaderName("source1").addTable("source2", source2, "ID").build());
        final Table s1fKeyed = bykey2.get("source1");
        final Table s2fKeyed = bykey2.get("source2");

        final LeaderTableFilter.Results<PartitionedTable> filteredByPartition = updateGraph.sharedLock()
                .computeLocked(() -> new LeaderTableFilter.PartitionedTableBuilder(sm1).setBinarySearchThreshold(16)
                        .setLeaderName("source1").addPartitionedTable("source2", sm2, "ID").build());
        final LeaderTableFilter.Results<PartitionedTable> filteredByPartitionKeyed =
                updateGraph.sharedLock().computeLocked(
                        () -> new LeaderTableFilter.PartitionedTableBuilder(sm1, "Truthy").setBinarySearchThreshold(16)
                                .setLeaderName("source1").addPartitionedTable("source2", sm2, "ID").build());

        final PartitionedTable s1tm = filteredByPartition.get("source1");
        final PartitionedTable s2tm = filteredByPartition.get("source2");

        final PartitionedTable s1tmKeyed = filteredByPartitionKeyed.get("source1");
        final PartitionedTable s2tmKeyed = filteredByPartitionKeyed.get("source2");

        final Table s1merged = s1tm.merge();
        final Table s2merged = s2tm.merge();
        final Table s1mergedSorted = s1merged.sort("SK1").dropColumns("SK1");
        final Table s2mergedSorted = s2merged.sort("SK2").dropColumns("SK2");

        final Table s1KeyedMerged = s1tmKeyed.merge();
        final Table s2KeyedMerged = s2tmKeyed.merge();
        final Table s1KeyedMergedSorted = s1KeyedMerged.sort("SK1").dropColumns("SK1");
        final Table s2KeyedMergedSorted = s2KeyedMerged.sort("SK2").dropColumns("SK2");

        assertTableEquals(s1f, s1mergedSorted);
        assertTableEquals(s2f, s2mergedSorted);

        assertTableEquals(s1fKeyed, s1KeyedMergedSorted);
        assertTableEquals(s2fKeyed, s2KeyedMergedSorted);

        for (int step = 0; step < maxSteps; ++step) {
            // if (printTableUpdates()) {
            // System.out.println("Seed = " + seed + ", step=" + step);
            // }
            updateGraph.runWithinUnitTestCycle(() -> {
                if (random.nextInt(10) == 0) {
                    GenerateTableUpdates.generateAppends(stepSize, random, filterSet1, columnInfoSet1);
                }
                if (random.nextInt(10) == 0) {
                    GenerateTableUpdates.generateAppends(stepSize, random, filterSet2, columnInfoSet2);
                }
                // append to table 1
                GenerateTableUpdates.generateAppends(stepSize, random, source1Unfiltered, columnInfo1);
                // append to table 2
                GenerateTableUpdates.generateAppends(stepSize / 2, random, source2Unfiltered, columnInfo2);
            });

            // if (printTableUpdates()) {
            // System.out.println("Source 1 (filtered, no table map)");
            // TableTools.showWithIndex(s1f);
            // System.out.println("Source 2 (filtered, no table map)");
            // TableTools.showWithIndex(s2f);
            //
            // System.out.println("Source 1 (tm)");
            // TableTools.showWithIndex(s1merged);
            // System.out.println("Source 2 (tm)");
            // TableTools.showWithIndex(s2merged);
            //
            // System.out.println("Source 1 Keyed (tm)");
            // TableTools.showWithIndex(s1KeyedMerged);
            // System.out.println("Source 2 (tm)");
            // TableTools.showWithIndex(s2KeyedMerged);
            // }

            assertTableEquals(s1f, s1mergedSorted);
            assertTableEquals(s2f, s2mergedSorted);

            assertTableEquals(s1fKeyed, s1KeyedMergedSorted);
            assertTableEquals(s2fKeyed, s2KeyedMergedSorted);
        }
    }

    @Test
    public void testJavaDocSyntax() {
        // these are meant to model the source from the Javadoc
        final Table syncLog = TableTools.emptyTable(1).update("session=`A`", "client=`B`", "msgId=7L", "execId=8L");
        final Table messageLog = TableTools.newTable(stringCol("SessionId", "A", "A", "C"),
                stringCol("client", "B", "B", "B"), longCol("id", 7, 7, 9), intCol("MSentinel", 1, 2, 3));
        final Table trades =
                TableTools.newTable(stringCol("SessionId", "A", "A"), stringCol("client", "B", "B"),
                        longCol("execId", 6, 8), intCol("TSentinel", 101, 102));

        // the next block is copied and pasted snippets from the Javadoc example
        final LeaderTableFilter.TableBuilder builder = new LeaderTableFilter.TableBuilder(syncLog, "client", "session");
        builder.addTable("messageLog", messageLog, "msgId=id", "client", "SessionId");
        builder.addTable("trades", trades, "execId", "client", "SessionId");

        LeaderTableFilter.Results<Table> result = builder.build();

        Table filteredMessageLog = result.get("messageLog");
        Table filteredTrades = result.get("trades");

        Table filteredLeader = result.get(io.deephaven.engine.util.LeaderTableFilter.DEFAULT_LEADER_NAME);

        // and since we bothered writing the test, we should validate it has what we want
        assertTableEquals(messageLog.where("id=7"), filteredMessageLog);
        assertTableEquals(trades.where("execId=8"), filteredTrades);
        assertTableEquals(syncLog, filteredLeader);
    }
}
