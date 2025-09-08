//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
        final Map<String, Table> result = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", LeaderTableFilter.DEFAULT_LEADER_NAME)), result.keySet());

        final Table fl = result.get(LeaderTableFilter.DEFAULT_LEADER_NAME);
        final Table fa = result.get("a");
        final Table fb = result.get("b");

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
        final Map<String, Table> result = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", leaderName)), result.keySet());

        final Table fl = result.get(leaderName);
        final Table fa = result.get("a");
        final Table fb = result.get("b");

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
        final Map<String, Table> result = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", LeaderTableFilter.DEFAULT_LEADER_NAME)), result.keySet());

        final Table fl = result.get(LeaderTableFilter.DEFAULT_LEADER_NAME);
        final Table fa = result.get("a");
        final Table fb = result.get("b");

        // TODO
        // final FailureListener leaderListener = new FailureListener("fl");
        // ((DynamicTable)fl).listenForUpdates(leaderListener);
        // final FailureListener aListener = new FailureListener("a");
        // ((DynamicTable)fa).listenForUpdates(aListener);
        // final FailureListener bListener = new FailureListener("b");
        // ((DynamicTable)fb).listenForUpdates(bListener);

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
        final Map<String, Table> result = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", leaderName)), result.keySet());

        final Table fl = result.get(leaderName);
        final Table fa = result.get("a");
        final Table fb = result.get("b");

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
        final Map<String, Table> result = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", leaderName)), result.keySet());

        final Table fl = result.get(leaderName);
        final Table fa = result.get("a");
        final Table fb = result.get("b");

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
        final Map<String, Table> result = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", LeaderTableFilter.DEFAULT_LEADER_NAME)), result.keySet());

        final Table fl = result.get(LeaderTableFilter.DEFAULT_LEADER_NAME);
        final Table fa = result.get("a");
        final Table fb = result.get("b");

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
        final Map<String, Table> result = updateGraph.sharedLock().computeLocked(builder::build);

        assertEquals(new HashSet<>(Arrays.asList("a", "b", LeaderTableFilter.DEFAULT_LEADER_NAME)), result.keySet());

        final Table fl = result.get(LeaderTableFilter.DEFAULT_LEADER_NAME);
        final Table fa = result.get("a");
        final Table fb = result.get("b");

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
        final Map<String, Table> result = updateGraph.sharedLock().computeLocked(builder::build);

        final Table fl = result.get(LeaderTableFilter.DEFAULT_LEADER_NAME);
        final Table fa = result.get("a");

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
}
