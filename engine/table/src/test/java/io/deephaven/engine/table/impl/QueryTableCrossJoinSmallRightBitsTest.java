package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.exceptions.OutOfKeySpaceException;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Arrays;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.table.impl.TstUtils.c;
import static io.deephaven.engine.table.impl.TstUtils.i;
import static io.deephaven.engine.table.impl.TstUtils.testRefreshingTable;
import static io.deephaven.engine.table.impl.TstUtils.testTable;

@Category(OutOfBandTest.class)
public class QueryTableCrossJoinSmallRightBitsTest extends QueryTableCrossJoinTestBase {
    public QueryTableCrossJoinSmallRightBitsTest() {
        super(1);
    }

    public void testIncrementalWithKeyColumnsShallow() {
        final int size = 10;

        for (int seed = 0; seed < 2000; ++seed) {
            testIncrementalWithKeyColumns("size == " + size, size, seed, new MutableInt(10));
        }
    }

    public void testZeroKeyOutOfKeySpace() {
        // idea here is that left uses LARGE keyspace and the right uses SMALL keyspace.
        // (61-bits on the left, 2-bits on the right)
        final QueryTable ltTable = testRefreshingTable(i(0, (1L << 61) - 1).toTracking(), c("A", 1, 2));
        final QueryTable lsTable = testTable(i(0, (1L << 61) - 1).toTracking(), c("A", 1, 2));
        final QueryTable rtTable = testRefreshingTable(i(0, 1, 2, 3).toTracking(), c("B", 1, 2, 3, 4));
        final QueryTable rsTable = testTable(i(0, 1, 2, 3).toTracking(), c("B", 1, 2, 3, 4));

        for (final QueryTable left : new QueryTable[] {ltTable, lsTable}) {
            for (final QueryTable right : new QueryTable[] {rtTable, rsTable}) {
                if (left.isRefreshing() || right.isRefreshing()) {
                    boolean thrown = false;
                    try {
                        left.join(right);
                    } catch (OutOfKeySpaceException ignored) {
                        thrown = true;
                    }
                    Assert.eqTrue(thrown, "thrown");

                    // we can fit if we use min right bits
                    left.join(right, 1);
                } else {
                    left.join(right); // static - static should be OK because it always uses min right bits
                }
            }
        }
    }

    public void testKeyColumnOutOfKeySpace() {
        // idea here is that left uses LARGE keyspace and the right uses SMALL keyspace.
        // (62-bits on the left, 1-bit on the right (per group))
        final QueryTable ltTable = testRefreshingTable(i(0, (1L << 62) - 1).toTracking(), c("A", 1, 2));
        final QueryTable lsTable = testTable(i(0, (1L << 62) - 1).toTracking(), c("A", 1, 2));
        final QueryTable rtTable = testRefreshingTable(i(0, 1, 2, 3).toTracking(), c("B", 1, 2, 3, 4));
        final QueryTable rsTable = testTable(i(0, 1, 2, 3).toTracking(), c("B", 1, 2, 3, 4));

        for (final QueryTable left : new QueryTable[] {ltTable, lsTable}) {
            for (final QueryTable right : new QueryTable[] {rtTable, rsTable}) {
                if (left.isRefreshing() || right.isRefreshing()) {
                    boolean thrown = false;
                    try {
                        left.join(right, "A=B");
                    } catch (OutOfKeySpaceException ignored) {
                        thrown = true;
                    }
                    Assert.eqTrue(thrown, "thrown");

                    // we can fit if we use min right bits
                    left.join(right, "A=B", 1);
                } else {
                    left.join(right, "A=B"); // static - static should be OK because it always uses min right bits
                }
            }
        }
    }

    public void testLeftGroupChangesOnRightShift() {
        // On the step with the shift:
        // - one row to not change groups, but group gets smaller (grp 0)
        // - one row to change groups to another of smaller size (grp 2 -> 1)
        // - one row to change groups to another of same size (grp 2 -> 3)
        // - one row to change groups to another of larger size (grp 2 -> 4)
        // - one row to not change groups, but group gets larger (grp 5)
        final QueryTable lTable = testRefreshingTable(i(0, 1, 2, 3, 4).toTracking(), c("A", 0, 2, 2, 2, 5));
        final QueryTable rTable = testRefreshingTable(i().toTracking(), intCol("A"));
        int numRightBitsToReserve = 1;

        int[] sizes = new int[] {2, 1, 3, 3, 4, 4};
        for (int grp = 0; grp < sizes.length; ++grp) {
            int[] data = new int[sizes[grp]];
            Arrays.fill(data, grp);
            TstUtils.addToTable(rTable, RowSetFactory.fromRange(grp * 10, grp * 10 + data.length - 1),
                    intCol("A", data));
        }

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> lTable.join(rTable, "A", numRightBitsToReserve)),
        };
        TstUtils.validate(en);

        final QueryTable jt = (QueryTable) lTable.join(rTable, "A", numRightBitsToReserve);
        final io.deephaven.engine.table.impl.SimpleListener listener =
                new io.deephaven.engine.table.impl.SimpleListener(jt);
        jt.listenForUpdates(listener);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(lTable, i(1, 2, 3), c("A", 1, 3, 4));

            final TableUpdateImpl lUpdate = new TableUpdateImpl();
            lUpdate.modified = i(1, 2, 3);
            lUpdate.added = i();
            lUpdate.removed = i();
            lUpdate.modifiedColumnSet = ModifiedColumnSet.ALL;
            lUpdate.shifted = RowSetShiftData.EMPTY;
            lTable.notifyListeners(lUpdate);

            TstUtils.removeRows(rTable, i(1));
            TstUtils.addToTable(rTable, i(54), c("A", 5));

            final TableUpdateImpl rUpdate = new TableUpdateImpl();
            rUpdate.added = i(54);
            rUpdate.removed = i(1);
            rUpdate.modified = i();
            rUpdate.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            rUpdate.shifted = RowSetShiftData.EMPTY;
            rTable.notifyListeners(rUpdate);
        });
        TstUtils.validate(en);
    }

    public void testLeftGroupChangesOnRightShiftWithAllInnerShifts() {
        // This test is similar to the above, but has at least one inner shift on every group (which hits different
        // logic).

        // On the step with the shift:
        // - one row to not change groups, but group gets smaller (grp 0)
        // - one row to change groups to another of smaller size (grp 2 -> 1)
        // - one row to change groups to another of same size (grp 2 -> 3)
        // - one row to change groups to another of larger size (grp 2 -> 4)
        // - one row to not change groups, but group gets larger (grp 5)
        final QueryTable lTable = testRefreshingTable(i(0, 1, 2, 3, 4).toTracking(), c("A", 0, 2, 2, 2, 5));
        final QueryTable rTable = testRefreshingTable(i().toTracking(), intCol("A"));
        int numRightBitsToReserve = 1;

        int[] sizes = new int[] {3, 2, 3, 3, 3, 4};
        for (int grp = 0; grp < sizes.length; ++grp) {
            int[] data = new int[sizes[grp]];
            Arrays.fill(data, grp);
            TstUtils.addToTable(rTable, RowSetFactory.fromRange(grp * 10, grp * 10 + data.length - 1),
                    intCol("A", data));
        }

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> lTable.join(rTable, "A", numRightBitsToReserve)),
        };
        TstUtils.validate(en);

        final QueryTable jt = (QueryTable) lTable.join(rTable, "A", numRightBitsToReserve);
        final io.deephaven.engine.table.impl.SimpleListener listener =
                new io.deephaven.engine.table.impl.SimpleListener(jt);
        jt.listenForUpdates(listener);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(lTable, i(1, 2, 3), c("A", 1, 3, 4));

            final TableUpdateImpl lUpdate = new TableUpdateImpl();
            lUpdate.modified = i(1, 2, 3);
            lUpdate.added = i();
            lUpdate.removed = i();
            lUpdate.modifiedColumnSet = ModifiedColumnSet.ALL;
            lUpdate.shifted = RowSetShiftData.EMPTY;
            lTable.notifyListeners(lUpdate);

            TstUtils.removeRows(rTable, i(1, 10));
            TstUtils.addToTable(rTable, i(21, 31, 41, 51, 54, 55), c("A", 1, 2, 3, 4, 5, 5));

            final TableUpdateImpl rUpdate = new TableUpdateImpl();
            rUpdate.added = i(54, 55);
            rUpdate.removed = i(1, 10);
            rUpdate.modified = i(21, 31, 41, 51);
            rUpdate.modifiedColumnSet = rTable.newModifiedColumnSet("A");
            rUpdate.shifted = RowSetShiftData.EMPTY;
            rTable.notifyListeners(rUpdate);
        });
        TstUtils.validate(en);
    }

    public void testLeftGroupChangesOnBothShift() {
        // On the step with the shift:
        // - one row to not change groups, but group gets smaller (grp 0)
        // - one row to change groups to another of smaller size (grp 2 -> 1)
        // - one row to change groups to another of same size (grp 2 -> 3)
        // - one row to change groups to another of larger size (grp 2 -> 4)
        // - one row to not change groups, but group gets larger (grp 5)
        final QueryTable lTable = testRefreshingTable(i(0, 1, 2, 3, 4).toTracking(), c("A", 0, 2, 2, 2, 5));
        final QueryTable rTable = testRefreshingTable(i().toTracking(), intCol("A"));
        int numRightBitsToReserve = 1;

        int[] sizes = new int[] {2, 1, 3, 3, 4, 4};
        for (int grp = 0; grp < sizes.length; ++grp) {
            int[] data = new int[sizes[grp]];
            Arrays.fill(data, grp);
            TstUtils.addToTable(rTable, RowSetFactory.fromRange(grp * 10, grp * 10 + data.length - 1),
                    intCol("A", data));
        }

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> lTable.join(rTable, "A", numRightBitsToReserve)),
        };
        TstUtils.validate(en);

        final QueryTable jt = (QueryTable) lTable.join(rTable, "A", numRightBitsToReserve);
        final io.deephaven.engine.table.impl.SimpleListener listener =
                new io.deephaven.engine.table.impl.SimpleListener(jt);
        jt.listenForUpdates(listener);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(lTable, i(0));
            TstUtils.addToTable(lTable, i(1, 2, 3, 4, 5), c("A", 0, 1, 3, 4, 5));

            final TableUpdateImpl lUpdate = new TableUpdateImpl();
            lUpdate.modified = i(2, 3, 4);
            lUpdate.added = i();
            lUpdate.removed = i();
            lUpdate.modifiedColumnSet = ModifiedColumnSet.ALL;
            final RowSetShiftData.Builder leftShifted = new RowSetShiftData.Builder();
            leftShifted.shiftRange(0, 4, 1);
            lUpdate.shifted = leftShifted.build();
            lTable.notifyListeners(lUpdate);

            TstUtils.removeRows(rTable, i(1));
            TstUtils.addToTable(rTable, i(54), c("A", 5));

            final TableUpdateImpl rUpdate = new TableUpdateImpl();
            rUpdate.added = i(54);
            rUpdate.removed = i(1);
            rUpdate.modified = i();
            rUpdate.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            rUpdate.shifted = RowSetShiftData.EMPTY;
            rTable.notifyListeners(rUpdate);
        });
        TstUtils.validate(en);
    }

    public void testLeftGroupChangesOnBothShiftWithInnerShifts() {
        // This test is similar to the above, but has at least one inner shift on every group (which hits different
        // logic).

        // On the step with the shift:
        // - one row to not change groups, but group gets smaller (grp 0)
        // - one row to change groups to another of smaller size (grp 2 -> 1)
        // - one row to change groups to another of same size (grp 2 -> 3)
        // - one row to change groups to another of larger size (grp 2 -> 4)
        // - one row to not change groups, but group gets larger (grp 5)
        final QueryTable lTable = testRefreshingTable(i(0, 1, 2, 3, 4).toTracking(), c("A", 0, 2, 2, 2, 5));
        final QueryTable rTable = testRefreshingTable(i().toTracking(), intCol("A"));
        int numRightBitsToReserve = 1;

        int[] sizes = new int[] {2, 1, 3, 3, 4, 4};
        for (int grp = 0; grp < sizes.length; ++grp) {
            int[] data = new int[sizes[grp]];
            Arrays.fill(data, grp);
            TstUtils.addToTable(rTable, RowSetFactory.fromRange(grp * 10, grp * 10 + data.length - 1),
                    intCol("A", data));
        }

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> lTable.join(rTable, "A", numRightBitsToReserve)),
        };
        TstUtils.validate(en);

        final QueryTable jt = (QueryTable) lTable.join(rTable, "A", numRightBitsToReserve);
        final io.deephaven.engine.table.impl.SimpleListener listener =
                new io.deephaven.engine.table.impl.SimpleListener(jt);
        jt.listenForUpdates(listener);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(lTable, i(0));
            TstUtils.addToTable(lTable, i(1, 2, 3, 4, 5), c("A", 0, 1, 3, 4, 5));

            final TableUpdateImpl lUpdate = new TableUpdateImpl();
            lUpdate.modified = i(2, 3, 4);
            lUpdate.added = i();
            lUpdate.removed = i();
            lUpdate.modifiedColumnSet = ModifiedColumnSet.ALL;
            final RowSetShiftData.Builder leftShifted = new RowSetShiftData.Builder();
            leftShifted.shiftRange(0, 4, 1);
            lUpdate.shifted = leftShifted.build();
            lTable.notifyListeners(lUpdate);

            TstUtils.removeRows(rTable, i(1, 10));
            TstUtils.addToTable(rTable, i(21, 31, 41, 51, 54, 55), c("A", 1, 2, 3, 4, 5, 5));

            final TableUpdateImpl rUpdate = new TableUpdateImpl();
            rUpdate.added = i(54, 55);
            rUpdate.removed = i(1, 10);
            rUpdate.modified = i(21, 31, 41, 51);
            rUpdate.modifiedColumnSet = rTable.newModifiedColumnSet("A");
            rUpdate.shifted = RowSetShiftData.EMPTY;
            rTable.notifyListeners(rUpdate);
        });
        TstUtils.validate(en);
    }
}
