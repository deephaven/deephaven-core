//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.NaturalJoinType;
import io.deephaven.api.Selectable;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ResettableWritableIntChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.table.impl.sources.CrossJoinRightColumnSource;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.sources.IntTestSource;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.OuterJoinTools;
import io.deephaven.engine.util.PrintListener;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static org.junit.Assert.*;
import static java.util.Collections.emptyList;

@Category(OutOfBandTest.class)
public abstract class QueryTableCrossJoinTestBase extends QueryTableTestBase {

    private final int numRightBitsToReserve;

    public QueryTableCrossJoinTestBase(int numRightBitsToReserve) {
        this.numRightBitsToReserve = numRightBitsToReserve;
    }

    private ColumnInfo<?, ?>[] getIncrementalColumnInfo(final String prefix, int numGroups) {
        String[] names = new String[] {"Sym", "IntCol"};

        return initColumnInfos(Arrays.stream(names).map(name -> prefix + name).toArray(String[]::new),
                new IntGenerator(0, numGroups - 1),
                new IntGenerator(10, 100000));
    }

    @Test
    public void testZeroKeyJoinBitExpansionOnAdd() {
        // Looking to force our row set space to need more keys.
        final QueryTable lTable = testRefreshingTable(col("X", "to-remove", "b", "c", "d"));
        removeRows(lTable, i(0)); // row @ 0 does not need outer shifting
        final QueryTable rTable = testRefreshingTable(longCol("Y"));

        addToTable(rTable, i(1, (1 << 16) - 1), longCol("Y", 1, 2));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> lTable.join(rTable, emptyList(), emptyList(), numRightBitsToReserve)),
        };
        TstUtils.validate(en);

        final QueryTable jt = (QueryTable) lTable.join(rTable, emptyList(), emptyList(), numRightBitsToReserve);
        final io.deephaven.engine.table.impl.SimpleListener listener =
                new io.deephaven.engine.table.impl.SimpleListener(jt);
        jt.addUpdateListener(listener);

        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(() -> {
            addToTable(rTable, i(1 << 16), longCol("Y", 3));
            final TableUpdateImpl update = new TableUpdateImpl();
            update.added = i(1 << 16);
            update.removed = i();
            update.modified = i();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            update.shifted = RowSetShiftData.EMPTY;
            rTable.notifyListeners(update);
        });
        TstUtils.validate(en);

        // One shift: the entire left row's sub-table
        Assert.eq(listener.update.shifted().size(), "listener.update.shifted.size()", lTable.size(), "lTable.size()");
    }

    @Test
    public void testZeroKeyJoinBitExpansionOnBoundaryShift() {
        // Looking to force our row set space to need more keys.
        final QueryTable lTable = testRefreshingTable(col("X", "to-remove", "b", "c", "d"));
        removeRows(lTable, i(0)); // row @ 0 does not need outer shifting
        final QueryTable rTable = testRefreshingTable(longCol("Y"));

        final long origIndex = (1 << 16) - 1;
        final long newIndex = 1 << 16;
        addToTable(rTable, i(0, origIndex), longCol("Y", 1, 2));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> lTable.join(rTable, emptyList(), emptyList(), numRightBitsToReserve)),
        };
        TstUtils.validate(en);

        final QueryTable jt = (QueryTable) lTable.join(rTable, emptyList(), emptyList(), numRightBitsToReserve);
        final io.deephaven.engine.table.impl.SimpleListener listener =
                new io.deephaven.engine.table.impl.SimpleListener(jt);
        jt.addUpdateListener(listener);

        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(() -> {
            removeRows(rTable, i(origIndex));
            addToTable(rTable, i(newIndex), longCol("Y", 2));
            final TableUpdateImpl update = new TableUpdateImpl();
            update.added = i();
            update.removed = i();
            update.modified = i();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
            shiftBuilder.shiftRange(origIndex, origIndex, newIndex - origIndex);
            update.shifted = shiftBuilder.build();
            rTable.notifyListeners(update);
        });
        TstUtils.validate(en);

        // Two shifts: before upstream shift, upstream shift (note: post upstream shift not possible because it exceeds
        // known keyspace range)
        Assert.eq(listener.update.shifted().size(), "listener.update.shifted.size()", 2 * lTable.size(),
                "2 * lTable.size()");
    }

    @Test
    public void testZeroKeyJoinBitExpansionWithInnerShift() {
        // Looking to force our row set space to need more keys.
        final QueryTable lTable = testRefreshingTable(col("X", "to-remove", "b", "c", "d"));
        removeRows(lTable, i(0)); // row @ 0 does not need outer shifting
        final QueryTable rTable = testRefreshingTable(longCol("Y"));

        addToTable(rTable, i(1, 128, (1 << 16) - 1), longCol("Y", 1, 2, 3));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> lTable.join(rTable, emptyList(), emptyList(), numRightBitsToReserve)),
        };
        TstUtils.validate(en);

        final QueryTable jt = (QueryTable) lTable.join(rTable, emptyList(), emptyList(), numRightBitsToReserve);
        final io.deephaven.engine.table.impl.SimpleListener listener =
                new io.deephaven.engine.table.impl.SimpleListener(jt);
        jt.addUpdateListener(listener);

        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(() -> {
            removeRows(rTable, i(128));
            addToTable(rTable, i(129, 1 << 16), longCol("Y", 2, 4));
            final TableUpdateImpl update = new TableUpdateImpl();
            update.added = i(1 << 16);
            update.removed = i();
            update.modified = i();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
            shiftBuilder.shiftRange(128, 128, 1);
            update.shifted = shiftBuilder.build();
            rTable.notifyListeners(update);
        });
        TstUtils.validate(en);

        // Three shifts: before upstream shift, upstream shift, post upstream shift
        Assert.eq(listener.update.shifted().size(), "listener.update.shifted.size()", 3 * lTable.size(),
                "3 * lTable.size()");
    }

    @Test
    public void testZeroKeyJoinCompoundShift() {
        // rightTable shift, leftTable shift, and bit expansion
        final QueryTable lTable = testRefreshingTable(col("X", "a", "b", "c", "d"));
        final QueryTable rTable = testRefreshingTable(longCol("Y"));

        addToTable(rTable, i(1, 128, (1 << 16) - 1), longCol("Y", 1, 2, 3));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> lTable.join(rTable, emptyList(), emptyList(), numRightBitsToReserve)),
        };
        TstUtils.validate(en);

        // left table
        // right table
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(() -> {
            // left table
            removeRows(lTable, i(0, 1, 2, 3));
            addToTable(lTable, i(2, 4, 5, 7), col("X", "a", "b", "c", "d"));
            final TableUpdateImpl lUpdate = new TableUpdateImpl();
            lUpdate.added = i();
            lUpdate.removed = i();
            lUpdate.modified = i();
            final RowSetShiftData.Builder lShiftBuilder = new RowSetShiftData.Builder();
            lShiftBuilder.shiftRange(0, 0, 2);
            lShiftBuilder.shiftRange(1, 2, 3);
            lShiftBuilder.shiftRange(3, 1024, 4);
            lUpdate.shifted = lShiftBuilder.build();
            lUpdate.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            lTable.notifyListeners(lUpdate);

            // right table
            removeRows(rTable, i(128));
            addToTable(rTable, i(129, 1 << 16), longCol("Y", 2, 4));
            final TableUpdateImpl rUpdate = new TableUpdateImpl();
            rUpdate.added = i(1 << 16);
            rUpdate.removed = i();
            rUpdate.modified = i();
            rUpdate.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            final RowSetShiftData.Builder rShiftBuilder = new RowSetShiftData.Builder();
            rShiftBuilder.shiftRange(128, 128, 1);
            rUpdate.shifted = rShiftBuilder.build();
            rTable.notifyListeners(rUpdate);
        });
        TstUtils.validate(en);
    }

    @Test
    public void testZeroKeyRightModifyReportsOnlyModifiedRows() {
        final QueryTable lTable = testRefreshingTable(i(1, 5, 9).toTracking(), intCol("LVal", 1, 5, 9));
        final QueryTable rTable = testRefreshingTable(i(0, 2, 4, 6).toTracking(), intCol("RVal", 10, 12, 14, 16));
        final QueryTable jt = (QueryTable) lTable.join(rTable, emptyList(), emptyList(), numRightBitsToReserve);
        final SimpleListener listener = new SimpleListener(jt);
        jt.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(rTable, i(4), intCol("RVal", -14));
            rTable.notifyListeners(new TableUpdateImpl(i(), i(), i(4), RowSetShiftData.EMPTY,
                    rTable.newModifiedColumnSet("RVal")));
        });

        assertEquals(1, listener.getCount());
        assertEquals(i(), listener.update.added());
        assertEquals(i(), listener.update.removed());
        assertTrue(listener.update.shifted().empty());
        assertEquals(jt.newModifiedColumnSet("RVal"), listener.update.modifiedColumnSet());
        assertEquals(lTable.size(), listener.update.modified().size());
        final ColumnSource<Integer> rVal = jt.getColumnSource("RVal", int.class);
        listener.update.modified().forAllRowKeys(rowKey -> assertEquals(-14, rVal.getInt(rowKey)));
        assertTableEquals(lTable.join(rTable, emptyList(), emptyList(), numRightBitsToReserve), jt);
        listener.reset();

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(rTable, i(3), intCol("RVal", 13));
            removeRows(rTable, i(0));
            rTable.notifyListeners(i(3), i(0), i());
        });
        assertEquals(1, listener.getCount());
        assertEquals(2 * lTable.size(), listener.update.added().size() + listener.update.removed().size());
        assertEquals(i(), listener.update.modified());
        assertTrue(listener.update.shifted().empty());
        assertTableEquals(lTable.join(rTable, emptyList(), emptyList(), numRightBitsToReserve), jt);
    }

    @Test
    public void testZeroKeyRightModifyOfColumnNotAdded() {
        final QueryTable lTable = testRefreshingTable(i(1, 5).toTracking(), intCol("LVal", 1, 5));
        final QueryTable rTable = testRefreshingTable(i(0, 2).toTracking(), intCol("RVal", 10, 12),
                intCol("RUnused", 20, 22));
        final QueryTable jt = (QueryTable) lTable.join(rTable, emptyList(), List.of(JoinAddition.parse("RVal")),
                numRightBitsToReserve);
        final SimpleListener listener = new SimpleListener(jt);
        jt.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(rTable, i(2), intCol("RVal", 12), intCol("RUnused", -22));
            rTable.notifyListeners(new TableUpdateImpl(i(), i(), i(2), RowSetShiftData.EMPTY,
                    rTable.newModifiedColumnSet("RUnused")));
        });
        assertEquals(0, listener.getCount());

        // the left modification paints only its own left row's block
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(lTable, i(1), intCol("LVal", -1));
            lTable.notifyListeners(new TableUpdateImpl(i(), i(), i(1), RowSetShiftData.EMPTY,
                    lTable.newModifiedColumnSet("LVal")));
            addToTable(rTable, i(2), intCol("RVal", 12), intCol("RUnused", 22));
            rTable.notifyListeners(new TableUpdateImpl(i(), i(), i(2), RowSetShiftData.EMPTY,
                    rTable.newModifiedColumnSet("RUnused")));
        });
        assertEquals(1, listener.getCount());
        assertEquals(rTable.size(), listener.update.modified().size());
        assertEquals(jt.newModifiedColumnSet("LVal"), listener.update.modifiedColumnSet());
        listener.reset();

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(rTable, i(2), intCol("RVal", -12), intCol("RUnused", 22));
            rTable.notifyListeners(new TableUpdateImpl(i(), i(), i(2), RowSetShiftData.EMPTY,
                    rTable.newModifiedColumnSet("RVal", "RUnused")));
        });
        assertEquals(1, listener.getCount());
        assertEquals(lTable.size(), listener.update.modified().size());
        assertEquals(jt.newModifiedColumnSet("RVal"), listener.update.modifiedColumnSet());
        assertTableEquals(lTable.join(rTable, emptyList(), List.of(JoinAddition.parse("RVal")),
                numRightBitsToReserve), jt);
    }

    @Test
    public void testIncrementalZeroKeyJoin() {
        final int[] sizes = {10, 100, 1000};
        for (int size : sizes) {
            testIncrementalZeroKeyJoin("size == " + size, size, 0, new MutableInt(50));
        }
    }

    @Test
    public void testCrossJoinShift() {
        final QueryTable left = (QueryTable) TableTools.newTable(intCol("LK", 1, 2, 3), intCol("LS", 1, 2, 3));
        final QueryTable right = TstUtils.testRefreshingTable(intCol("RK", 1, 2, 3), intCol("RS", 10, 20, 30));

        final QueryTable joined = (QueryTable) CrossJoinHelper.join(left, right,
                MatchPairFactory.getExpressions("LK=RK"), MatchPairFactory.getExpressions("RS"), 1);
        assertTableEquals(TableTools.newTable(intCol("LK", 1, 2, 3), intCol("LS", 1, 2, 3), intCol("RS", 10, 20, 30)),
                joined);

        final ErrorListener errorListener = makeAndListenToValidator(joined);
        final PrintListener printListener = new PrintListener("joined", joined);
        final SimpleListener listener = new SimpleListener(joined);
        joined.addUpdateListener(listener);

        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(() -> {
            addToTable(right, i(4, 5), intCol("RK", 2, 2), intCol("RS", 40, 50));
            right.notifyListeners(i(4, 5), i(), i());
        });

        assertTableEquals(TableTools.newTable(intCol("LK", 1, 2, 2, 2, 3), intCol("LS", 1, 2, 2, 2, 3),
                intCol("RS", 10, 20, 40, 50, 30)), joined);

        assertEquals(1, listener.count);
        assertEquals(i(), listener.update.removed());
        assertEquals(i(), listener.update.modified());
        assertEquals(i(5, 6), listener.update.added());
        listener.reset();
    }

    @Test
    public void testLeftTickingModifiedColumnsPerCycle() {
        for (final boolean leftOuterJoin : new boolean[] {false, true}) {
            final QueryTable left = testRefreshingTable(i(0, 1).toTracking(), intCol("K", 1, 2), intCol("A", 1, 2),
                    intCol("B", 1, 2));
            final QueryTable right = testTable(i(0, 1).toTracking(), intCol("K", 1, 2), intCol("Y", 3, 4));
            final MatchPair[] columnsToMatch = MatchPairFactory.getExpressions("K");
            final MatchPair[] columnsToAdd = MatchPairFactory.getExpressions("Y");
            final QueryTable joined = (QueryTable) (leftOuterJoin
                    ? CrossJoinHelper.leftOuterJoin(left, right, columnsToMatch, columnsToAdd, numRightBitsToReserve)
                    : CrossJoinHelper.join(left, right, columnsToMatch, columnsToAdd, numRightBitsToReserve));
            final SimpleListener listener = new SimpleListener(joined);
            joined.addUpdateListener(listener);

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            // the key column is written with its existing value, so the row keeps its slot
            final int[] aValues = {11, 11, 11};
            final int[] bValues = {1, 1, 12};
            final String[] columns = {"A", "K", "B"};
            for (int step = 0; step < columns.length; ++step) {
                final String column = columns[step];
                final int aValue = aValues[step];
                final int bValue = bValues[step];
                updateGraph.runWithinUnitTestCycle(() -> {
                    addToTable(left, i(0), intCol("K", 1), intCol("A", aValue), intCol("B", bValue));
                    left.notifyListeners(new TableUpdateImpl(i(), i(), i(0), RowSetShiftData.EMPTY,
                            left.newModifiedColumnSet(column)));
                });
                assertEquals("leftOuterJoin=" + leftOuterJoin + ", column=" + column,
                        joined.newModifiedColumnSet(column), listener.update.modifiedColumnSet());
                listener.reset();
            }
        }
    }

    @Test
    public void testStaticEmptyInputResultIsStatic() {
        for (final boolean keyed : new boolean[] {false, true}) {
            final MatchPair[] columnsToMatch =
                    keyed ? MatchPairFactory.getExpressions("K") : MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY;
            final MatchPair[] columnsToAdd = MatchPairFactory.getExpressions("Y");
            for (final boolean leftOuterJoin : new boolean[] {false, true}) {
                final String description = "keyed=" + keyed + ", leftOuterJoin=" + leftOuterJoin;

                // a static empty left table has no rows to join
                final QueryTable emptyLeft = testTable(intCol("K"), intCol("A"));
                final QueryTable tickingRight =
                        testRefreshingTable(i(0).toTracking(), intCol("K", 1), intCol("Y", 10));
                final Table emptyLeftJoined = leftOuterJoin
                        ? CrossJoinHelper.leftOuterJoin(emptyLeft, tickingRight, columnsToMatch, columnsToAdd,
                                numRightBitsToReserve)
                        : CrossJoinHelper.join(emptyLeft, tickingRight, columnsToMatch, columnsToAdd,
                                numRightBitsToReserve);
                assertFalse(description, emptyLeftJoined.isRefreshing());
                assertTrue(description, emptyLeftJoined.isEmpty());

                // an inner join against a static empty right table has no rows to match; an outer join follows the
                // left table
                final QueryTable tickingLeft = testRefreshingTable(i(0).toTracking(), intCol("K", 1), intCol("A", 2));
                final QueryTable emptyRight = testTable(intCol("K"), intCol("Y"));
                final EvalNugget[] en = new EvalNugget[] {
                        EvalNugget.from(() -> leftOuterJoin
                                ? CrossJoinHelper.leftOuterJoin(tickingLeft, emptyRight, columnsToMatch,
                                        columnsToAdd, numRightBitsToReserve)
                                : CrossJoinHelper.join(tickingLeft, emptyRight, columnsToMatch, columnsToAdd,
                                        numRightBitsToReserve)),
                };
                assertEquals(description, leftOuterJoin, en[0].originalValue.isRefreshing());

                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(() -> {
                    addToTable(tickingRight, i(1), intCol("K", 1), intCol("Y", 11));
                    tickingRight.notifyListeners(i(1), i(), i());
                    addToTable(tickingLeft, i(1), intCol("K", 1), intCol("A", 3));
                    tickingLeft.notifyListeners(i(1), i(), i());
                });
                assertTrue(description, emptyLeftJoined.isEmpty());
                TstUtils.validate(description, en);
            }
        }
    }

    private ErrorListener makeAndListenToValidator(QueryTable joined) {
        final TableUpdateValidator validator = TableUpdateValidator.make("joined", joined);
        final QueryTable validatorResult = validator.getResultTable();
        final ErrorListener errorListener = new ErrorListener(validatorResult);
        validatorResult.addUpdateListener(errorListener);
        return errorListener;
    }

    private void testIncrementalZeroKeyJoin(final String ctxt, final int size, final int seed,
            final MutableInt numSteps) {
        final int leftSize = (int) Math.ceil(Math.sqrt(size));

        final int maxSteps = numSteps.get();
        final Random random = new Random(seed);

        final int numGroups = (int) Math.max(4, Math.ceil(Math.sqrt(leftSize)));
        final ColumnInfo<?, ?>[] leftColumns = getIncrementalColumnInfo("lt", numGroups);
        final QueryTable leftTicking = getTable(leftSize, random, leftColumns);

        final ColumnInfo<?, ?>[] rightColumns = getIncrementalColumnInfo("rt", numGroups);
        final QueryTable rightTicking = getTable(size, random, rightColumns);

        final QueryTable leftStatic = getTable(false, leftSize, random, getIncrementalColumnInfo("ls", numGroups));
        final QueryTable rightStatic = getTable(false, size, random, getIncrementalColumnInfo("rs", numGroups));

        final EvalNugget[] en = new EvalNugget[] {
                // Zero-Key Joins
                EvalNugget.from(() -> leftTicking.join(rightTicking, emptyList(), emptyList(), numRightBitsToReserve)),
                EvalNugget.from(() -> leftStatic.join(rightTicking, emptyList(), emptyList(), numRightBitsToReserve)),
                EvalNugget.from(() -> leftTicking.join(rightStatic, emptyList(), emptyList(), numRightBitsToReserve)),
        };

        for (numSteps.set(0); numSteps.get() < maxSteps; numSteps.increment()) {
            // left size is sqrt right table size; which is a good update size for the right table
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(() -> {
                final int stepInstructions = random.nextInt();
                if (stepInstructions % 4 != 1) {
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, leftSize,
                            random, leftTicking, leftColumns);
                }
                if (stepInstructions % 4 != 0) {
                    // left size is sqrt right table size; which is a good update size for the right table
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, leftSize,
                            random, rightTicking, rightColumns);
                }
            });
            TstUtils.validate(ctxt + " step == " + numSteps.get(), en);
        }
    }

    @Test
    public void testSmallStaticJoin() {
        final String[] types = new String[] {"single", "none", "multi"};
        final int[] cardinality = new int[] {1, 0, 3};
        for (int lt = 0; lt < 2; ++lt) {
            for (int rt = 0; rt < 2; ++rt) {
                boolean leftTicking = lt == 1;
                boolean rightTicking = rt == 1;
                testStaticJoin(types, cardinality, types.length, types.length, leftTicking, rightTicking,
                        TestJoinControl.DEFAULT_JOIN_CONTROL);
                // force left build
                testStaticJoin(types, cardinality, 1, types.length, leftTicking, rightTicking,
                        TestJoinControl.DEFAULT_JOIN_CONTROL);
                // force right build
                testStaticJoin(types, cardinality, types.length, 1, leftTicking, rightTicking,
                        TestJoinControl.DEFAULT_JOIN_CONTROL);
            }
        }
    }

    @Test
    public void testLargeStaticJoin() {
        final String[] types = new String[26];
        final int[] cardinality = new int[26];
        for (int i = 0; i < 26; ++i) {
            types[i] = String.valueOf('a' + i);
            cardinality[i] = i * i;
        }
        for (int lt = 0; lt < 2; ++lt) {
            for (int rt = 0; rt < 2; ++rt) {
                boolean leftTicking = lt == 1;
                boolean rightTicking = rt == 1;
                testStaticJoin(types, cardinality, types.length, types.length, leftTicking, rightTicking,
                        TestJoinControl.DEFAULT_JOIN_CONTROL);
            }
        }
    }

    @Test
    public void testLargeStaticOverflow() {
        final String[] types = new String[26];
        final int[] cardinality = new int[26];
        for (int i = 0; i < 26; ++i) {
            types[i] = String.valueOf('a' + i);
            cardinality[i] = i * i;
        }
        testStaticJoin(types, cardinality, types.length, types.length, false, false,
                TestJoinControl.SMALL_TABLE_JOIN_CONTROL);
    }

    // generate a table such that all pairs of types exist and are part of the cross-join
    private void testStaticJoin(final String[] types, final int[] cardinality, int maxLeftType, int maxRightType,
            boolean leftTicking, boolean rightTicking, JoinControl joinControl) {
        Assert.eq(types.length, "types.length", cardinality.length, "cardinality.length");

        long nextLeftRow = 0;
        final ArrayList<String> leftKeys = new ArrayList<>();
        final LongArrayList leftData = new LongArrayList();

        long nextRightRow = 0;
        final ArrayList<String> rightKeys = new ArrayList<>();
        final LongArrayList rightData = new LongArrayList();

        int expectedSize = 0;
        final Map<String, MutableLong> expectedByKey = Maps.newHashMap();

        for (int i = 0; i < maxLeftType; ++i) {
            final String keyPrefix = types[i];
            final int leftSize = cardinality[i];
            for (int j = 0; j < maxRightType; ++j) {
                final String keySuffix = types[j];
                final int rightSize = cardinality[j];
                final String sharedKey = keyPrefix + "-" + keySuffix;
                for (long ll = 0; ll < leftSize; ++ll) {
                    final long id = nextLeftRow++;
                    leftKeys.add(sharedKey);
                    leftData.add(id);
                }

                for (long rr = 0; rr < rightSize; ++rr) {
                    final long id = nextRightRow++;
                    rightKeys.add(sharedKey);
                    rightData.add(id);
                }

                expectedSize += leftSize * rightSize;
                Assert.eqFalse(expectedByKey.containsKey(sharedKey), "expectedByKey.containsKey(sharedKey)");
                expectedByKey.put(sharedKey, new MutableLong((long) leftSize * rightSize));
            }
        }

        final QueryTable left = (QueryTable) TableTools.newTable(
                stringCol("sharedKey", leftKeys.toArray(String[]::new)),
                longCol("leftData", leftData.toLongArray()));
        if (leftTicking) {
            left.setRefreshing(true);
        }

        final QueryTable right = (QueryTable) TableTools.newTable(stringCol("sharedKey",
                rightKeys.toArray(String[]::new)),
                longCol("rightData", rightData.toLongArray()));
        if (rightTicking) {
            right.setRefreshing(true);
        }

        final Table chunkedCrossJoin =
                left.join(right, List.of(JoinMatch.parse("sharedKey")), emptyList(), numRightBitsToReserve);
        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Left Table (" + left.size() + " rows): ");
            TableTools.showWithRowSet(left, 100);
            System.out.println("\nRight Table (" + right.size() + " rows): ");
            TableTools.showWithRowSet(right, 100);
            System.out.println("\nCross Join Table (" + chunkedCrossJoin.size() + " rows): ");
            TableTools.showWithRowSet(chunkedCrossJoin, 100);
        }

        final Table nonChunkedCrossJoin = simulatedCrossJoin(
                left, right, List.of(JoinMatch.parse("sharedKey")), emptyList(), numRightBitsToReserve);
        TstUtils.assertTableEquals(nonChunkedCrossJoin, chunkedCrossJoin);

        Assert.eq(expectedSize, "expectedSize", chunkedCrossJoin.size(), "chunkedCrossJoin.size()");
        final ColumnSource<?> keyColumn = chunkedCrossJoin.getColumnSource("sharedKey");
        final ColumnSource<?> leftColumn = chunkedCrossJoin.getColumnSource("leftData");
        final ColumnSource<?> rightColumn = chunkedCrossJoin.getColumnSource("rightData");

        final MutableLong lastLeftId = new MutableLong();
        final MutableLong lastRightId = new MutableLong();
        final MutableObject<String> lastSharedKey = new MutableObject<>();

        chunkedCrossJoin.getRowSet().forAllRowKeys(ii -> {
            final String sharedKey = (String) keyColumn.get(ii);

            final long leftId = leftColumn.getLong(ii);
            final long rightId = rightColumn.getLong(ii);
            if (lastSharedKey.getValue() != null && lastSharedKey.getValue().equals(sharedKey)) {
                Assert.leq(lastLeftId.get(), "lastLeftId.longValue()", leftId, "leftId");
                if (lastLeftId.get() == leftId) {
                    Assert.lt(lastRightId.get(), "lastRightId.longValue()", rightId, "rightId");
                }
            } else {
                lastSharedKey.setValue(sharedKey);
                lastLeftId.set(leftId);
            }
            lastRightId.set(rightId);

            final MutableLong remainingCount = expectedByKey.get(sharedKey);
            Assert.neqNull(remainingCount, "remainingCount");
            Assert.gtZero(remainingCount.get(), "remainingCount.longValue()");
            remainingCount.decrement();
        });

        for (final Map.Entry<String, MutableLong> entry : expectedByKey.entrySet()) {
            Assert.eqZero(entry.getValue().get(), "entry.getValue().longValue");
        }
    }

    private static Table simulatedCrossJoin(
            @NotNull final Table leftTable,
            @NotNull Table rightTableCandidate,
            @NotNull final Collection<? extends JoinMatch> columnsToMatchIn,
            @NotNull final Collection<? extends JoinAddition> columnsToAdd,
            int numRightBitsToReserve) {
        final JoinMatch[] columnsToMatch = columnsToMatchIn.toArray(JoinMatch[]::new);
        final Set<String> columnsToMatchSet =
                columnsToMatchIn.stream().map(m -> m.right().name())
                        .collect(Collectors.toCollection(HashSet::new));

        final Map<String, Selectable> columnsToAddSelectColumns = new LinkedHashMap<>();
        final List<String> columnsToUngroupBy = new ArrayList<>();
        final String[] rightColumnsToMatch = new String[columnsToMatch.length];
        for (int i = 0; i < rightColumnsToMatch.length; i++) {
            rightColumnsToMatch[i] = columnsToMatch[i].right().name();
            columnsToAddSelectColumns.put(columnsToMatch[i].right().name(), columnsToMatch[i].right());
        }

        final MatchPair[] columnMatchPairs = MatchPair.fromMatches(columnsToMatchIn);
        final MatchPair[] realColumnsToAdd =
                QueryTable.createColumnsToAddIfMissing(rightTableCandidate,
                        columnMatchPairs,
                        MatchPair.fromAddition(columnsToAdd));

        final ArrayList<MatchPair> columnsToAddAfterRename = new ArrayList<>(realColumnsToAdd.length);
        for (MatchPair matchPair : realColumnsToAdd) {
            columnsToAddAfterRename.add(new MatchPair(matchPair.leftColumn, matchPair.leftColumn));
            if (!columnsToMatchSet.contains(matchPair.leftColumn)) {
                columnsToUngroupBy.add(matchPair.leftColumn);
            }
            columnsToAddSelectColumns.put(matchPair.leftColumn,
                    Selectable.of(ColumnName.of(matchPair.leftColumn), ColumnName.of(matchPair.rightColumn)));
        }

        boolean sentinelAdded = false;
        final Table rightTable;
        if (columnsToUngroupBy.isEmpty()) {
            rightTable = rightTableCandidate.updateView("__sentinel__=null");
            columnsToUngroupBy.add("__sentinel__");
            columnsToAddSelectColumns.put("__sentinel__", ColumnName.of("__sentinel__"));
            columnsToAddAfterRename.add(new MatchPair("__sentinel__", "__sentinel__"));
            sentinelAdded = true;
        } else {
            rightTable = rightTableCandidate;
        }

        final Table rightGrouped = rightTable.groupBy(rightColumnsToMatch)
                .view(columnsToAddSelectColumns.values());
        final Table naturalJoinResult = ((QueryTable) leftTable).naturalJoinImpl(rightGrouped,
                columnMatchPairs,
                columnsToAddAfterRename.toArray(MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY),
                NaturalJoinType.ERROR_ON_DUPLICATE);
        final QueryTable ungroupedResult = (QueryTable) naturalJoinResult
                .ungroup(columnsToUngroupBy.toArray(String[]::new));

        ((QueryTable) leftTable).maybeCopyColumnDescriptions(
                ungroupedResult, rightTable, columnMatchPairs, realColumnsToAdd);

        return sentinelAdded ? ungroupedResult.dropColumns("__sentinel__") : ungroupedResult;
    }

    @Test
    public void testStaticVsNaturalJoin() {
        final int size = 10000;
        final Table x = TableTools.emptyTable(size).update("Col1=i");
        final Table y = TableTools.emptyTable(size).update("Col2=i*2");
        final Table z = x.join(y, "Col1=Col2");
        final Table z2 = x.naturalJoin(y, "Col1=Col2");
        final Table z3 = z2.where("!isNull(Col2)");

        assertTableEquals(z3, z);
    }

    @Test
    public void testStaticVsNaturalJoin2() {
        final int size = 10000;

        final QueryTable xqt = TstUtils.testRefreshingTable(RowSetFactory.flat(size).toTracking());
        final QueryTable yqt = TstUtils.testRefreshingTable(RowSetFactory.flat(size).toTracking());

        final Table x = xqt.update("Col1=i");
        final Table y = yqt.update("Col2=i*2");
        final Table z = x.join(y, "Col1=Col2");
        final Table z2 = x.naturalJoin(y, "Col1=Col2");
        final Table z3 = z2.where("!isNull(Col2)");

        assertTableEquals(z3, z);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            xqt.getRowSet().writableCast().insertRange(size, size * 2);
            xqt.notifyListeners(RowSetFactory.fromRange(size, size * 2), i(), i());
        });

        assertTableEquals(z3, z);

        updateGraph.runWithinUnitTestCycle(() -> {
            yqt.getRowSet().writableCast().insertRange(size, size * 2);
            yqt.notifyListeners(RowSetFactory.fromRange(size, size * 2), i(), i());
        });

        assertTableEquals(z3, z);
    }

    @Test
    public void testIncrementalOverflow() {
        final int[] sizes = {10, 100, 10000};

        for (int size : sizes) {
            testIncrementalOverflow("size == " + size, size, 0, new MutableInt(100));
        }
    }

    private void testIncrementalOverflow(final String ctxt, final int numGroups, final int seed,
            final MutableInt numSteps) {
        final int maxSteps = numSteps.get();
        final Random random = new Random(seed);

        // Note: make our join helper think this left table might tick
        final QueryTable leftNotTicking = getTable(1000, random, getIncrementalColumnInfo("lt", numGroups));

        final ColumnInfo<?, ?>[] leftColumns = getIncrementalColumnInfo("lt", numGroups);
        final QueryTable leftTicking = getTable(0, random, leftColumns);

        final ColumnInfo<?, ?>[] leftShiftingColumns = getIncrementalColumnInfo("lt", numGroups);
        final QueryTable leftShifting = getTable(1000, random, leftShiftingColumns);

        final ColumnInfo<?, ?>[] rightColumns = getIncrementalColumnInfo("rt", numGroups);
        final QueryTable rightTicking = getTable(0, random, rightColumns);

        final JoinControl control = new JoinControl() {
            @Override
            int initialBuildSize() {
                return 256;
            }

            @Override
            double getMaximumLoadFactor() {
                return 0.95;
            }

            @Override
            double getTargetLoadFactor() {
                return 0.9;
            }
        };

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> CrossJoinHelper.join(leftNotTicking, rightTicking,
                        MatchPairFactory.getExpressions("ltSym=rtSym"), MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                        numRightBitsToReserve, control)),
                EvalNugget.from(() -> CrossJoinHelper.join(leftTicking, rightTicking,
                        MatchPairFactory.getExpressions("ltSym=rtSym"), MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                        numRightBitsToReserve, control)),
                EvalNugget.from(() -> CrossJoinHelper.join(leftShifting, rightTicking,
                        MatchPairFactory.getExpressions("ltSym=rtSym"), MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                        numRightBitsToReserve, control)),
        };

        final int updateSize = (int) Math.ceil(Math.sqrt(numGroups));

        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Left Ticking:");
            TableTools.showWithRowSet(leftTicking);
            System.out.println("Right Ticking:");
            TableTools.showWithRowSet(rightTicking);
        }

        final GenerateTableUpdates.SimulationProfile shiftingProfile = new GenerateTableUpdates.SimulationProfile() {
            {
                SHIFT_10_PERCENT_POS_SPACE = 5;
                SHIFT_10_PERCENT_KEY_SPACE = 5;
                SHIFT_AGGRESSIVELY = 85;
            }
        };

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (numSteps.set(0); numSteps.get() < maxSteps; numSteps.increment()) {
            updateGraph.runWithinUnitTestCycle(() -> {
                final int stepInstructions = random.nextInt();
                if (stepInstructions % 4 != 1) {
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                            updateSize, random, leftTicking, leftColumns);
                    GenerateTableUpdates.generateShiftAwareTableUpdates(shiftingProfile, updateSize, random,
                            leftShifting, leftShiftingColumns);
                }
                if (stepInstructions % 4 != 0) {
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                            updateSize, random, rightTicking, rightColumns);
                }
            });

            TstUtils.validate(ctxt + " step == " + numSteps.get(), en);
        }
    }

    @Test
    public void testIncrementalWithKeyColumns() {
        final int[] sizes = {10, 100, 1000};

        for (int size : sizes) {
            testIncrementalWithKeyColumns("size == " + size, size, 0, new MutableInt(100));
        }
    }

    protected void testIncrementalWithKeyColumns(final String ctxt, final int initialSize, final int seed,
            final MutableInt numSteps) {
        final int maxSteps = numSteps.get();
        final Random random = new Random(seed);

        final int numGroups = (int) Math.max(4, Math.ceil(Math.sqrt(initialSize)));
        final ColumnInfo<?, ?>[] leftColumns = getIncrementalColumnInfo("lt", numGroups);
        final QueryTable leftTicking = getTable(initialSize, random, leftColumns);

        final ColumnInfo<?, ?>[] rightColumns = getIncrementalColumnInfo("rt", numGroups);
        final QueryTable rightTicking = getTable(initialSize, random, rightColumns);

        final QueryTable leftStatic = getTable(false, initialSize, random, getIncrementalColumnInfo("ls", numGroups));
        final QueryTable rightStatic = getTable(false, initialSize, random, getIncrementalColumnInfo("rs", numGroups));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> leftTicking.join(rightTicking, List.of(JoinMatch.parse("ltSym=rtSym")),
                        emptyList(), numRightBitsToReserve)),
                EvalNugget.from(() -> leftStatic.join(rightTicking, List.of(JoinMatch.parse("lsSym=rtSym")),
                        emptyList(), numRightBitsToReserve)),
                EvalNugget.from(() -> leftTicking.join(rightStatic, List.of(JoinMatch.parse("ltSym=rsSym")),
                        emptyList(), numRightBitsToReserve)),
        };

        final int updateSize = (int) Math.ceil(Math.sqrt(initialSize));

        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Left Ticking:");
            TableTools.showWithRowSet(leftTicking);
            System.out.println("Right Ticking:");
            TableTools.showWithRowSet(rightTicking);
            System.out.println("Left Static:");
            TableTools.showWithRowSet(leftStatic);
            System.out.println("Right Static:");
            TableTools.showWithRowSet(rightStatic);
        }

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (numSteps.set(0); numSteps.get() < maxSteps; numSteps.increment()) {
            updateGraph.runWithinUnitTestCycle(() -> {
                final int stepInstructions = random.nextInt();
                if (stepInstructions % 4 != 1) {
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                            updateSize, random, leftTicking, leftColumns);
                }
                if (stepInstructions % 4 != 0) {
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                            updateSize, random, rightTicking, rightColumns);
                }
            });

            TstUtils.validate(ctxt + " step == " + numSteps.get(), en);
        }
    }

    @Test
    public void testColumnSourceCanReuseContextWithSmallerRowSequence() {
        final QueryTable t1 = testRefreshingTable(i(0, 1).toTracking());
        final QueryTable t2 = (QueryTable) t1.update("K=k", "A=1");
        final QueryTable t3 = (QueryTable) testTable(i(2, 3).toTracking()).update("I=i", "A=1");
        final QueryTable jt =
                (QueryTable) t2.join(t3, List.of(JoinMatch.parse("A")), emptyList(), numRightBitsToReserve);

        final int CHUNK_SIZE = 4;
        final ColumnSource<Integer> column = jt.getColumnSource("I", int.class);
        try (final ColumnSource.FillContext context = column.makeFillContext(CHUNK_SIZE);
                final WritableIntChunk<Values> dest = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
                final ResettableWritableIntChunk<Values> rdest =
                        ResettableWritableIntChunk.makeResettableChunk()) {

            rdest.resetFromChunk(dest, 0, 4);
            column.fillChunk(context, rdest, jt.getRowSet().subSetByPositionRange(0, 4));
            rdest.resetFromChunk(dest, 0, 2);
            column.fillChunk(context, rdest, jt.getRowSet().subSetByPositionRange(0, 2));
        }
    }

    @Test
    public void testShiftingDuringRehash() {
        final int maxSteps = 2500;
        final MutableInt numSteps = new MutableInt();

        final QueryTable leftTicking = TstUtils.testRefreshingTable(i().toTracking(), longCol("intCol"));
        final QueryTable rightTicking = TstUtils.testRefreshingTable(i().toTracking(), longCol("intCol"));

        final JoinControl control = new JoinControl() {
            @Override
            int initialBuildSize() {
                return 256;
            }

            @Override
            double getMaximumLoadFactor() {
                return 0.95;
            }

            @Override
            double getTargetLoadFactor() {
                return 0.9;
            }
        };

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(
                        () -> CrossJoinHelper.join(leftTicking, rightTicking, MatchPairFactory.getExpressions("intCol"),
                                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY, numRightBitsToReserve, control)),
        };

        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Left Ticking:");
            TableTools.showWithRowSet(leftTicking);
            System.out.println("Right Ticking:");
            TableTools.showWithRowSet(rightTicking);
        }

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (numSteps.set(0); numSteps.get() < maxSteps; numSteps.increment()) {
            final long rightOffset = numSteps.get();

            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(leftTicking, i(numSteps.get()), longCol("intCol", numSteps.get()));
                TableUpdateImpl up = new TableUpdateImpl();
                up.shifted = RowSetShiftData.EMPTY;
                up.added = i(numSteps.get());
                up.removed = i();
                up.modified = i();
                up.modifiedColumnSet = ModifiedColumnSet.ALL;
                leftTicking.notifyListeners(up);

                final long[] data = new long[numSteps.get() + 1];
                for (int i = 0; i <= numSteps.get(); ++i) {
                    data[i] = i;
                }
                addToTable(rightTicking, RowSetFactory.fromRange(rightOffset, rightOffset + numSteps.get()),
                        longCol("intCol", data));
                TstUtils.removeRows(rightTicking, i(rightOffset - 1));

                up = new TableUpdateImpl();
                final RowSetShiftData.Builder shifted = new RowSetShiftData.Builder();
                shifted.shiftRange(0, numSteps.get() + rightOffset, 1);
                up.shifted = shifted.build();
                up.added = i(rightOffset + numSteps.get());
                up.removed = i();
                if (numSteps.get() == 0) {
                    up.modified = RowSetFactory.empty();
                } else {
                    up.modified = RowSetFactory.fromRange(rightOffset, rightOffset + numSteps.get() - 1);
                }
                up.modifiedColumnSet = ModifiedColumnSet.ALL;
                rightTicking.notifyListeners(up);
            });

            TstUtils.validate(" step == " + numSteps.get(), en);
        }
    }

    @Test
    public void testBothTickingKeyChurn() {
        // Keys come from a small pool, and whole keys frequently leave both sides at once; a key that leaves releases
        // its slot, and a key that arrives later may reuse it.
        final Random random = new Random(0);
        final QueryTable left = TstUtils.testRefreshingTable(i().toTracking(), longCol("LK"), intCol("LV"));
        final QueryTable right = TstUtils.testRefreshingTable(i().toTracking(), longCol("RK"), intCol("RV"));
        final MatchPair[] columnsToMatch = MatchPairFactory.getExpressions("LK=RK");

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> CrossJoinHelper.join(left, right, columnsToMatch,
                        MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY, numRightBitsToReserve,
                        TestJoinControl.SMALL_TABLE_JOIN_CONTROL)),
                EvalNugget.from(() -> CrossJoinHelper.leftOuterJoin(left, right, columnsToMatch,
                        MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY, numRightBitsToReserve,
                        TestJoinControl.SMALL_TABLE_JOIN_CONTROL)),
        };

        final Table joined = CrossJoinHelper.join(left, right, columnsToMatch,
                MatchPairFactory.getExpressions("RV"), numRightBitsToReserve,
                TestJoinControl.SMALL_TABLE_JOIN_CONTROL);
        final RightIncrementalChunkedCrossJoinStateManager stateManager =
                (RightIncrementalChunkedCrossJoinStateManager) ((CrossJoinRightColumnSource<?>) joined
                        .getColumnSource("RV")).getCrossJoinManager();

        final MutableLong nextRowKey = new MutableLong();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < 300; ++step) {
            updateGraph.runWithinUnitTestCycle(() -> {
                churnKeys(random, left, "LK", "LV", nextRowKey);
                churnKeys(random, right, "RK", "RV", nextRowKey);
            });
            TstUtils.validate("step == " + step, en);

            // only the keys still on either side have a slot, and released slots are reused rather than new ones
            // handed out
            final Set<Long> liveKeys = new HashSet<>();
            left.getRowSet().forAllRowKeys(rowKey -> liveKeys.add(left.getColumnSource("LK").getLong(rowKey)));
            right.getRowSet().forAllRowKeys(rowKey -> liveKeys.add(right.getColumnSource("RK").getLong(rowKey)));
            assertEquals("step == " + step, liveKeys.size(), stateManager.liveSlotCount());
            assertTrue("step == " + step, stateManager.slotCapacity() <= 8);
        }
    }

    /**
     * Remove some (or all) of the rows of {@code table}, change the key of some of the rest, and add a few rows.
     */
    private static void churnKeys(final Random random, final QueryTable table, final String keyName,
            final String valueName, final MutableLong nextRowKey) {
        if (random.nextInt(4) == 0) {
            return;
        }
        final boolean removeAll = random.nextInt(3) == 0;
        final RowSetBuilderSequential removedBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential modifiedBuilder = RowSetFactory.builderSequential();
        table.getRowSet().forAllRowKeys(rowKey -> {
            final int choice = random.nextInt(10);
            if (removeAll || choice < 4) {
                removedBuilder.appendKey(rowKey);
            } else if (choice < 6) {
                modifiedBuilder.appendKey(rowKey);
            }
        });
        final WritableRowSet removed = removedBuilder.build();
        final WritableRowSet modified = modifiedBuilder.build();
        final int numAdded = random.nextInt(6);
        final WritableRowSet added = numAdded == 0 ? RowSetFactory.empty()
                : RowSetFactory.fromRange(nextRowKey.get(), nextRowKey.get() + numAdded - 1);
        nextRowKey.add(numAdded);

        TstUtils.removeRows(table, removed);
        final int numChanged = modified.intSize() + numAdded;
        final long[] keys = new long[numChanged];
        final int[] values = new int[numChanged];
        for (int ii = 0; ii < numChanged; ++ii) {
            keys[ii] = random.nextInt(8);
            values[ii] = random.nextInt(1000);
        }
        // the modified rows precede the added rows, which have new row keys
        try (final WritableRowSet changed = modified.union(added)) {
            TstUtils.addToTable(table, changed, longCol(keyName, keys), intCol(valueName, values));
        }
        table.notifyListeners(added, removed, modified);
    }

    private static final class CountingIntTestSource extends IntTestSource {
        long reads;

        CountingIntTestSource(final RowSet rowSet, final int[] values) {
            super(rowSet, IntChunk.chunkWrap(values));
        }

        @Override
        public int getInt(final long index) {
            ++reads;
            return super.getInt(index);
        }

        @Override
        public int getPrevInt(final long index) {
            ++reads;
            return super.getPrevInt(index);
        }
    }

    @Test
    public void testBothTickingLeftNonKeyModifyDoesNotReadKeys() {
        final int size = 1_000;
        final int[] keys = IntStream.range(0, size).toArray();
        final CountingIntTestSource keySource = new CountingIntTestSource(RowSetFactory.flat(size), keys);
        final IntTestSource valueSource = new IntTestSource(RowSetFactory.flat(size), IntChunk.chunkWrap(keys));
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        columns.put("Key", keySource);
        columns.put("LVal", valueSource);
        final QueryTable lTable = new QueryTable(RowSetFactory.flat(size).toTracking(), columns);
        lTable.setRefreshing(true);
        final QueryTable rTable = testRefreshingTable(RowSetFactory.flat(size).toTracking(), intCol("Key", keys),
                intCol("RVal", keys));

        // no validator listens to the result, so only the join reads the key column during the update
        final Table joined = lTable.join(rTable, List.of(JoinMatch.parse("Key")),
                List.of(JoinAddition.parse("RVal")), numRightBitsToReserve);

        final ModifiedColumnSet valueColumnSet = lTable.newModifiedColumnSet("LVal");
        keySource.reads = 0;
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(() -> {
            valueSource.add(RowSetFactory.flat(size), IntChunk.chunkWrap(IntStream.range(0, size).map(ii -> -ii)
                    .toArray()));
            lTable.notifyListeners(new TableUpdateImpl(i(), i(), RowSetFactory.flat(size), RowSetShiftData.EMPTY,
                    valueColumnSet));
        });
        // the modified rows keep their slots, which the left row redirection already holds
        assertEquals(0, keySource.reads);
        assertTableEquals(lTable.snapshot().join(rTable.snapshot(), "Key", "RVal"), joined);
    }

    @Test
    public void testBothTickingLeftKeyModifySomeKeysUnchanged() {
        final QueryTable lTable = testRefreshingTable(i(10, 20, 30, 40, 50, 60).toTracking(),
                intCol("Key", 1, 1, 2, 2, 3, 3), intCol("LVal", 10, 20, 30, 40, 50, 60));
        final QueryTable rTable = testRefreshingTable(i(0, 1, 2, 3).toTracking(),
                intCol("Key", 1, 2, 2, 3), intCol("RVal", 100, 200, 201, 300));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> lTable.join(rTable, List.of(JoinMatch.parse("Key")),
                        List.of(JoinAddition.parse("RVal")), numRightBitsToReserve)),
                EvalNugget.from(() -> CrossJoinHelper.leftOuterJoin(lTable, rTable,
                        MatchPairFactory.getExpressions("Key"), MatchPairFactory.getExpressions("RVal"),
                        numRightBitsToReserve)),
        };

        final ModifiedColumnSet keyAndValueColumnSet = lTable.newModifiedColumnSet("Key", "LVal");
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(() -> {
            // rows 40, 50 and 60 shift to 45, 55 and 65; of the modified rows, 20 and 45 keep their keys, 30 moves to
            // a matched key, 55 to an unmatched key and 65 to a matched key
            removeRows(lTable, i(40, 50, 60));
            addToTable(lTable, i(20, 30, 45, 55, 65), intCol("Key", 1, 3, 2, 4, 1),
                    intCol("LVal", 21, 31, 41, 51, 61));
            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
            shiftBuilder.shiftRange(40, 60, 5);
            lTable.notifyListeners(new TableUpdateImpl(i(), i(), i(20, 30, 45, 55, 65), shiftBuilder.build(),
                    keyAndValueColumnSet));
        });
        TstUtils.validate(en);

        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(() -> {
            // the key column is modified but no row's key value changes
            addToTable(lTable, i(10, 45), intCol("Key", 1, 2), intCol("LVal", 12, 42));
            lTable.notifyListeners(new TableUpdateImpl(i(), i(), i(10, 45), RowSetShiftData.EMPTY,
                    keyAndValueColumnSet));
        });
        TstUtils.validate(en);
    }

    @Test
    public void testConfiguredReserveBitsValidated() {
        for (final int invalid : new int[] {Integer.MIN_VALUE, -1, 0, 63, Integer.MAX_VALUE}) {
            final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> CrossJoinHelper.validateConfiguredNumRightBitsToReserve(invalid));
            assertTrue(thrown.getMessage(), thrown.getMessage().contains("CrossJoinHelper.numRightBitsToReserve"));
            assertTrue(thrown.getMessage(), thrown.getMessage().endsWith("but was " + invalid));
        }
        for (final int valid : new int[] {1, 10, 62}) {
            assertEquals(valid, CrossJoinHelper.validateConfiguredNumRightBitsToReserve(valid));
        }
    }

    @Test
    public void testReserveBitsOutOfRange() {
        for (final boolean leftRefreshing : new boolean[] {false, true}) {
            for (final boolean rightRefreshing : new boolean[] {false, true}) {
                final QueryTable lTable = leftRefreshing
                        ? testRefreshingTable(i(0, 1).toTracking(), intCol("Key", 1, 2))
                        : testTable(i(0, 1).toTracking(), intCol("Key", 1, 2));
                final QueryTable rTable = rightRefreshing
                        ? testRefreshingTable(i(0, 1).toTracking(), intCol("Key", 1, 2), intCol("RVal", 3, 4))
                        : testTable(i(0, 1).toTracking(), intCol("Key", 1, 2), intCol("RVal", 3, 4));
                for (final int reserveBits : new int[] {Integer.MIN_VALUE, -1, 0, 63, 64, Integer.MAX_VALUE}) {
                    final String description = "leftRefreshing=" + leftRefreshing + ", rightRefreshing="
                            + rightRefreshing + ", reserveBits=" + reserveBits;
                    final String expectedMessage =
                            "reserveBits must be between 1 and 62 (inclusive), but was " + reserveBits;
                    for (final String columnsToMatch : new String[] {"Key", ""}) {
                        final IllegalArgumentException joinException = assertThrows(description,
                                IllegalArgumentException.class,
                                () -> lTable.join(rTable, columnsToMatch, "RVal", reserveBits));
                        assertEquals(description, expectedMessage, joinException.getMessage());

                        final IllegalArgumentException leftOuterException = assertThrows(description,
                                IllegalArgumentException.class,
                                () -> OuterJoinTools.leftOuterJoin(lTable, rTable, columnsToMatch, "RVal",
                                        reserveBits));
                        assertEquals(description, expectedMessage, leftOuterException.getMessage());
                    }

                    final IllegalArgumentException fullOuterException = assertThrows(description,
                            IllegalArgumentException.class,
                            () -> OuterJoinTools.fullOuterJoin(lTable, rTable,
                                    MatchPairFactory.getExpressions("Key"), MatchPairFactory.getExpressions("RVal"),
                                    reserveBits));
                    assertEquals(description, expectedMessage, fullOuterException.getMessage());
                }

                for (final int reserveBits : new int[] {1, 62}) {
                    final String description = "leftRefreshing=" + leftRefreshing + ", rightRefreshing="
                            + rightRefreshing + ", reserveBits=" + reserveBits;
                    assertTableEquals(description, testTable(intCol("Key", 1, 2), intCol("RVal", 3, 4)),
                            lTable.join(rTable, "Key", "RVal", reserveBits));
                    assertTableEquals(description, testTable(intCol("Key", 1, 1, 2, 2), intCol("RVal", 3, 4, 3, 4)),
                            lTable.join(rTable, "", "RVal", reserveBits));
                    assertTableEquals(description, testTable(intCol("Key", 1, 2), intCol("RVal", 3, 4)),
                            OuterJoinTools.leftOuterJoin(lTable, rTable, "Key", "RVal", reserveBits));
                }
            }
        }
    }

    @Test
    public void testAddOnlyAndAppendOnlyLeftWithStaticRight() {
        for (final String leftAttribute : new String[] {Table.ADD_ONLY_TABLE_ATTRIBUTE,
                Table.APPEND_ONLY_TABLE_ATTRIBUTE}) {
            for (final boolean rightRefreshing : new boolean[] {false, true}) {
                for (final String columnsToMatch : new String[] {"Key", ""}) {
                    for (final boolean leftOuter : new boolean[] {false, true}) {
                        final String description = "leftAttribute=" + leftAttribute + ", rightRefreshing="
                                + rightRefreshing + ", columnsToMatch=" + columnsToMatch + ", leftOuter=" + leftOuter;
                        final QueryTable lTable = testRefreshingTable(i(10, 20).toTracking(),
                                intCol("Key", 1, 2), intCol("LVal", 10, 20));
                        lTable.setAttribute(leftAttribute, true);
                        final QueryTable rTable = rightRefreshing
                                ? testRefreshingTable(i(0, 1).toTracking(), intCol("Key", 1, 1), intCol("RVal", 3, 4))
                                : testTable(i(0, 1).toTracking(), intCol("Key", 1, 1), intCol("RVal", 3, 4));

                        final Table result = leftOuter
                                ? OuterJoinTools.leftOuterJoin(lTable, rTable, columnsToMatch, "RVal",
                                        numRightBitsToReserve)
                                : lTable.join(rTable, columnsToMatch, "RVal", numRightBitsToReserve);
                        final boolean appendOnlyLeft = leftAttribute.equals(Table.APPEND_ONLY_TABLE_ATTRIBUTE);
                        assertEquals(description, !rightRefreshing,
                                Boolean.TRUE.equals(result.getAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE)));
                        assertEquals(description, !rightRefreshing && appendOnlyLeft,
                                Boolean.TRUE.equals(result.getAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE)));
                        if (rightRefreshing) {
                            continue;
                        }

                        final long lastRowKeyBefore = result.getRowSet().lastRowKey();
                        final SimpleListener listener = new SimpleListener(result);
                        result.addUpdateListener(listener);
                        // key 1 matches two right rows and key 3 matches none
                        final RowSet leftAdded = appendOnlyLeft ? i(30, 40) : i(5, 15, 30);
                        final int[] addedKeys = appendOnlyLeft ? new int[] {1, 3} : new int[] {3, 1, 3};
                        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast()
                                .runWithinUnitTestCycle(() -> {
                                    addToTable(lTable, leftAdded, intCol("Key", addedKeys),
                                            intCol("LVal", new int[addedKeys.length]));
                                    lTable.notifyListeners(leftAdded.copy(), i(), i());
                                });
                        assertEquals(description, 1, listener.getCount());
                        final TableUpdate update = listener.getUpdate();
                        assertTrue(description, update.added().isNonempty());
                        assertTrue(description, update.removed().isEmpty());
                        assertTrue(description, update.modified().isEmpty());
                        assertTrue(description, update.shifted().empty());
                        if (appendOnlyLeft) {
                            assertTrue(description, update.added().firstRowKey() > lastRowKeyBefore);
                        }
                        listener.close();
                    }
                }
            }
        }
    }
}
