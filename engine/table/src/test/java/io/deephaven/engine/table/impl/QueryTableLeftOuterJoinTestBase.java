//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import com.google.common.collect.Maps;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ResettableWritableIntChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.IntArrayGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.StringArrayGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.PrintListener;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.experimental.categories.Category;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static io.deephaven.api.TableOperationsDefaults.splitToCollection;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_INT;

@Category(OutOfBandTest.class)
public abstract class QueryTableLeftOuterJoinTestBase extends QueryTableTestBase {
    static private final int MAX_SEEDS = 1;
    private final int numRightBitsToReserve;

    public QueryTableLeftOuterJoinTestBase(int numRightBitsToReserve) {
        this.numRightBitsToReserve = numRightBitsToReserve;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ChunkPoolReleaseTracking.enableStrict();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ChunkPoolReleaseTracking.checkAndDisable();
    }

    private ColumnInfo<?, ?>[] getIncrementalColumnInfo(final String prefix, int numGroups) {
        String[] names = new String[] {"Sym", "IntCol"};

        return initColumnInfos(Arrays.stream(names).map(name -> prefix + name).toArray(String[]::new),
                new IntGenerator(0, numGroups - 1),
                new IntGenerator(10, 100000));
    }

    private ColumnInfo[] getIncrementalArrayColumnInfo(final String prefix, int numGroups) {
        String[] names = new String[] {"Sym", "IntCol"};

        return initColumnInfos(Arrays.stream(names).map(name -> prefix + name).toArray(String[]::new),
                new IntArrayGenerator(0, numGroups - 1, 1, 2),
                new StringArrayGenerator(4, 5));
    }

    private MatchPair[] createColumnsToAddIfMissing(Table rightTable, MatchPair[] columnsToMatch,
            MatchPair[] columnsToAdd) {
        if (columnsToAdd.length == 0) {
            final Set<String> matchColumns = Arrays.stream(columnsToMatch).map(matchPair -> matchPair.leftColumn)
                    .collect(Collectors.toCollection(HashSet::new));
            final List<String> columnNames = rightTable.getDefinition().getColumnNames();
            return columnNames.stream().filter((name) -> !matchColumns.contains(name))
                    .map(name -> new MatchPair(name, name)).toArray(MatchPair[]::new);
        }
        return columnsToAdd;
    }

    Table doLeftOuterJoin(final Table left, final Table right, final String columnsToMatchString,
            final String columnsToAddString) {
        MatchPair[] columnsToMatch =
                MatchPairFactory.getExpressions(splitToCollection(columnsToMatchString));
        MatchPair[] columnsToAdd = MatchPairFactory.getExpressions(splitToCollection(columnsToAddString));
        columnsToAdd = createColumnsToAddIfMissing(right, columnsToMatch, columnsToAdd);
        return CrossJoinHelper.leftOuterJoin((QueryTable) left, (QueryTable) right, columnsToMatch, columnsToAdd,
                numRightBitsToReserve);
    }

    Table doLeftOuterJoin(final Table left, final Table right, final String columnsToMatchString,
            final String columnsToAddString, final JoinControl joinControl) {
        MatchPair[] columnsToMatch =
                MatchPairFactory.getExpressions(splitToCollection(columnsToMatchString));
        MatchPair[] columnsToAdd = MatchPairFactory.getExpressions(splitToCollection(columnsToAddString));
        columnsToAdd = createColumnsToAddIfMissing(right, columnsToMatch, columnsToAdd);
        return CrossJoinHelper.leftOuterJoin((QueryTable) left, (QueryTable) right, columnsToMatch, columnsToAdd,
                numRightBitsToReserve, joinControl);
    }


    Table doLeftOuterJoin(final Table left, final Table right, final String columnsToMatch) {
        return doLeftOuterJoin(left, right, columnsToMatch, "");
    }

    Table doLeftOuterJoin(final Table left, final Table right) {
        return doLeftOuterJoin(left, right, "", "");
    }

    public void testZeroKeyJoinBitExpansionOnAdd() {
        // Looking to force our index space to need more keys.
        final QueryTable lTable = testRefreshingTable(col("X", "to-remove", "b", "c", "d"));
        removeRows(lTable, i(0)); // row @ 0 does not need outer shifting
        final QueryTable rTable = testRefreshingTable(longCol("Y"));

        addToTable(rTable, i(1, (1 << 16) - 1), longCol("Y", 1, 2));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> doLeftOuterJoin(lTable, rTable)),
        };
        TstUtils.validate(en);

        final QueryTable jt = (QueryTable) doLeftOuterJoin(lTable, rTable);
        final SimpleListener listener = new SimpleListener(jt);
        jt.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
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
        Assert.eq(listener.update.shifted().size(), "listener.update.shifted().size()", lTable.size(), "lTable.size()");
    }

    public void testZeroKeyJoinBitExpansionOnBoundaryShift() {
        // Looking to force our index space to need more keys.
        final QueryTable lTable = testRefreshingTable(col("X", "to-remove", "b", "c", "d"));
        removeRows(lTable, i(0)); // row @ 0 does not need outer shifting
        final QueryTable rTable = testRefreshingTable(longCol("Y"));

        final long origIndex = (1 << 16) - 1;
        final long newIndex = 1 << 16;
        addToTable(rTable, i(0, origIndex), longCol("Y", 1, 2));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> doLeftOuterJoin(lTable, rTable)),
        };
        TstUtils.validate(en);

        final QueryTable jt = (QueryTable) doLeftOuterJoin(lTable, rTable);
        final SimpleListener listener = new SimpleListener(jt);
        jt.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
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
        Assert.eq(listener.update.shifted().size(), "listener.update.shifted().size()", 2 * lTable.size(),
                "2 * lTable.size()");
    }

    public void testZeroKeyJoinBitExpansionWithInnerShift() {
        // Looking to force our index space to need more keys.
        final QueryTable lTable = testRefreshingTable(col("X", "to-remove", "b", "c", "d"));
        removeRows(lTable, i(0)); // row @ 0 does not need outer shifting
        final QueryTable rTable = testRefreshingTable(longCol("Y"));

        addToTable(rTable, i(1, 128, (1 << 16) - 1), longCol("Y", 1, 2, 3));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> doLeftOuterJoin(lTable, rTable)),
        };
        TstUtils.validate(en);

        final QueryTable jt = (QueryTable) doLeftOuterJoin(lTable, rTable);
        final SimpleListener listener = new SimpleListener(jt);
        jt.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
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
        Assert.eq(listener.update.shifted().size(), "listener.update.shifted().size()", 3 * lTable.size(),
                "3 * lTable.size()");
    }

    public void testZeroKeyJoinCompoundShift() {
        // rightTable shift, leftTable shift, and bit expansion
        final QueryTable lTable = testRefreshingTable(col("X", "a", "b", "c", "d"));
        final QueryTable rTable = testRefreshingTable(longCol("Y"));

        addToTable(rTable, i(1, 128, (1 << 16) - 1), longCol("Y", 1, 2, 3));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> doLeftOuterJoin(lTable, rTable)),
        };
        TstUtils.validate(en);

        // left table
        // right table
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
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

    public void testIncrementalZeroKeyJoin() {
        final int maxSteps = 50;
        final int[] sizes = {10, 100, 1000};
        for (int size : sizes) {
            for (int seed = 0; seed < MAX_SEEDS; ++seed) {
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testIncrementalZeroKeyJoin("size == " + size, size, seed, false, new MutableInt(maxSteps));
                }
            }
        }
    }

    public void testIncrementalZeroKeyJoinArrays() {
        final int maxSteps = 10;
        final int[] sizes = {10};
        for (int size : sizes) {
            for (int seed = 0; seed < MAX_SEEDS; ++seed) {
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testIncrementalZeroKeyJoin("size == " + size, size, seed, true, new MutableInt(maxSteps));
                }
            }
        }
    }

    private void testIncrementalZeroKeyJoin(final String ctxt, final int size, final int seed, boolean useArrays,
            final MutableInt numSteps) {
        final int leftSize = (int) Math.ceil(Math.sqrt(size));

        final int maxSteps = numSteps.get();
        final Random random = new Random(seed);

        final int numGroups = (int) Math.max(4, Math.ceil(Math.sqrt(leftSize)));
        final ColumnInfo<?, ?>[] leftColumns =
                useArrays ? getIncrementalArrayColumnInfo("lt", numGroups) : getIncrementalColumnInfo("lt", numGroups);
        final QueryTable leftTicking = getTable(leftSize, random, leftColumns);

        final ColumnInfo<?, ?>[] rightColumns =
                useArrays ? getIncrementalArrayColumnInfo("rt", numGroups) : getIncrementalColumnInfo("rt", numGroups);
        final QueryTable rightTicking = getTable(size, random, rightColumns);

        if (printTableUpdates) {
            System.out.println("Original leftTicking:");
            TableTools.showWithRowSet(leftTicking);
            System.out.println("Original rightTicking:");
            TableTools.showWithRowSet(rightTicking);
        }

        final QueryTable leftStatic = getTable(false, leftSize, random,
                useArrays ? getIncrementalArrayColumnInfo("ls", numGroups) : getIncrementalColumnInfo("ls", numGroups));
        final QueryTable rightStatic = getTable(false, size, random,
                useArrays ? getIncrementalArrayColumnInfo("rs", numGroups) : getIncrementalColumnInfo("rs", numGroups));

        final EvalNugget[] en = new EvalNugget[] {
                // Zero-Key Joins
                EvalNugget.from(() -> doLeftOuterJoin(leftTicking, rightTicking)),
                EvalNugget.from(() -> doLeftOuterJoin(leftStatic, rightTicking)),
                EvalNugget.from(() -> doLeftOuterJoin(leftTicking, rightStatic)),
        };

        for (numSteps.set(0); numSteps.get() < maxSteps; numSteps.increment()) {
            if (printTableUpdates) {
                System.out.println("Size = " + size + ", seed=" + seed + ", step = " + numSteps.get());
            }
            // left size is sqrt right table size; which is a good update size for the right table
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
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

    public void testZeroKeyLeftOuterJoinSimple() {
        final QueryTable left = TstUtils.testRefreshingTable(intCol("LS", 1, 2, 3, 4, 5));
        final QueryTable right = TstUtils.testRefreshingTable(intCol("RS", 10, 20));

        final Table joined = CrossJoinHelper.leftOuterJoin(left, right, MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                MatchPairFactory.getExpressions("RS"), 1);
        assertTableEquals(TableTools.newTable(intCol("LS", 1, 1, 2, 2, 3, 3, 4, 4, 5, 5),
                intCol("RS", 10, 20, 10, 20, 10, 20, 10, 20, 10, 20)), joined);

        final ErrorListener errorListener = makeAndListenToValidator((QueryTable) joined);
        final PrintListener printListener = new PrintListener("joined", joined);

        final SimpleListener listener = new SimpleListener(joined);
        joined.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(right, i(0, 1));
            right.notifyListeners(i(), i(0, 1), i());
        });

        // we should remove everything, but add back a null row
        assertEquals(1, listener.count);
        assertEquals(RowSetFactory.fromRange(0, 9), listener.update.removed());
        assertEquals(i(0, 2, 4, 6, 8), listener.update.added());
        assertEquals(i(), listener.update.modified());
        listener.reset();

        assertTableEquals(TableTools.newTable(intCol("LS", 1, 2, 3, 4, 5),
                intCol("RS", NULL_INT, NULL_INT, NULL_INT, NULL_INT, NULL_INT)), joined);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(7), intCol("RS", 30));
            right.notifyListeners(i(7), i(), i());
            addToTable(left, i(6), intCol("LS", 6));
            left.notifyListeners(i(6), i(), i());
        });

        assertTableEquals(TableTools.newTable(intCol("LS", 1, 2, 3, 4, 5, 6), intCol("RS", 30, 30, 30, 30, 30, 30)),
                joined);

        // we should remove our null rows, in favor of the real row; there will be lots of meaningless shifts as well
        // TODO: it would be nice to eliminate these extra shifts
        assertEquals(1, listener.count);
        assertEquals(i(0, 2, 4, 6, 8), listener.update.removed());
        assertEquals(i(7, 15, 23, 31, 39, 55), listener.update.added());
        assertEquals(i(), listener.update.modified());
        listener.reset();

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(7), intCol("RS", 40));
            final TableUpdateImpl update = new TableUpdateImpl(i(), i(), i(7), RowSetShiftData.EMPTY,
                    right.newModifiedColumnSet("RS"));
            right.notifyListeners(update);
        });
        assertTableEquals(TableTools.newTable(intCol("LS", 1, 2, 3, 4, 5, 6), intCol("RS", 40, 40, 40, 40, 40, 40)),
                joined);
        // the listener should simply modify rows, without any other changes
        assertEquals(1, listener.count);
        assertEquals(i(7, 15, 23, 31, 39, 55), listener.update.modified());
        assertEquals(((QueryTable) joined).newModifiedColumnSet("RS"), listener.update.modifiedColumnSet());
        assertEquals(i(), listener.update.added());
        assertEquals(i(), listener.update.removed());
        listener.reset();

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(7), intCol("RS", 50));
            addToTable(right, i(4), intCol("RS", 60));
            right.notifyListeners(new TableUpdateImpl(i(4), i(), i(7), RowSetShiftData.EMPTY,
                    right.newModifiedColumnSet("RS")));
            removeRows(left, i(2));
            left.notifyListeners(
                    new TableUpdateImpl(i(), i(2), i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        });
        assertTableEquals(TableTools.newTable(intCol("LS", 1, 1, 2, 2, 4, 4, 5, 5, 6, 6),
                intCol("RS", 60, 50, 60, 50, 60, 50, 60, 50, 60, 50)), joined);
        // we need to take out the row with "3" for the left, have modifications for the right with 40->50, and
        // additions of 60
        assertEquals(1, listener.count);
        assertEquals(i(7, 15, 31, 39, 55), listener.update.modified());
        assertEquals(((QueryTable) joined).newModifiedColumnSet("RS"), listener.update.modifiedColumnSet());
        assertEquals(i(4, 12, 28, 36, 52), listener.update.added());
        assertEquals(i(23), listener.update.removed());
        listener.reset();

        TableTools.showWithRowSet(right);
        TableTools.showWithRowSet(left);
        TableTools.showWithRowSet(joined);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(right, i(4, 7));
            addToTable(right, i(2, 3), intCol("RS", 70, 80));
            right.notifyListeners(new TableUpdateImpl(i(2, 3), i(4, 7), i(), RowSetShiftData.EMPTY,
                    ModifiedColumnSet.EMPTY));
            addToTable(left, i(6), intCol("LS", 16));
            left.notifyListeners(new TableUpdateImpl(i(), i(), i(6), RowSetShiftData.EMPTY,
                    left.newModifiedColumnSet("LS")));
        });
        assertTableEquals(TableTools.newTable(intCol("LS", 1, 1, 2, 2, 4, 4, 5, 5, 16, 16),
                intCol("RS", 70, 80, 70, 80, 70, 80, 70, 80, 70, 80)), joined);

        // everything on the right will be removed and added, there should be no modifications
        // assertEquals(1, listener.count);
        // assertEquals(i(), listener.update.modified);
        // assertEquals(((QueryTable) joined).newModifiedColumnSet("LS"), listener.update.modifiedColumnSet();
        // assertEquals(ModifiedColumnSet.EMPTY, listener.update.modifiedColumnSet();
        // assertEquals(i(4, 12, 28, 36, 52), listener.update.added());
        // assertEquals(i(23), listener.update.removed());
        // listener.reset();
    }

    public void testZeroKeyTransitions() {
        final QueryTable left = TstUtils.testRefreshingTable(i(10).toTracking(), intCol("LS", 1), intCol("LS2", 100));
        final QueryTable right = TstUtils.testRefreshingTable(intCol("RS"));

        final Table joined = CrossJoinHelper.leftOuterJoin(left, right, MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                MatchPairFactory.getExpressions("RS"), 1);
        assertTableEquals(TableTools.newTable(intCol("LS", 1), intCol("LS2", 100), intCol("RS", NULL_INT)), joined);

        final ErrorListener errorListener = makeAndListenToValidator((QueryTable) joined);
        final PrintListener printListener = new PrintListener("joined", joined);

        final SimpleListener listener = new SimpleListener(joined);
        joined.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(0, 1), intCol("RS", 1, 2));
            right.notifyListeners(i(0, 1), i(), i());
        });

        assertEquals(1, listener.count);
        assertEquals(RowSetFactory.fromKeys(20), listener.update.removed());
        assertEquals(i(20, 21), listener.update.added());
        assertEquals(i(), listener.update.modified());
        listener.reset();

        assertTableEquals(TableTools.newTable(intCol("LS", 1, 1), intCol("LS2", 100, 100), intCol("RS", 1, 2)), joined);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(right, i(0, 1));
            right.notifyListeners(i(), i(0, 1), i());
        });

        assertTableEquals(TableTools.newTable(intCol("LS", 1), intCol("LS2", 100), intCol("RS", NULL_INT)), joined);

        assertEquals(1, listener.count);
        assertEquals(i(20, 21), listener.update.removed());
        assertEquals(i(20), listener.update.added());
        assertEquals(i(), listener.update.modified());
        listener.reset();

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(left, i(10), intCol("LS", 1), intCol("LS2", 101));
            final TableUpdateImpl update3 = new TableUpdateImpl(i(), i(), i(10),
                    RowSetShiftData.EMPTY, left.newModifiedColumnSet("LS2"));
            left.notifyListeners(update3);
        });

        assertTableEquals(TableTools.newTable(intCol("LS", 1), intCol("LS2", 101), intCol("RS", NULL_INT)), joined);

        // the listener should simply modify rows, without any other changes
        assertEquals(1, listener.count);
        assertEquals(i(20), listener.update.modified());
        assertEquals(((QueryTable) joined).newModifiedColumnSet("LS2"), listener.update.modifiedColumnSet());
        assertEquals(i(), listener.update.added());
        assertEquals(i(), listener.update.removed());
        listener.reset();

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(0), intCol("RS", 50));
            right.notifyListeners(i(0), i(), i());
        });
        assertTableEquals(TableTools.newTable(intCol("LS", 1), intCol("LS2", 101), intCol("RS", 50)), joined);

        // we need to take out the row with "3" for the left, have modifications for the right with 40->50, and
        // additions of 60
        assertEquals(1, listener.count);
        assertEquals(i(), listener.update.modified());
        assertEquals(i(20), listener.update.added());
        assertEquals(i(20), listener.update.removed());
        listener.reset();

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(0), intCol("RS", 60));
            final TableUpdateImpl update2 = new TableUpdateImpl(i(), i(), i(0), RowSetShiftData.EMPTY,
                    right.newModifiedColumnSet("RS"));
            right.notifyListeners(update2);
        });
        assertTableEquals(TableTools.newTable(intCol("LS", 1), intCol("LS2", 101), intCol("RS", 60)), joined);

        // we need to take out the row with "3" for the left, have modifications for the right with 40->50, and
        // additions of 60
        assertEquals(1, listener.count);
        assertEquals(i(20), listener.update.modified());
        assertEquals(((QueryTable) joined).newModifiedColumnSet("RS"), listener.update.modifiedColumnSet());
        assertEquals(i(), listener.update.added());
        assertEquals(i(), listener.update.removed());
        listener.reset();

        // empty out right in preparation for next tests, make something from left we can remove
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(right, i(0));
            final TableUpdateImpl update1 =
                    new TableUpdateImpl(i(), i(0), i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);
            right.notifyListeners(update1);
            addToTable(left, i(11, 20), intCol("LS", 2, 4), intCol("LS2", 102, 104));
            left.notifyListeners(i(11, 20), i(), i());
        });
        assertTableEquals(TableTools.newTable(intCol("LS", 1, 2, 4), intCol("LS2", 101, 102, 104),
                intCol("RS", NULL_INT, NULL_INT, NULL_INT)), joined);
        listener.reset();

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(left, i(11), intCol("LS", 3), intCol("LS2", 102));
            final TableUpdateImpl updateLeft2 = new TableUpdateImpl(i(), i(), i(11),
                    RowSetShiftData.EMPTY, left.newModifiedColumnSet("LS"));
            left.notifyListeners(updateLeft2);
        });
        assertTableEquals(TableTools.newTable(intCol("LS", 1, 3, 4), intCol("LS2", 101, 102, 104),
                intCol("RS", NULL_INT, NULL_INT, NULL_INT)), joined);
        assertEquals(1, listener.count);
        assertEquals(i(22), listener.update.modified());
        assertEquals(((QueryTable) joined).newModifiedColumnSet("LS"), listener.update.modifiedColumnSet());
        assertEquals(i(), listener.update.added());
        assertEquals(i(), listener.update.removed());
        listener.reset();

        // right empty, remove from left
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(left, i(20));
            final TableUpdateImpl updateLeft1 =
                    new TableUpdateImpl(i(), i(20), i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);
            left.notifyListeners(updateLeft1);
        });
        assertTableEquals(
                TableTools.newTable(intCol("LS", 1, 3), intCol("LS2", 101, 102), intCol("RS", NULL_INT, NULL_INT)),
                joined);
        assertEquals(1, listener.count);
        assertEquals(i(), listener.update.modified());
        assertEquals(ModifiedColumnSet.EMPTY, listener.update.modifiedColumnSet());
        assertEquals(i(), listener.update.added());
        assertEquals(i(40), listener.update.removed());
        listener.reset();

        // right transitions to non-empty, left has a remove
        updateGraph.runWithinUnitTestCycle(() -> {
            // right transitions to non-empty, left has a remove
            addToTable(right, i(0), intCol("RS", 70));
            final TableUpdateImpl update =
                    new TableUpdateImpl(i(0), i(), i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);
            right.notifyListeners(update);
            removeRows(left, i(10));
            final TableUpdateImpl updateLeft =
                    new TableUpdateImpl(i(), i(10), i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);
            left.notifyListeners(updateLeft);
        });
        assertTableEquals(TableTools.newTable(intCol("LS", 3), intCol("LS2", 102), intCol("RS", 70)), joined);

        // we need to take out the row with "3" for the left, have modifications for the right with 40->50, and
        // additions of 60
        assertEquals(1, listener.count);
        assertEquals(i(), listener.update.modified());
        assertEquals(i(22), listener.update.added());
        assertEquals(i(20, 22), listener.update.removed());
        listener.reset();
    }

    public void testLeftOuterJoinSimpleStatic() {
        testLeftOuterJoinSimpleStatic(TestJoinControl.BUILD_LEFT_CONTROL);
        testLeftOuterJoinSimpleStatic(TestJoinControl.BUILD_RIGHT_CONTROL);
    }

    private void testLeftOuterJoinSimpleStatic(JoinControl joinControl) {
        final Table left = TableTools.newTable(intCol("LK", 1, 2, 1, 3, 2, 1), intCol("LS", 1, 2, 3, 4, 5, 6));
        final Table right = TableTools.newTable(intCol("RK", 1, 1, 2), intCol("RS", 10, 20, 30));

        final Table joined = CrossJoinHelper.leftOuterJoin((QueryTable) left, (QueryTable) right,
                MatchPairFactory.getExpressions("LK=RK"), MatchPairFactory.getExpressions("RS"), 1, joinControl);
        assertTableEquals(TableTools.newTable(intCol("LK", 1, 1, 2, 1, 1, 3, 2, 1, 1),
                intCol("LS", 1, 1, 2, 3, 3, 4, 5, 6, 6), intCol("RS", 10, 20, 30, 10, 20, NULL_INT, 30, 10, 20)),
                joined);
    }

    public void testLeftOuterJoinSimpleStaticEmpty() {
        testLeftOuterJoinSimpleStaticEmpty(TestJoinControl.BUILD_LEFT_CONTROL);
        testLeftOuterJoinSimpleStaticEmpty(TestJoinControl.BUILD_RIGHT_CONTROL);
    }

    private void testLeftOuterJoinSimpleStaticEmpty(JoinControl joinControl) {
        final Table left = TableTools.newTable(intCol("LK", 1, 2, 1, 3, 2, 1), intCol("LS", 1, 2, 3, 4, 5, 6));
        final Table right = TableTools.newTable(intCol("RK", 1, 1, 2), intCol("RS", 10, 20, 30));
        final Table leftEmpty = TableTools.newTable(intCol("LK"), intCol("LS"));
        final Table rightEmpty = TableTools.newTable(intCol("RK"), intCol("RS"));

        final Table left1 = leftEmpty.join(right, "LK=RK");
        final Table emptyResult = newTable(intCol("LK"), intCol("LS"), intCol("RK"), intCol("RS"));
        assertTableEquals(emptyResult, left1);
        final Table right1 = left.join(rightEmpty, "LK=RK");
        assertTableEquals(emptyResult, right1);

        final Table left2 = CrossJoinHelper.leftOuterJoin((QueryTable) leftEmpty, (QueryTable) right,
                MatchPairFactory.getExpressions("LK=RK"), MatchPairFactory.getExpressions("RS"), 1, joinControl);
        assertTableEquals(newTable(intCol("LK"), intCol("LS"), intCol("RS")), left2);
        final Table right2 = CrossJoinHelper.leftOuterJoin((QueryTable) left, (QueryTable) rightEmpty,
                MatchPairFactory.getExpressions("LK=RK"), MatchPairFactory.getExpressions("RS"), 1, joinControl);
        assertTableEquals(TableTools.newTable(intCol("LK", 1, 2, 1, 3, 2, 1), intCol("LS", 1, 2, 3, 4, 5, 6),
                intCol("RS", NULL_INT, NULL_INT, NULL_INT, NULL_INT, NULL_INT, NULL_INT)), right2);
    }

    public void testLeftOuterJoinSimpleIncremental() {
        testLeftOuterJoinSimpleIncremental(TestJoinControl.BUILD_LEFT_CONTROL);
    }

    private void testLeftOuterJoinSimpleIncremental(JoinControl joinControl) {
        final QueryTable left =
                TstUtils.testRefreshingTable(intCol("LK", 1, 2, 1, 3, 2, 1), intCol("LS", 1, 2, 3, 4, 5, 6));
        final QueryTable right = TstUtils.testRefreshingTable(intCol("RK", 1, 1, 2), intCol("RS", 10, 20, 30));

        final Table joined = CrossJoinHelper.leftOuterJoin(left, right, MatchPairFactory.getExpressions("LK=RK"),
                MatchPairFactory.getExpressions("RS"), 1, joinControl);
        assertTableEquals(TableTools.newTable(intCol("LK", 1, 1, 2, 1, 1, 3, 2, 1, 1),
                intCol("LS", 1, 1, 2, 3, 3, 4, 5, 6, 6), intCol("RS", 10, 20, 30, 10, 20, NULL_INT, 30, 10, 20)),
                joined);

        final ErrorListener errorListener = makeAndListenToValidator((QueryTable) joined);
        final PrintListener printListener = new PrintListener("joined", joined);
        final SimpleListener listener = new SimpleListener(joined);
        joined.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(right, i(0, 1));
            right.notifyListeners(i(), i(0, 1), i());
        });

        // we should remove from the right rows, but add back a null row
        assertEquals(1, listener.count);
        assertEquals(i(0, 1, 4, 5, 10, 11), listener.update.removed());
        assertEquals(i(), listener.update.modified());
        assertEquals(i(0, 4, 10), listener.update.added());
        listener.reset();

        assertTableEquals(TableTools.newTable(intCol("LK", 1, 2, 1, 3, 2, 1), intCol("LS", 1, 2, 3, 4, 5, 6),
                intCol("RS", NULL_INT, 30, NULL_INT, NULL_INT, 30, NULL_INT)), joined);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(7), intCol("RK", 3), intCol("RS", 40));
            right.notifyListeners(i(7), i(), i());
        });

        TableTools.showWithRowSet(left);
        TableTools.showWithRowSet(right);
        TableTools.showWithRowSet(joined);

        assertTableEquals(TableTools.newTable(intCol("LK", 1, 2, 1, 3, 2, 1), intCol("LS", 1, 2, 3, 4, 5, 6),
                intCol("RS", NULL_INT, 30, NULL_INT, 40, 30, NULL_INT)), joined);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(8, 9), intCol("RK", 3, 3), intCol("RS", 50, 60));
            right.notifyListeners(i(8, 9), i(), i());
            addToTable(left, i(7), intCol("LK", 1), intCol("LS", 8));
            left.notifyListeners(i(7), i(), i());
        });

        assertTableEquals(
                TableTools.newTable(intCol("LK", 1, 2, 1, 3, 3, 3, 2, 1, 1), intCol("LS", 1, 2, 3, 4, 4, 4, 5, 6, 8),
                        intCol("RS", NULL_INT, 30, NULL_INT, 40, 50, 60, 30, NULL_INT, NULL_INT)),
                joined);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(0), intCol("RK", 4), intCol("RS", 70));
            final TableUpdateImpl update = new TableUpdateImpl(i(0), i(), i(), RowSetShiftData.EMPTY,
                    right.newModifiedColumnSet("RS"));
            right.notifyListeners(update);
        });
        System.out.println("Create K=4");
        TableTools.showWithRowSet(left);
        TableTools.showWithRowSet(right);
        TableTools.showWithRowSet(joined);
        assertTableEquals(
                TableTools.newTable(intCol("LK", 1, 2, 1, 3, 3, 3, 2, 1, 1), intCol("LS", 1, 2, 3, 4, 4, 4, 5, 6, 8),
                        intCol("RS", NULL_INT, 30, NULL_INT, 40, 50, 60, 30, NULL_INT, NULL_INT)),
                joined);

        System.out.println("Activate K=4");
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(left, i(0, 11), intCol("LK", 4, 3), intCol("LS", 1, 10));
            removeRows(left, i(1));
            left.notifyListeners(new TableUpdateImpl(i(11), i(1), i(0), RowSetShiftData.EMPTY,
                    left.newModifiedColumnSet("LK")));
        });

        TableTools.showWithRowSet(left);
        TableTools.showWithRowSet(right);
        TableTools.showWithRowSet(joined);
        assertTableEquals(TableTools.newTable(intCol("LK", 4, 1, 3, 3, 3, 2, 1, 1, 3, 3, 3),
                intCol("LS", 1, 3, 4, 4, 4, 5, 6, 8, 10, 10, 10),
                intCol("RS", 70, NULL_INT, 40, 50, 60, 30, NULL_INT, NULL_INT, 40, 50, 60)), joined);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(left, i(1), intCol("LK", 4), intCol("LS", 1));
            removeRows(left, i(0));
            final RowSetShiftData.Builder shiftBuilder1 = new RowSetShiftData.Builder();
            shiftBuilder1.shiftRange(0, 0, 1);
            left.notifyListeners(
                    new TableUpdateImpl(i(), i(), i(), shiftBuilder1.build(), ModifiedColumnSet.EMPTY));
        });
        assertTableEquals(TableTools.newTable(intCol("LK", 4, 1, 3, 3, 3, 2, 1, 1, 3, 3, 3),
                intCol("LS", 1, 3, 4, 4, 4, 5, 6, 8, 10, 10, 10),
                intCol("RS", 70, NULL_INT, 40, 50, 60, 30, NULL_INT, NULL_INT, 40, 50, 60)), joined);

        updateGraph.runWithinUnitTestCycle(() -> {

            addToTable(right, i(16, 17), intCol("RK", 3, 3), intCol("RS", 50, 60));
            removeRows(right, i(8, 9));
            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
            shiftBuilder.shiftRange(8, 16, 8);
            right.notifyListeners(
                    new TableUpdateImpl(i(), i(), i(), shiftBuilder.build(), ModifiedColumnSet.EMPTY));
        });
        assertTableEquals(TableTools.newTable(intCol("LK", 4, 1, 3, 3, 3, 2, 1, 1, 3, 3, 3),
                intCol("LS", 1, 3, 4, 4, 4, 5, 6, 8, 10, 10, 10),
                intCol("RS", 70, NULL_INT, 40, 50, 60, 30, NULL_INT, NULL_INT, 40, 50, 60)), joined);
    }

    public void testLeftOuterJoinShiftAndTransitionToFull() {
        final QueryTable left = TstUtils.testRefreshingTable(intCol("LK", 1), intCol("LS", 1));
        final QueryTable right = TstUtils.testRefreshingTable(intCol("RK", 2), intCol("RS", 10));

        final Table joined = CrossJoinHelper.leftOuterJoin(left, right,
                MatchPairFactory.getExpressions("LK=RK"), MatchPairFactory.getExpressions("RS"), 1,
                TestJoinControl.DEFAULT_JOIN_CONTROL);
        assertTableEquals(TableTools.newTable(intCol("LK", 1), intCol("LS", 1), intCol("RS", NULL_INT)), joined);

        final ErrorListener errorListener = makeAndListenToValidator((QueryTable) joined);
        final PrintListener printListener = new PrintListener("joined", joined);
        final SimpleListener listener = new SimpleListener(joined);
        joined.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(1), intCol("RK", 1), intCol("RS", 20));
            right.notifyListeners(i(1), i(), i());

            removeRows(left, i(0));
            addToTable(left, i(1), intCol("LK", 1), intCol("LS", 1));

            final RowSetShiftData.Builder shiftData = new RowSetShiftData.Builder();
            shiftData.shiftRange(0, 0, 1);
            left.notifyListeners(
                    new TableUpdateImpl(i(), i(), i(), shiftData.build(), ModifiedColumnSet.EMPTY));
        });

        TableTools.showWithRowSet(left);
        TableTools.showWithRowSet(right);
        TableTools.showWithRowSet(joined);

        assertTableEquals(TableTools.newTable(intCol("LK", 1), intCol("LS", 1), intCol("RS", 20)), joined);

        // we should remove the null left row, and add back our new right one
        assertEquals(1, listener.count);
        assertEquals(i(0), listener.update.removed());
        assertEquals(i(), listener.update.modified());
        assertEquals(i(2), listener.update.added());
        final RowSetShiftData.Builder resultShiftBuilder = new RowSetShiftData.Builder();
        resultShiftBuilder.shiftRange(0, 1, 2);
        final RowSetShiftData resultShift = resultShiftBuilder.build();
        assertEquals(resultShift, listener.update.shifted());
        listener.reset();
    }

    public void testLeftOuterJoinShiftAndRightBitsIncrease() {
        final QueryTable left = TstUtils.testRefreshingTable(i(1).toTracking(), intCol("LK", 1), intCol("LS", 1));
        final QueryTable right = TstUtils.testRefreshingTable(intCol("RK", 2), intCol("RS", 10));

        final Table joined = CrossJoinHelper.leftOuterJoin(left, right, MatchPairFactory.getExpressions("LK=RK"),
                MatchPairFactory.getExpressions("RS"), 1, TestJoinControl.DEFAULT_JOIN_CONTROL);
        assertTableEquals(TableTools.newTable(intCol("LK", 1), intCol("LS", 1), intCol("RS", NULL_INT)), joined);

        final ErrorListener errorListener = makeAndListenToValidator((QueryTable) joined);
        final PrintListener printListener = new PrintListener("joined", joined);
        final SimpleListener listener = new SimpleListener(joined);
        joined.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(1, 2, 3), intCol("RK", 1, 2, 2), intCol("RS", 20, 30, 40));
            right.notifyListeners(i(1, 2, 3), i(), i());

            removeRows(left, i(1));
            addToTable(left, i(2), intCol("LK", 1), intCol("LS", 1));

            final RowSetShiftData.Builder shiftData = new RowSetShiftData.Builder();
            shiftData.shiftRange(1, 1, 1);
            left.notifyListeners(
                    new TableUpdateImpl(i(), i(), i(), shiftData.build(), ModifiedColumnSet.EMPTY));
        });

        TableTools.showWithRowSet(left);
        TableTools.showWithRowSet(right);
        TableTools.showWithRowSet(joined);

        assertTableEquals(TableTools.newTable(intCol("LK", 1), intCol("LS", 1), intCol("RS", 20)), joined);

        // we should remove the null left row, and add back our new right one
        assertEquals(1, listener.count);
        assertEquals(i(2), listener.update.removed());
        assertEquals(i(), listener.update.modified());
        assertEquals(i(8), listener.update.added());
        final RowSetShiftData.Builder resultShiftBuilder = new RowSetShiftData.Builder();
        resultShiftBuilder.shiftRange(2, 3, 6);
        final RowSetShiftData resultShift = resultShiftBuilder.build();
        assertEquals(resultShift, listener.update.shifted());
        listener.reset();
    }


    public void testLeftOuterJoinShiftAndTransitionToEmpty() {
        final QueryTable left = TstUtils.testRefreshingTable(i(10).toTracking(), intCol("LK", 1), intCol("LS", 1));
        final QueryTable right = TstUtils.testRefreshingTable(intCol("RK", 1), intCol("RS", 10));

        final Table joined = CrossJoinHelper.leftOuterJoin(left, right, MatchPairFactory.getExpressions("LK=RK"),
                MatchPairFactory.getExpressions("RS"), 1, TestJoinControl.DEFAULT_JOIN_CONTROL);
        assertTableEquals(TableTools.newTable(intCol("LK", 1), intCol("LS", 1), intCol("RS", 10)), joined);

        final ErrorListener errorListener = makeAndListenToValidator((QueryTable) joined);
        final PrintListener printListener = new PrintListener("joined", joined);
        final SimpleListener listener = new SimpleListener(joined);
        joined.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(right, i(0));
            right.notifyListeners(i(), i(0), i());

            removeRows(left, i(10));
            addToTable(left, i(15), intCol("LK", 1), intCol("LS", 1));

            final RowSetShiftData.Builder shiftData = new RowSetShiftData.Builder();
            shiftData.shiftRange(10, 10, 5);
            left.notifyListeners(
                    new TableUpdateImpl(i(), i(), i(), shiftData.build(), ModifiedColumnSet.EMPTY));
        });

        TableTools.showWithRowSet(left);
        TableTools.showWithRowSet(right);
        TableTools.showWithRowSet(joined);

        assertTableEquals(TableTools.newTable(intCol("LK", 1), intCol("LS", 1), intCol("RS", NULL_INT)), joined);

        // we should remove the null left row, and add back our new right one
        assertEquals(1, listener.count);
        assertEquals(i(20), listener.update.removed());
        assertEquals(i(), listener.update.modified());
        assertEquals(i(30), listener.update.added());
        final RowSetShiftData.Builder resultShiftBuilder = new RowSetShiftData.Builder();
        resultShiftBuilder.shiftRange(20, 21, 10);
        final RowSetShiftData resultShift = resultShiftBuilder.build();
        assertEquals(resultShift, listener.update.shifted());
        listener.reset();
    }

    public void testLeftOuterJoinSimpleLeftIncremental() {
        testLeftOuterJoinSimpleLeftIncremental(TestJoinControl.BUILD_LEFT_CONTROL);
    }

    private void testLeftOuterJoinSimpleLeftIncremental(JoinControl joinControl) {
        final QueryTable left =
                TstUtils.testRefreshingTable(intCol("LK", 1, 2, 1, 3, 2, 1), intCol("LS", 1, 2, 3, 4, 5, 6));
        final Table right = TableTools.newTable(intCol("RK", 1, 1, 2), intCol("RS", 10, 20, 30));

        final Table joined = CrossJoinHelper.leftOuterJoin(left, (QueryTable) right,
                MatchPairFactory.getExpressions("LK=RK"), MatchPairFactory.getExpressions("RS"), 1, joinControl);
        assertTableEquals(TableTools.newTable(intCol("LK", 1, 1, 2, 1, 1, 3, 2, 1, 1),
                intCol("LS", 1, 1, 2, 3, 3, 4, 5, 6, 6), intCol("RS", 10, 20, 30, 10, 20, NULL_INT, 30, 10, 20)),
                joined);

        final ErrorListener errorListener = makeAndListenToValidator((QueryTable) joined);
        final PrintListener printListener = new PrintListener("joined", joined);
        final SimpleListener listener = new SimpleListener(joined);
        (joined).addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(left, i(7), intCol("LK", 1), intCol("LS", 8));
            left.notifyListeners(i(7), i(), i());
        });

        assertTableEquals(TableTools.newTable(intCol("LK", 1, 1, 2, 1, 1, 3, 2, 1, 1, 1, 1),
                intCol("LS", 1, 1, 2, 3, 3, 4, 5, 6, 6, 8, 8),
                intCol("RS", 10, 20, 30, 10, 20, NULL_INT, 30, 10, 20, 10, 20)), joined);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(left, i(0, 11), intCol("LK", 4, 3), intCol("LS", 1, 10));
            removeRows(left, i(1));
            left.notifyListeners(new TableUpdateImpl(i(11), i(1), i(0), RowSetShiftData.EMPTY,
                    left.newModifiedColumnSet("LK")));
        });

        TableTools.showWithRowSet(left);
        TableTools.showWithRowSet(right);
        TableTools.showWithRowSet(joined);

        assertTableEquals(TableTools.newTable(intCol("LK", 4, 1, 1, 3, 2, 1, 1, 1, 1, 3),
                intCol("LS", 1, 3, 3, 4, 5, 6, 6, 8, 8, 10),
                intCol("RS", NULL_INT, 10, 20, NULL_INT, 30, 10, 20, 10, 20, NULL_INT)), joined);
    }

    public void testLeftTickingRightStaticRemoveWithoutRightState() {
        final QueryTable left = TstUtils.testRefreshingTable(i(10, 20).toTracking(), intCol("LK", 1, 1),
                intCol("LS", 1, 2));
        final Table right = TableTools.newTable(intCol("RK", 2), intCol("RS", 10));

        final Table joined = CrossJoinHelper.leftOuterJoin(left, (QueryTable) right,
                MatchPairFactory.getExpressions("LK=RK"), MatchPairFactory.getExpressions("RS"), 1,
                TestJoinControl.DEFAULT_JOIN_CONTROL);
        assertTableEquals(TableTools.newTable(intCol("LK", 1, 1), intCol("LS", 1, 2), intCol("RS", NULL_INT, NULL_INT)),
                joined);

        final ErrorListener errorListener = makeAndListenToValidator((QueryTable) joined);
        final PrintListener printListener = new PrintListener("joined", joined);
        final SimpleListener listener = new SimpleListener(joined);
        joined.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(left, i(20), intCol("LK", 1), intCol("LS", 3));
            removeRows(left, i(10));
            left.notifyListeners(i(), i(10), i(20));
        });

        TableTools.showWithRowSet(left);
        TableTools.showWithRowSet(right);
        TableTools.showWithRowSet(joined);

        assertTableEquals(TableTools.newTable(intCol("LK", 1), intCol("LS", 3), intCol("RS", NULL_INT)), joined);

        // we should remove the removed left row, and modify the modified left row
        assertEquals(1, listener.count);
        assertEquals(i(20), listener.update.removed());
        assertEquals(i(40), listener.update.modified());
        assertEquals(i(), listener.update.added());
        assertEquals(RowSetShiftData.EMPTY, listener.update.shifted());
        listener.reset();
    }


    public void testLeftOuterJoinSimpleRightIncremental() {
        testLeftOuterJoinSimpleRightIncremental(TestJoinControl.DEFAULT_JOIN_CONTROL);
    }

    private void testLeftOuterJoinSimpleRightIncremental(JoinControl joinControl) {
        final QueryTable left =
                (QueryTable) TableTools.newTable(intCol("LK", 1, 2, 1, 3, 2, 1), intCol("LS", 1, 2, 3, 4, 5, 6));
        final QueryTable right = TstUtils.testRefreshingTable(intCol("RK", 1, 1, 2), intCol("RS", 10, 20, 30));

        final Table joined = CrossJoinHelper.leftOuterJoin(left, right, MatchPairFactory.getExpressions("LK=RK"),
                MatchPairFactory.getExpressions("RS"), 1, joinControl);
        assertTableEquals(TableTools.newTable(intCol("LK", 1, 1, 2, 1, 1, 3, 2, 1, 1),
                intCol("LS", 1, 1, 2, 3, 3, 4, 5, 6, 6), intCol("RS", 10, 20, 30, 10, 20, NULL_INT, 30, 10, 20)),
                joined);

        final ErrorListener errorListener = makeAndListenToValidator((QueryTable) joined);
        final PrintListener printListener = new PrintListener("joined", joined);
        final SimpleListener listener = new SimpleListener(joined);
        joined.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(right, i(0, 1));
            right.notifyListeners(i(), i(0, 1), i());
        });

        // we should remove from the right rows, but add back a null row
        assertEquals(1, listener.count);
        assertEquals(i(0, 1, 4, 5, 10, 11), listener.update.removed());
        assertEquals(i(), listener.update.modified());
        assertEquals(i(0, 4, 10), listener.update.added());
        listener.reset();

        assertTableEquals(TableTools.newTable(intCol("LK", 1, 2, 1, 3, 2, 1), intCol("LS", 1, 2, 3, 4, 5, 6),
                intCol("RS", NULL_INT, 30, NULL_INT, NULL_INT, 30, NULL_INT)), joined);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(7), intCol("RK", 3), intCol("RS", 40));
            right.notifyListeners(i(7), i(), i());
        });

        assertTableEquals(TableTools.newTable(intCol("LK", 1, 2, 1, 3, 2, 1), intCol("LS", 1, 2, 3, 4, 5, 6),
                intCol("RS", NULL_INT, 30, NULL_INT, 40, 30, NULL_INT)), joined);

        // we should remove the null right row, but add back a proper row
        assertEquals(1, listener.count);
        assertEquals(i(6), listener.update.removed());
        assertEquals(i(), listener.update.modified());
        assertEquals(i(6), listener.update.added());
        listener.reset();

        TableTools.showWithRowSet(joined);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(8, 9), intCol("RK", 3, 3), intCol("RS", 50, 60));
            right.notifyListeners(i(8, 9), i(), i());
        });

        TableTools.showWithRowSet(left);
        TableTools.showWithRowSet(right);
        TableTools.showWithRowSet(joined);

        assertTableEquals(TableTools.newTable(intCol("LK", 1, 2, 1, 3, 3, 3, 2, 1),
                intCol("LS", 1, 2, 3, 4, 4, 4, 5, 6), intCol("RS", NULL_INT, 30, NULL_INT, 40, 50, 60, 30, NULL_INT)),
                joined);
        assertEquals(1, listener.count);
        assertEquals(i(), listener.update.removed());
        assertEquals(i(), listener.update.modified());
        assertEquals(i(13, 14), listener.update.added());
        assertEquals(5, listener.update.shifted().size());
        for (int ii = 1; ii < 6; ++ii) {
            assertEquals(ii * 2, listener.update.shifted().getBeginRange(ii - 1));
            assertEquals(ii * 2, listener.update.shifted().getEndRange(ii - 1));
            assertEquals(ii * 2, listener.update.shifted().getShiftDelta(ii - 1));
        }
        listener.reset();

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(0), intCol("RK", 4), intCol("RS", 70));
            final TableUpdateImpl update = new TableUpdateImpl(i(0), i(), i(), RowSetShiftData.EMPTY,
                    right.newModifiedColumnSet("RS"));
            right.notifyListeners(update);
        });
        assertTableEquals(TableTools.newTable(intCol("LK", 1, 2, 1, 3, 3, 3, 2, 1),
                intCol("LS", 1, 2, 3, 4, 4, 4, 5, 6), intCol("RS", NULL_INT, 30, NULL_INT, 40, 50, 60, 30, NULL_INT)),
                joined);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(16, 17), intCol("RK", 3, 3), intCol("RS", 50, 60));
            removeRows(right, i(8, 9));
            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
            shiftBuilder.shiftRange(8, 16, 8);
            right.notifyListeners(
                    new TableUpdateImpl(i(), i(), i(), shiftBuilder.build(), ModifiedColumnSet.EMPTY));
        });
        assertTableEquals(TableTools.newTable(intCol("LK", 1, 2, 1, 3, 3, 3, 2, 1),
                intCol("LS", 1, 2, 3, 4, 4, 4, 5, 6), intCol("RS", NULL_INT, 30, NULL_INT, 40, 50, 60, 30, NULL_INT)),
                joined);
    }

    private ErrorListener makeAndListenToValidator(QueryTable joined) {
        final TableUpdateValidator validator = TableUpdateValidator.make("joined", joined);
        final QueryTable validatorResult = validator.getResultTable();
        final ErrorListener errorListener = new ErrorListener(validatorResult);
        validatorResult.addUpdateListener(errorListener);
        return errorListener;
    }

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

    public void testLargeStaticJoinWithOverflow() {
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
                        TestJoinControl.OVERFLOW_BUILD_LEFT);
                testStaticJoin(types, cardinality, types.length, types.length, leftTicking, rightTicking,
                        TestJoinControl.OVERFLOW_BUILD_RIGHT);
            }
        }
    }

    // generate a table such that all pairs of types exist and are part of the cross-join
    private void testStaticJoin(final String[] types, final int[] cardinality, int maxLeftType, int maxRightType,
            boolean leftTicking, boolean rightTicking, JoinControl joinControl) {
        Assert.eq(types.length, "types.length", cardinality.length, "cardinality.length");

        long nextLeftRow = 0;
        final ArrayList<String> leftKeys = new ArrayList<>();
        final TLongArrayList leftData = new TLongArrayList();

        long nextRightRow = 0;
        final ArrayList<String> rightKeys = new ArrayList<>();
        final TLongArrayList rightData = new TLongArrayList();

        int leftReserveSize = 0;
        for (int i = 0; i < maxLeftType; ++i) {
            final int leftSize = cardinality[i];
            leftReserveSize += leftSize;
        }
        int rightReserveSize = 0;
        for (int j = 0; j < maxRightType; ++j) {
            final int rightSize = cardinality[j];
            rightReserveSize += rightSize;
        }
        leftKeys.ensureCapacity(leftReserveSize);
        leftData.ensureCapacity(leftReserveSize);
        rightKeys.ensureCapacity(rightReserveSize);
        rightData.ensureCapacity(rightReserveSize);

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

                final int expectedForKey = leftSize * Math.max(rightSize, 1);
                expectedSize += expectedForKey;
                Assert.eqFalse(expectedByKey.containsKey(sharedKey), "expectedByKey.containsKey(sharedKey)");
                expectedByKey.put(sharedKey, new MutableLong(expectedForKey));
            }
        }

        final QueryTable left = (QueryTable) TableTools.newTable(
                stringCol("sharedKey", leftKeys.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY)),
                longCol("leftData", leftData.toArray()));
        if (leftTicking) {
            left.setRefreshing(true);
        }
        final QueryTable right = (QueryTable) TableTools.newTable(
                stringCol("sharedKey", rightKeys.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY)),
                longCol("rightData", rightData.toArray()));
        if (rightTicking) {
            right.setRefreshing(true);
        }

        final Table joined = doLeftOuterJoin(left, right, "sharedKey", "", joinControl);
        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Left Table (" + left.size() + " rows): ");
            TableTools.showWithRowSet(left, 100);
            System.out.println("\nRight Table (" + right.size() + " rows): ");
            TableTools.showWithRowSet(right, 100);
            System.out.println("\nCross Join Table (" + joined.size() + " rows): ");
            TableTools.showWithRowSet(joined, 100);
        }

        Assert.eq(expectedSize, "expectedSize", joined.size(), "joined.size()");
        final ColumnSource<?> keyColumn = joined.getColumnSource("sharedKey");
        final ColumnSource<?> leftColumn = joined.getColumnSource("leftData");
        final ColumnSource<?> rightColumn = joined.getColumnSource("rightData");

        final MutableLong lastLeftId = new MutableLong();
        final MutableLong lastRightId = new MutableLong();
        final MutableObject<String> lastSharedKey = new MutableObject<>();

        joined.getRowSet().forAllRowKeys(ii -> {
            final String sharedKey = (String) keyColumn.get(ii);

            final long leftId = leftColumn.getLong(ii);
            final long rightId = rightColumn.getLong(ii);
            if (lastSharedKey.getValue() != null && lastSharedKey.getValue().equals(sharedKey)) {
                Assert.leq(lastLeftId.longValue(), "lastLeftId.longValue()", leftId, "leftId");
                if (lastLeftId.longValue() == leftId) {
                    Assert.lt(lastRightId.longValue(), "lastRightId.longValue()", rightId, "rightId");
                }
            } else {
                lastSharedKey.setValue(sharedKey);
                lastLeftId.setValue(leftId);
            }
            lastRightId.setValue(rightId);

            final MutableLong remainingCount = expectedByKey.get(sharedKey);
            Assert.neqNull(remainingCount, "remainingCount");
            Assert.gtZero(remainingCount.longValue(), "remainingCount.longValue()");
            remainingCount.decrement();
        });

        for (final Map.Entry<String, MutableLong> entry : expectedByKey.entrySet()) {
            Assert.eqZero(entry.getValue().longValue(), "entry.getValue().longValue");
        }
    }

    public void testStaticVsNaturalJoin() {
        final int size = 10000;
        final Table x = TableTools.emptyTable(size).update("Col1=i");
        final Table y = TableTools.emptyTable(size).update("Col2=i*2");
        final Table z = doLeftOuterJoin(x, y, "Col1=Col2");
        final Table z2 = x.naturalJoin(y, "Col1=Col2");

        assertTableEquals(z2, z);
    }

    public void testStaticVsNaturalJoin2() {
        final int size = 10000;

        final QueryTable xqt = TstUtils.testRefreshingTable(RowSetFactory.flat(size).toTracking());
        final QueryTable yqt = TstUtils.testRefreshingTable(RowSetFactory.flat(size).toTracking());

        final Table x = xqt.update("Col1=i");
        final Table y = yqt.update("Col2=i*2");
        final Table z = doLeftOuterJoin(x, y, "Col1=Col2");
        final Table z2 = doLeftOuterJoin(x, y, "Col1=Col2");

        assertTableEquals(z2, z);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            xqt.getRowSet().writableCast().insertRange(size, size * 2);
            xqt.notifyListeners(RowSetFactory.fromRange(size, size * 2), i(), i());
        });

        assertTableEquals(z2, z);

        updateGraph.runWithinUnitTestCycle(() -> {
            yqt.getRowSet().writableCast().insertRange(size, size * 2);
            yqt.notifyListeners(RowSetFactory.fromRange(size, size * 2), i(), i());
        });

        assertTableEquals(z2, z);
    }

    public void testIncrementalOverflow() {
        final int[] sizes = {100};

        for (int size : sizes) {
            for (int seed = 0; seed < MAX_SEEDS; ++seed) {
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testIncrementalWithKeyColumns("size == " + size, size, seed, false, new MutableInt(20),
                            TestJoinControl.OVERFLOW_BUILD_LEFT);
                }
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testIncrementalWithKeyColumns("size == " + size, size, seed, false, new MutableInt(20),
                            TestJoinControl.OVERFLOW_BUILD_RIGHT);
                }
            }
        }
    }

    public void testLeftIncrementalOverflowRemove() {
        final int size = 32;
        final int sentinelOffset = 10000;

        final QueryTable leftTable = TstUtils.testRefreshingTable(intCol("LK", IntStream.range(0, size).toArray()));
        final Table rightTable =
                TableTools.newTable(intCol("RK", IntStream.range(0, size).map(vv -> (vv / 2) * 2).toArray()),
                        intCol("RS", IntStream.range(sentinelOffset, sentinelOffset + size).toArray()));

        final Table result =
                doLeftOuterJoin(leftTable, rightTable, "LK=RK", "RK,RS", TestJoinControl.OVERFLOW_BUILD_RIGHT);

        TableTools.showWithRowSet(result);

        final Table expected = TstUtils.testRefreshingTable(
                intCol("LK",
                        IntStream.range(0, size).flatMap(vv -> vv % 2 == 0 ? IntStream.of(vv, vv) : IntStream.of(vv))
                                .toArray()),
                intCol("RK",
                        IntStream.range(0, size)
                                .flatMap(vv -> vv % 2 == 0 ? IntStream.of(vv, vv) : IntStream.of(NULL_INT)).toArray()),
                intCol("RS",
                        IntStream.range(0, size)
                                .flatMap(vv -> vv % 2 == 0 ? IntStream.of(sentinelOffset + vv, sentinelOffset + vv + 1)
                                        : IntStream.of(NULL_INT))
                                .toArray()));

        assertTableEquals(expected, result);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet toRemove = RowSetFactory.fromKeys(
                    LongStream.range(0, size / 4).map(vv1 -> vv1 * 4 + 3).toArray());
            removeRows(leftTable, toRemove);
            leftTable.notifyListeners(
                    new TableUpdateImpl(i(), toRemove, i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        });

        TableTools.showWithRowSet(result);

        final Table expected2 = TstUtils.testRefreshingTable(
                intCol("LK",
                        IntStream.range(0, size).filter(vv -> vv % 4 != 3)
                                .flatMap(vv -> vv % 2 == 0 ? IntStream.of(vv, vv) : IntStream.of(vv)).toArray()),
                intCol("RK",
                        IntStream.range(0, size).filter(vv -> vv % 4 != 3)
                                .flatMap(vv -> vv % 2 == 0 ? IntStream.of(vv, vv) : IntStream.of(NULL_INT)).toArray()),
                intCol("RS",
                        IntStream.range(0, size).filter(vv -> vv % 4 != 3)
                                .flatMap(vv -> vv % 2 == 0 ? IntStream.of(sentinelOffset + vv, sentinelOffset + vv + 1)
                                        : IntStream.of(NULL_INT))
                                .toArray()));
        assertTableEquals(expected2, result);
    }

    public void testRightIncrementalOverflowModifyKeys() {
        final int size = 32;
        final int sentinelOffset = 10000;

        final Table leftTable = TableTools.newTable(intCol("LK", IntStream.range(0, size).toArray()));
        final QueryTable rightTable =
                TstUtils.testRefreshingTable(intCol("RK", IntStream.range(0, size).map(vv -> (vv / 2) * 2).toArray()),
                        intCol("RS", IntStream.range(sentinelOffset, sentinelOffset + size).toArray()));

        final Table result =
                doLeftOuterJoin(leftTable, rightTable, "LK=RK", "RK,RS", TestJoinControl.OVERFLOW_BUILD_LEFT);

        TableTools.showWithRowSet(result);

        final Table expected = TableTools.newTable(
                intCol("LK",
                        IntStream.range(0, size).flatMap(vv -> vv % 2 == 0 ? IntStream.of(vv, vv) : IntStream.of(vv))
                                .toArray()),
                intCol("RK",
                        IntStream.range(0, size)
                                .flatMap(vv -> vv % 2 == 0 ? IntStream.of(vv, vv) : IntStream.of(NULL_INT)).toArray()),
                intCol("RS",
                        IntStream.range(0, size)
                                .flatMap(vv -> vv % 2 == 0 ? IntStream.of(sentinelOffset + vv, sentinelOffset + vv + 1)
                                        : IntStream.of(NULL_INT))
                                .toArray()));

        assertTableEquals(expected, result);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet toModify =
                    RowSetFactory.fromKeys(LongStream.range(0, size / 4).map(vv1 -> vv1 * 4 + 3).toArray());
            addToTable(rightTable, toModify,
                    intCol("RK", IntStream.range(0, size / 4).map(vv1 -> vv1 + (size * 4)).toArray()),
                    intCol("RS", IntStream.range(0, size / 4).map(vv1 -> 2 * sentinelOffset + vv1).toArray()));
            rightTable.notifyListeners(new TableUpdateImpl(i(), i(), toModify, RowSetShiftData.EMPTY,
                    rightTable.newModifiedColumnSet("RK")));
        });
        TableTools.show(rightTable);
        TableTools.show(result);

        final Table expected2 = TableTools.newTable(
                intCol("LK",
                        IntStream.range(0, size)
                                .flatMap(vv -> vv % 2 == 0 ? (vv % 4 == 0 ? IntStream.of(vv, vv) : IntStream.of(vv))
                                        : IntStream.of(vv))
                                .toArray()),
                intCol("RK",
                        IntStream.range(0, size)
                                .flatMap(vv -> vv % 2 == 0 ? (vv % 4 == 0 ? IntStream.of(vv, vv) : IntStream.of(vv))
                                        : IntStream.of(NULL_INT))
                                .toArray()),
                intCol("RS",
                        IntStream
                                .range(0,
                                        size)
                                .flatMap(vv -> vv % 2 == 0
                                        ? (vv % 4 == 0 ? IntStream.of(sentinelOffset + vv, sentinelOffset + vv + 1)
                                                : IntStream.of(sentinelOffset + vv))
                                        : IntStream.of(NULL_INT))
                                .toArray()));

        assertTableEquals(expected2, result);
    }

    public void testIncrementalWithKeyColumns() {
        final int maxSteps = 100;
        final int[] sizes = {10, 100, 1000};

        for (int size : sizes) {
            for (int seed = 0; seed < MAX_SEEDS; ++seed) {
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testIncrementalWithKeyColumns("size == " + size, size, seed, false, new MutableInt(maxSteps),
                            TestJoinControl.DEFAULT_JOIN_CONTROL);
                }
            }
        }
    }

    public void testIncrementalWithKeyColumnArraysArray() {
        final int maxSteps = 10;
        final int[] sizes = {10, 10};

        for (int size : sizes) {
            for (int seed = 0; seed < MAX_SEEDS; ++seed) {
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testIncrementalWithKeyColumns("size == " + size, size, seed, true, new MutableInt(maxSteps),
                            TestJoinControl.DEFAULT_JOIN_CONTROL);
                }
            }
        }
    }

    protected void testIncrementalWithKeyColumns(final String ctxt, final int initialSize, final int seed,
            final boolean useArrays, final MutableInt numSteps, JoinControl joinControl) {
        final int maxSteps = numSteps.get();
        final Random random = new Random(seed);

        final int numGroups = (int) Math.max(4, Math.ceil(Math.sqrt(initialSize)));
        final ColumnInfo<?, ?>[] leftColumns =
                useArrays ? getIncrementalArrayColumnInfo("lt", numGroups) : getIncrementalColumnInfo("lt", numGroups);
        final QueryTable leftTicking = getTable(initialSize, random, leftColumns);

        final ColumnInfo<?, ?>[] rightColumns =
                useArrays ? getIncrementalArrayColumnInfo("rt", numGroups) : getIncrementalColumnInfo("rt", numGroups);
        final QueryTable rightTicking = getTable(initialSize, random, rightColumns);

        final QueryTable leftStatic = getTable(false, initialSize, random,
                useArrays ? getIncrementalArrayColumnInfo("ls", numGroups) : getIncrementalColumnInfo("ls", numGroups));
        final QueryTable rightStatic = getTable(false, initialSize, random,
                useArrays ? getIncrementalArrayColumnInfo("rs", numGroups) : getIncrementalColumnInfo("rs", numGroups));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> doLeftOuterJoin(leftTicking, rightTicking, "ltSym=rtSym", "", joinControl)),
                EvalNugget.from(() -> doLeftOuterJoin(leftStatic, rightTicking, "lsSym=rtSym", "", joinControl)),
                EvalNugget.from(() -> doLeftOuterJoin(leftTicking, rightStatic, "ltSym=rsSym", "", joinControl)),
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
            System.out.println("Seed = " + seed + ", size = " + initialSize + ", step = " + numSteps.get());

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

    public void testColumnSourceCanReuseContextWithSmallerOrderedKeys() {
        final QueryTable t1 = testRefreshingTable(i(0, 1).toTracking());
        final QueryTable t2 = (QueryTable) t1.update("K=k", "A=1");
        final QueryTable t3 = (QueryTable) testTable(i(2, 3).toTracking()).update("I=i", "A=1");
        final QueryTable jt = (QueryTable) doLeftOuterJoin(t2, t3, "A");

        final int CHUNK_SIZE = 4;
        final ColumnSource<Integer> column = jt.getColumnSource("I", int.class);
        try (final ColumnSource.FillContext context = column.makeFillContext(CHUNK_SIZE);
                final WritableIntChunk<Values> dest = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
                final ResettableWritableIntChunk<Values> rdest = ResettableWritableIntChunk.makeResettableChunk()) {
            column.fillChunk(context, rdest.resetFromChunk(dest, 0, 4), jt.getRowSet().subSetByPositionRange(0, 4));
            column.fillChunk(context, rdest.resetFromChunk(dest, 0, 2), jt.getRowSet().subSetByPositionRange(0, 2));
        }
    }

    public void testShiftingDuringRehash() {
        final int maxSteps = 2500;
        final MutableInt numSteps = new MutableInt();

        final QueryTable leftTicking = TstUtils.testRefreshingTable(i().toTracking(), longCol("intCol"));
        final QueryTable rightTicking = TstUtils.testRefreshingTable(i().toTracking(), longCol("intCol"));

        final JoinControl control = new JoinControl() {
            @Override
            public int initialBuildSize() {
                return 256;
            }

            @Override
            public double getMaximumLoadFactor() {
                return 20;
            }

            @Override
            public double getTargetLoadFactor() {
                return 19;
            }
        };

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> CrossJoinHelper.leftOuterJoin(leftTicking, rightTicking,
                        MatchPairFactory.getExpressions("intCol"), MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                        numRightBitsToReserve, control)),
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

    public void testMultipleKeyColumns() {
        testMultipleKeyColumns(true, true);
        testMultipleKeyColumns(true, false);
        testMultipleKeyColumns(false, true);
        testMultipleKeyColumns(false, false);
    }

    private void testMultipleKeyColumns(boolean leftRefreshing, boolean rightRefreshing) {
        final Table left = TableTools.newTable(stringCol("LK1", "A", "B", "C", "A", "B", "C"),
                col("LK2", true, true, true, false, false, false), intCol("LS", 1, 2, 3, 4, 5, 6));
        final Table right = TableTools.newTable(stringCol("RK1", "D", "B", "B", "D", "C", "C"),
                col("RK2", true, true, false, false, false, false), intCol("RS", 10, 12, 13, 14, 15, 16));

        if (leftRefreshing) {
            left.setRefreshing(true);
        }
        if (rightRefreshing) {
            right.setRefreshing(true);
        }

        final Table result = doLeftOuterJoin(left, right, "LK1=RK1,LK2=RK2", "RS");

        TableTools.showWithRowSet(left);
        TableTools.showWithRowSet(right);
        TableTools.showWithRowSet(result);

        final Table expected = TableTools.newTable(stringCol("LK1", "A", "B", "C", "A", "B", "C", "C"),
                col("LK2", true, true, true, false, false, false, false), intCol("LS", 1, 2, 3, 4, 5, 6, 6),
                intCol("RS", NULL_INT, 12, NULL_INT, NULL_INT, 13, 15, 16));
        assertTableEquals(expected, result);
    }
}
