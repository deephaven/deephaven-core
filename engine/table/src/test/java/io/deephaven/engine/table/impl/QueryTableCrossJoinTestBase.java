//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import com.google.common.collect.Maps;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.api.JoinMatch;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ResettableWritableIntChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.PrintListener;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.experimental.categories.Category;

import java.util.*;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
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

    public void testIncrementalZeroKeyJoin() {
        final int[] sizes = {10, 100, 1000};
        for (int size : sizes) {
            testIncrementalZeroKeyJoin("size == " + size, size, 0, new MutableInt(50));
        }
    }

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

    public void testLargeStaticOverflow() {
        final String[] types = new String[26];
        final int[] cardinality = new int[26];
        for (int i = 0; i < 26; ++i) {
            types[i] = String.valueOf('a' + i);
            cardinality[i] = i * i;
        }
        testStaticJoin(types, cardinality, types.length, types.length, false, false,
                TestJoinControl.OVERFLOW_JOIN_CONTROL);
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
                stringCol("sharedKey", leftKeys.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY)),
                longCol("leftData", leftData.toArray()));
        if (leftTicking) {
            left.setRefreshing(true);
        }

        final QueryTable right = (QueryTable) TableTools.newTable(stringCol("sharedKey",
                rightKeys.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY)),
                longCol("rightData", rightData.toArray()));
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

        QueryTable.USE_CHUNKED_CROSS_JOIN = false;
        final Table nonChunkedCrossJoin =
                left.join(right, List.of(JoinMatch.parse("sharedKey")), emptyList(), numRightBitsToReserve);
        QueryTable.USE_CHUNKED_CROSS_JOIN = true;
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

    public void testStaticVsNaturalJoin() {
        final int size = 10000;
        final Table x = TableTools.emptyTable(size).update("Col1=i");
        final Table y = TableTools.emptyTable(size).update("Col2=i*2");
        final Table z = x.join(y, "Col1=Col2");
        final Table z2 = x.naturalJoin(y, "Col1=Col2");
        final Table z3 = z2.where("!isNull(Col2)");

        assertTableEquals(z3, z);
    }

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
                return 20;
            }

            @Override
            double getTargetLoadFactor() {
                return 19;
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
                return 20;
            }

            @Override
            double getTargetLoadFactor() {
                return 19;
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
}
