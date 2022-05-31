package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.datastructures.util.CollectionUtil;
import com.google.common.collect.Maps;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.*;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.table.impl.TstUtils.*;

@Category(OutOfBandTest.class)
public abstract class QueryTableCrossJoinTestBase extends QueryTableTestBase {

    private final int numRightBitsToReserve;

    public QueryTableCrossJoinTestBase(int numRightBitsToReserve) {
        this.numRightBitsToReserve = numRightBitsToReserve;
    }

    private TstUtils.ColumnInfo<?, ?>[] getIncrementalColumnInfo(final String prefix, int numGroups) {
        String[] names = new String[] {"Sym", "IntCol"};

        return initColumnInfos(Arrays.stream(names).map(name -> prefix + name).toArray(String[]::new),
                new TstUtils.IntGenerator(0, numGroups - 1),
                new TstUtils.IntGenerator(10, 100000));
    }

    public void testZeroKeyJoinBitExpansionOnAdd() {
        // Looking to force our row set space to need more keys.
        final QueryTable lTable = testRefreshingTable(c("X", "to-remove", "b", "c", "d"));
        removeRows(lTable, i(0)); // row @ 0 does not need outer shifting
        final QueryTable rTable = testRefreshingTable(longCol("Y"));

        addToTable(rTable, i(1, (1 << 16) - 1), longCol("Y", 1, 2));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> lTable.join(rTable, numRightBitsToReserve)),
        };
        TstUtils.validate(en);

        final QueryTable jt = (QueryTable) lTable.join(rTable, numRightBitsToReserve);
        final io.deephaven.engine.table.impl.SimpleListener listener =
                new io.deephaven.engine.table.impl.SimpleListener(jt);
        jt.listenForUpdates(listener);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
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
        final QueryTable lTable = testRefreshingTable(c("X", "to-remove", "b", "c", "d"));
        removeRows(lTable, i(0)); // row @ 0 does not need outer shifting
        final QueryTable rTable = testRefreshingTable(longCol("Y"));

        final long origIndex = (1 << 16) - 1;
        final long newIndex = 1 << 16;
        addToTable(rTable, i(0, origIndex), longCol("Y", 1, 2));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> lTable.join(rTable, numRightBitsToReserve)),
        };
        TstUtils.validate(en);

        final QueryTable jt = (QueryTable) lTable.join(rTable, numRightBitsToReserve);
        final io.deephaven.engine.table.impl.SimpleListener listener =
                new io.deephaven.engine.table.impl.SimpleListener(jt);
        jt.listenForUpdates(listener);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
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
        final QueryTable lTable = testRefreshingTable(c("X", "to-remove", "b", "c", "d"));
        removeRows(lTable, i(0)); // row @ 0 does not need outer shifting
        final QueryTable rTable = testRefreshingTable(longCol("Y"));

        addToTable(rTable, i(1, 128, (1 << 16) - 1), longCol("Y", 1, 2, 3));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> lTable.join(rTable, numRightBitsToReserve)),
        };
        TstUtils.validate(en);

        final QueryTable jt = (QueryTable) lTable.join(rTable, numRightBitsToReserve);
        final io.deephaven.engine.table.impl.SimpleListener listener =
                new io.deephaven.engine.table.impl.SimpleListener(jt);
        jt.listenForUpdates(listener);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
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
        final QueryTable lTable = testRefreshingTable(c("X", "a", "b", "c", "d"));
        final QueryTable rTable = testRefreshingTable(longCol("Y"));

        addToTable(rTable, i(1, 128, (1 << 16) - 1), longCol("Y", 1, 2, 3));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> lTable.join(rTable, numRightBitsToReserve)),
        };
        TstUtils.validate(en);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            // left table
            removeRows(lTable, i(0, 1, 2, 3));
            addToTable(lTable, i(2, 4, 5, 7), c("X", "a", "b", "c", "d"));
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

    private void testIncrementalZeroKeyJoin(final String ctxt, final int size, final int seed,
            final MutableInt numSteps) {
        final int leftSize = (int) Math.ceil(Math.sqrt(size));

        final int maxSteps = numSteps.intValue();
        final Random random = new Random(seed);

        final int numGroups = (int) Math.max(4, Math.ceil(Math.sqrt(leftSize)));
        final TstUtils.ColumnInfo<?, ?>[] leftColumns = getIncrementalColumnInfo("lt", numGroups);
        final QueryTable leftTicking = getTable(leftSize, random, leftColumns);

        final TstUtils.ColumnInfo<?, ?>[] rightColumns = getIncrementalColumnInfo("rt", numGroups);
        final QueryTable rightTicking = getTable(size, random, rightColumns);

        final QueryTable leftStatic = getTable(false, leftSize, random, getIncrementalColumnInfo("ls", numGroups));
        final QueryTable rightStatic = getTable(false, size, random, getIncrementalColumnInfo("rs", numGroups));

        final EvalNugget[] en = new EvalNugget[] {
                // Zero-Key Joins
                EvalNugget.from(() -> leftTicking.join(rightTicking, numRightBitsToReserve)),
                EvalNugget.from(() -> leftStatic.join(rightTicking, numRightBitsToReserve)),
                EvalNugget.from(() -> leftTicking.join(rightStatic, numRightBitsToReserve)),
        };

        for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
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
            TstUtils.validate(ctxt + " step == " + numSteps.getValue(), en);
        }
    }

    public void testSmallStaticJoin() {
        final String[] types = new String[] {"single", "none", "multi"};
        final int[] cardinality = new int[] {1, 0, 3};
        for (int lt = 0; lt < 2; ++lt) {
            for (int rt = 0; rt < 2; ++rt) {
                boolean leftTicking = lt == 1;
                boolean rightTicking = rt == 1;
                testStaticJoin(types, cardinality, types.length, types.length, leftTicking, rightTicking);
                // force left build
                testStaticJoin(types, cardinality, 1, types.length, leftTicking, rightTicking);
                // force right build
                testStaticJoin(types, cardinality, types.length, 1, leftTicking, rightTicking);
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
                testStaticJoin(types, cardinality, types.length, types.length, leftTicking, rightTicking);
            }
        }
    }

    // generate a table such that all pairs of types exist and are part of the cross-join
    private void testStaticJoin(final String[] types, final int[] cardinality, int maxLeftType, int maxRightType,
            boolean leftTicking, boolean rightTicking) {
        Assert.eq(types.length, "types.length", cardinality.length, "cardinality.length");

        long nextLeftRow = 0;
        final ArrayList<String> leftKeys = new ArrayList<>();
        final ArrayList<Long> leftData = new ArrayList<>();

        long nextRightRow = 0;
        final ArrayList<String> rightKeys = new ArrayList<>();
        final ArrayList<Long> rightData = new ArrayList<>();

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

        final QueryTable left;
        if (leftTicking) {
            left = TstUtils.testRefreshingTable(RowSetFactory.flat(nextLeftRow).toTracking(),
                    c("sharedKey", leftKeys.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY)),
                    c("leftData", leftData.toArray(new Long[] {})));
        } else {
            left = TstUtils.testTable(RowSetFactory.flat(nextLeftRow).toTracking(),
                    c("sharedKey", leftKeys.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY)),
                    c("leftData", leftData.toArray(new Long[] {})));
        }
        final QueryTable right;
        if (rightTicking) {
            right = TstUtils.testRefreshingTable(RowSetFactory.flat(nextRightRow).toTracking(),
                    c("sharedKey", rightKeys.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY)),
                    c("rightData", rightData.toArray(new Long[] {})));
        } else {
            right = TstUtils.testTable(RowSetFactory.flat(nextRightRow).toTracking(),
                    c("sharedKey", rightKeys.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY)),
                    c("rightData", rightData.toArray(new Long[] {})));
        }

        final Table chunkedCrossJoin = left.join(right, "sharedKey", numRightBitsToReserve);
        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Left Table (" + left.size() + " rows): ");
            TableTools.showWithRowSet(left, 100);
            System.out.println("\nRight Table (" + right.size() + " rows): ");
            TableTools.showWithRowSet(right, 100);
            System.out.println("\nCross Join Table (" + chunkedCrossJoin.size() + " rows): ");
            TableTools.showWithRowSet(chunkedCrossJoin, 100);
        }

        QueryTable.USE_CHUNKED_CROSS_JOIN = false;
        final Table nonChunkedCrossJoin = left.join(right, "sharedKey", numRightBitsToReserve);
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
        final Table z = x.join(y, "Col1=Col2");
        final Table z2 = x.naturalJoin(y, "Col1=Col2");
        final Table z3 = z2.where("!isNull(Col2)");

        assertTableEquals(z3, z);
    }

    public void testStaticVsNaturalJoin2() {
        final int size = 10000;

        final QueryTable xqt = new QueryTable(RowSetFactory.flat(size).toTracking(),
                Collections.emptyMap());
        xqt.setRefreshing(true);
        final QueryTable yqt = new QueryTable(RowSetFactory.flat(size).toTracking(),
                Collections.emptyMap());
        yqt.setRefreshing(true);

        final Table x = xqt.update("Col1=i");
        final Table y = yqt.update("Col2=i*2");
        final Table z = x.join(y, "Col1=Col2");
        final Table z2 = x.naturalJoin(y, "Col1=Col2");
        final Table z3 = z2.where("!isNull(Col2)");

        assertTableEquals(z3, z);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            xqt.getRowSet().writableCast().insertRange(size, size * 2);
            xqt.notifyListeners(RowSetFactory.fromRange(size, size * 2), i(), i());
        });

        assertTableEquals(z3, z);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
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
        final int maxSteps = numSteps.intValue();
        final Random random = new Random(seed);

        // Note: make our join helper think this left table might tick
        final QueryTable leftNotTicking = getTable(1000, random, getIncrementalColumnInfo("lt", numGroups));

        final TstUtils.ColumnInfo<?, ?>[] leftColumns = getIncrementalColumnInfo("lt", numGroups);
        final QueryTable leftTicking = getTable(0, random, leftColumns);

        final TstUtils.ColumnInfo<?, ?>[] leftShiftingColumns = getIncrementalColumnInfo("lt", numGroups);
        final QueryTable leftShifting = getTable(1000, random, leftShiftingColumns);

        final TstUtils.ColumnInfo<?, ?>[] rightColumns = getIncrementalColumnInfo("rt", numGroups);
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

        final GenerateTableUpdates.SimulationProfile shiftingProfile = new GenerateTableUpdates.SimulationProfile();
        shiftingProfile.SHIFT_10_PERCENT_POS_SPACE = 5;
        shiftingProfile.SHIFT_10_PERCENT_KEY_SPACE = 5;
        shiftingProfile.SHIFT_AGGRESSIVELY = 85;

        for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
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

            TstUtils.validate(ctxt + " step == " + numSteps.getValue(), en);
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
        final int maxSteps = numSteps.intValue();
        final Random random = new Random(seed);

        final int numGroups = (int) Math.max(4, Math.ceil(Math.sqrt(initialSize)));
        final TstUtils.ColumnInfo<?, ?>[] leftColumns = getIncrementalColumnInfo("lt", numGroups);
        final QueryTable leftTicking = getTable(initialSize, random, leftColumns);

        final TstUtils.ColumnInfo<?, ?>[] rightColumns = getIncrementalColumnInfo("rt", numGroups);
        final QueryTable rightTicking = getTable(initialSize, random, rightColumns);

        final QueryTable leftStatic = getTable(false, initialSize, random, getIncrementalColumnInfo("ls", numGroups));
        final QueryTable rightStatic = getTable(false, initialSize, random, getIncrementalColumnInfo("rs", numGroups));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> leftTicking.join(rightTicking, "ltSym=rtSym", numRightBitsToReserve)),
                EvalNugget.from(() -> leftStatic.join(rightTicking, "lsSym=rtSym", numRightBitsToReserve)),
                EvalNugget.from(() -> leftTicking.join(rightStatic, "ltSym=rsSym", numRightBitsToReserve)),
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

        for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
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

            TstUtils.validate(ctxt + " step == " + numSteps.getValue(), en);
        }
    }

    public void testColumnSourceCanReuseContextWithSmallerRowSequence() {
        final QueryTable t1 = testRefreshingTable(i(0, 1).toTracking());
        final QueryTable t2 = (QueryTable) t1.update("K=k", "A=1");
        final QueryTable t3 = (QueryTable) testTable(i(2, 3).toTracking()).update("I=i", "A=1");
        final QueryTable jt = (QueryTable) t2.join(t3, "A", numRightBitsToReserve);

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

        for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
            final long rightOffset = numSteps.getValue();

            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                addToTable(leftTicking, i(numSteps.getValue()), longCol("intCol", numSteps.getValue()));
                TableUpdateImpl up = new TableUpdateImpl();
                up.shifted = RowSetShiftData.EMPTY;
                up.added = i(numSteps.getValue());
                up.removed = i();
                up.modified = i();
                up.modifiedColumnSet = ModifiedColumnSet.ALL;
                leftTicking.notifyListeners(up);

                final long[] data = new long[numSteps.getValue() + 1];
                for (int i = 0; i <= numSteps.getValue(); ++i) {
                    data[i] = i;
                }
                addToTable(rightTicking, RowSetFactory.fromRange(rightOffset, rightOffset + numSteps.getValue()),
                        longCol("intCol", data));
                TstUtils.removeRows(rightTicking, i(rightOffset - 1));

                up = new TableUpdateImpl();
                final RowSetShiftData.Builder shifted = new RowSetShiftData.Builder();
                shifted.shiftRange(0, numSteps.getValue() + rightOffset, 1);
                up.shifted = shifted.build();
                up.added = i(rightOffset + numSteps.getValue());
                up.removed = i();
                if (numSteps.getValue() == 0) {
                    up.modified = RowSetFactory.empty();
                } else {
                    up.modified = RowSetFactory.fromRange(rightOffset, rightOffset + numSteps.getValue() - 1);
                }
                up.modifiedColumnSet = ModifiedColumnSet.ALL;
                rightTicking.notifyListeners(up);
            });

            TstUtils.validate(" step == " + numSteps.getValue(), en);
        }
    }
}
