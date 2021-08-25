package io.deephaven.db.v2;

import io.deephaven.base.FileUtils;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.MatchPairFactory;
import io.deephaven.db.tables.utils.*;
import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.ColumnHolder;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.function.BiConsumer;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.tables.utils.TableTools.*;
import static io.deephaven.db.v2.TstUtils.*;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static java.util.Arrays.asList;

@Category(OutOfBandTest.class)
public class QueryTableNaturalJoinTest extends QueryTableTestBase {

    public void testNaturalJoinRehash() {
        setExpectError(false);

        final Random random = new Random(0);

        final String[] leftJoinKey = new String[1024];
        final int[] leftSentinel = new int[1024];
        final String[] rightJoinKey = new String[1024];
        final int[] rightSentinel = new int[1024];

        int offset = 0;
        fillRehashKeys(offset, leftJoinKey, leftSentinel, rightJoinKey, rightSentinel);

        final QueryTable leftTable = TstUtils.testRefreshingTable(stringCol("JoinKey", leftJoinKey),
            intCol("LeftSentinel", leftSentinel));
        final QueryTable rightTable = TstUtils.testRefreshingTable(
            stringCol("JoinKey", rightJoinKey), intCol("RightSentinel", rightSentinel));

        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return leftTable.naturalJoin(rightTable, "JoinKey",
                            "RJK=JoinKey,RightSentinel");
                    }
                },
        };

        if (printTableUpdates) {
            for (int ii = 0; ii < en.length; ++ii) {
                en[ii].showResult("Original " + ii, en[ii].originalValue);
            }
        }


        for (int step = 0; step < 40; step++) {
            System.out.println("Step = " + step + ", leftSize=" + leftTable.size() + ", rightSize="
                + rightTable.size());

            offset += leftJoinKey.length;
            fillRehashKeys(offset, leftJoinKey, leftSentinel, rightJoinKey, rightSentinel);

            final int foffset = offset;
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                final Index addIndex =
                    Index.FACTORY.getIndexByRange(foffset, foffset + leftJoinKey.length - 1);
                addToTable(leftTable, addIndex, stringCol("JoinKey", leftJoinKey),
                    intCol("LeftSentinel", leftSentinel));
                leftTable.notifyListeners(addIndex, i(), i());


                final Index.SequentialBuilder modIndexBuilder =
                    Index.FACTORY.getSequentialBuilder();

                int slot = random.nextInt(foffset / 100);
                for (int ii = 0; ii < 100; ++ii) {
                    modIndexBuilder.appendKey(slot);
                    slot += 1 + random.nextInt(foffset / 100);
                    if (slot >= foffset) {
                        break;
                    }
                }

                final Index modIndex = modIndexBuilder.getIndex();
                final String[] rightModifications = new String[modIndex.intSize()];
                final int[] rightModifySentinel = new int[modIndex.intSize()];

                final MutableInt position = new MutableInt();
                modIndex.forAllLongs((long ll) -> {
                    final int ii = (int) ll;
                    if (ii % 2 == 0) {
                        // make something that exists go away
                        rightModifications[position.intValue()] = Integer.toString(ii * 10 + 2);
                    } else {
                        // make something that did not exist come back
                        rightModifications[position.intValue()] = Integer.toString(ii * 10);
                    }
                    rightModifySentinel[position.intValue()] = ii * 100 + 25;
                    position.increment();
                });

                addToTable(rightTable, addIndex, stringCol("JoinKey", rightJoinKey),
                    intCol("RightSentinel", rightSentinel));
                addToTable(rightTable, modIndex, stringCol("JoinKey", rightModifications),
                    intCol("RightSentinel", rightModifySentinel));
                rightTable.notifyListeners(addIndex, i(), modIndex);
            });
            TstUtils.validate(en);
        }
    }

    private void fillRehashKeys(int offset, String[] leftJoinKey, int[] leftSentinel,
        String[] rightJoinKey, int[] rightSentinel) {
        for (int ii = 0; ii < leftJoinKey.length; ii++) {
            final int iio = ii + offset;
            leftJoinKey[ii] = Integer.toString(iio * 10);
            leftSentinel[ii] = iio * 100;

            if (iio % 2 == 0) {
                rightJoinKey[ii] = Integer.toString(iio * 10);
            } else {
                rightJoinKey[ii] = Integer.toString((iio / 4 * 10) + 1);
            }
            rightSentinel[ii] = iio * 100 + 25;
        }
    }

    public void testNaturalJoinIncremental() {
        setExpectError(false);

        final int sz = 5;
        final int maxSteps = 10;
        for (JoinIncrement joinIncrement : joinIncrementorsShift) {
            testNaturalJoinIncremental(false, false, sz, sz, joinIncrement, 0, maxSteps);
        }

        final int[] leftSizes = new int[] {10, 50, 100};
        final int[] rightSizes = new int[] {10, 50, 100};
        for (long seed = 0; seed < 5; seed++) {
            for (int leftSize : leftSizes) {
                for (int rightSize : rightSizes) {
                    for (JoinIncrement joinIncrement : joinIncrementorsShift) {
                        testNaturalJoinIncremental(false, false, leftSize, rightSize, joinIncrement,
                            seed, maxSteps);
                    }
                }
            }
        }
    }

    public void testNaturalJoinIncrementalOverflow() {
        setExpectError(false);

        final int maxSteps = 5;
        final int[] leftSizes = new int[] {10000};
        final int[] rightSizes = new int[] {10000};
        for (long seed = 0; seed < 5; seed++) {
            for (int leftSize : leftSizes) {
                for (int rightSize : rightSizes) {
                    for (JoinIncrement joinIncrement : new JoinIncrement[] {
                            leftRightConcurrentStepShift}) {
                        testNaturalJoinIncremental(false, false, leftSize, rightSize, joinIncrement,
                            seed, maxSteps, QueryTableJoinTest.HIGH_LOAD_FACTOR_CONTROL);
                    }
                }
            }
        }
    }

    public void testNaturalJoinLeftIncrementalRightStatic() {
        for (JoinIncrement joinIncrement : new JoinIncrement[] {leftStepShift, leftStep}) {
            final int sz = 5;
            final int maxSteps = 20;
            testNaturalJoinIncremental(false, true, sz, sz, joinIncrement, 0, maxSteps);

            final int[] leftSizes = new int[] {50, 100};
            final int[] rightSizes = new int[] {50, 100};
            for (long seed = 0; seed < 1; seed++) {
                for (int leftSize : leftSizes) {
                    for (int rightSize : rightSizes) {
                        testNaturalJoinIncremental(false, true, leftSize, rightSize, joinIncrement,
                            seed, maxSteps);
                    }
                }
            }
        }
    }

    public void testNaturalJoinLeftStaticRightIncremental() {
        for (JoinIncrement joinIncrement : new JoinIncrement[] {rightStepShift, rightStep}) {
            final int sz = 5;
            final int maxSteps = 20;
            testNaturalJoinIncremental(true, false, sz, sz, joinIncrement, 0, maxSteps);

            final int[] leftSizes = new int[] {50, 100};
            final int[] rightSizes = new int[] {50, 100};
            for (long seed = 0; seed < 5; seed++) {
                for (int leftSize : leftSizes) {
                    for (int rightSize : rightSizes) {
                        testNaturalJoinIncremental(true, false, leftSize, rightSize, joinIncrement,
                            seed, maxSteps);
                    }
                }
            }
        }
    }

    private void testNaturalJoinIncremental(boolean leftStatic, boolean rightStatic, int leftSize,
        int rightSize, JoinIncrement joinIncrement, long seed, long maxSteps) {
        testNaturalJoinIncremental(leftStatic, rightStatic, leftSize, rightSize, joinIncrement,
            seed, new MutableInt((int) maxSteps));
    }

    private void testNaturalJoinIncremental(boolean leftStatic, boolean rightStatic, int leftSize,
        int rightSize, JoinIncrement joinIncrement, long seed, MutableInt numSteps) {
        testNaturalJoinIncremental(leftStatic, rightStatic, leftSize, rightSize, joinIncrement,
            seed, numSteps, new JoinControl());
    }

    private static void testNaturalJoinIncremental(boolean leftStatic, boolean rightStatic,
        int leftSize, int rightSize, JoinIncrement joinIncrement, long seed, long maxSteps,
        JoinControl control) {
        testNaturalJoinIncremental(leftStatic, rightStatic, leftSize, rightSize, joinIncrement,
            seed, new MutableInt((int) maxSteps), control);
    }

    private static void testNaturalJoinIncremental(boolean leftStatic, boolean rightStatic,
        int leftSize, int rightSize, JoinIncrement joinIncrement, long seed, MutableInt numSteps,
        JoinControl control) {
        final Random random = new Random(seed);
        final int maxSteps = numSteps.intValue();
        final Logger log = new StreamLoggerImpl();

        final TstUtils.ColumnInfo[] rightColumnInfo;
        final TstUtils.UniqueIntGenerator rightIntGenerator =
            new TstUtils.UniqueIntGenerator(1, rightSize * (rightStatic ? 2 : 4));
        final TstUtils.UniqueIntGenerator rightInt2Generator =
            new TstUtils.UniqueIntGenerator(1, rightSize * (rightStatic ? 2 : 4));

        final TstUtils.IntGenerator duplicateGenerator = new TstUtils.IntGenerator(100000, 100010);

        final List<TstUtils.Generator<Integer, Integer>> generatorList =
            Arrays.asList(rightIntGenerator, duplicateGenerator);
        final TstUtils.Generator<Integer, Integer> compositeGenerator =
            new TstUtils.CompositeGenerator<>(generatorList, 0.9);

        final QueryTable rightTable = getTable(!rightStatic, rightSize, random,
            rightColumnInfo = initColumnInfos(new String[] {"I1", "C1", "C2"},
                compositeGenerator,
                new SetGenerator<>("a", "b"),
                rightInt2Generator));

        final ColumnInfo[] leftColumnInfo;
        final QueryTable leftTable = getTable(!leftStatic, leftSize, random,
            leftColumnInfo = initColumnInfos(new String[] {"I1", "C1", "C2"},
                new FromUniqueIntGenerator(rightIntGenerator, new IntGenerator(20, 10000), 0.75),
                new SetGenerator<>("a", "b", "c"),
                new FromUniqueIntGenerator(rightInt2Generator, new IntGenerator(20, 10000), 0.75)));

        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return NaturalJoinHelper.naturalJoin(leftTable, rightTable,
                            MatchPairFactory.getExpressions("I1"),
                            MatchPairFactory.getExpressions("LI1=I1", "LC1=C1", "LC2=C2"), false,
                            control);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return NaturalJoinHelper.naturalJoin(leftTable, rightTable,
                            MatchPairFactory.getExpressions("C1", "I1"),
                            MatchPairFactory.getExpressions("LC2=C2"), false, control);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return NaturalJoinHelper.naturalJoin(leftTable,
                            (QueryTable) rightTable.update("Exists=true"),
                            MatchPairFactory.getExpressions("C1", "C2", "I1"),
                            MatchPairFactory.getExpressions("Exists"), false, control);
                    }
                },
        };

        if (printTableUpdates) {
            for (int ii = 0; ii < en.length; ++ii) {
                en[ii].showResult("Original " + ii, en[ii].originalValue);
            }
        }

        final int leftStepSize = (int) Math.ceil(Math.sqrt(leftSize));
        final int rightStepSize = (int) Math.ceil(Math.sqrt(rightSize));

        for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
            if (printTableUpdates) {
                System.out.println(
                    "Step = " + numSteps.intValue() + ", leftSize=" + leftSize + ", rightSize="
                        + rightSize + ", seed = " + seed + ", joinIncrement=" + joinIncrement);
                System.out.println("Left Table:" + leftTable.size());
                showWithIndex(leftTable, 100);
                System.out.println("Right Table:" + rightTable.size());
                showWithIndex(rightTable, 100);
            }
            joinIncrement.step(leftStepSize, rightStepSize, leftTable, rightTable, leftColumnInfo,
                rightColumnInfo, en, random);
        }
    }

    public void testNaturalJoinMixedGrouping() {
        testNaturalJoinMixedGroupingLeftStatic(10, 10, 0, 20);
        testNaturalJoinMixedGroupingLeftStatic(1000, 1000, 1, 10);
        testNaturalJoinMixedGroupingLeftStatic(10000, 10000, 1, 10);
    }

    private void testNaturalJoinMixedGroupingLeftStatic(int leftSize, int rightSize, long seed,
        int steps) {
        final Random random = new Random(seed);

        final QueryTable leftTable = getTable(false, leftSize, random,
            initColumnInfos(new String[] {"I1", "C1", "C2"},
                new ColumnInfo.ColAttributes[] {ColumnInfo.ColAttributes.Grouped},
                new IntGenerator(1, rightSize * 10),
                new SetGenerator<>("a", "b", "c", "d", "e", "f"),
                new IntGenerator(1, 10)));
        final ColumnInfo[] rightColumnInfos = initColumnInfos(new String[] {"I1", "C1", "C2"},
            new ColumnInfo.ColAttributes[] {},
            new UniqueIntGenerator(1, rightSize * 10),
            new SetGenerator<>("a", "b", "c", "d", "e"),
            new IntGenerator(1, 10));
        final QueryTable rightTable = getTable(true, rightSize, random, rightColumnInfos);

        System.out.println("Left:");
        TableTools.showWithIndex(leftTable);
        System.out.println("Right:");
        TableTools.showWithIndex(rightTable);

        final Table result = leftTable.naturalJoin(rightTable, "I1", "LC1=C1,LC2=C2");

        System.out.println("Result:");
        TableTools.showWithIndex(result);

        final Table ungroupedResult = leftTable.update("I1=I1*10")
            .naturalJoin(rightTable.update("I1=I1*10"), "I1", "LC1=C1,LC2=C2")
            .update("I1=(int)(I1/10)");

        System.out.println("Ungrouped Result:");
        TableTools.showWithIndex(ungroupedResult);

        assertTableEquals(ungroupedResult, result);

        final Table leftFlat = leftTable.flatten();
        final ColumnSource flatGrouped = leftFlat.getColumnSource("I1");
        final Index flatIndex = leftFlat.getIndex();
        final Map<Object, Index> grouping = flatIndex.getGrouping(flatGrouped);
        // noinspection unchecked
        ((AbstractColumnSource) flatGrouped).setGroupToRange(grouping);

        final Table resultFlat = leftFlat.naturalJoin(rightTable, "I1", "LC1=C1,LC2=C2");
        assertTableEquals(ungroupedResult, resultFlat);

        for (int step = 0; step < steps; ++step) {
            if (LiveTableTestCase.printTableUpdates) {
                System.out.println("Step = " + step);
            }

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, rightSize, random, rightTable,
                    rightColumnInfos);
            });

            if (LiveTableTestCase.printTableUpdates) {
                System.out.println("Expected");
                TableTools.showWithIndex(result);
                System.out.println("Result");
                TableTools.showWithIndex(result);
            }

            assertTableEquals(ungroupedResult, result);
            assertTableEquals(ungroupedResult, resultFlat);
        }
    }

    public void testNaturalJoinSimpleStatic() {
        // noinspection unchecked
        testNaturalJoinSimpleStatic(TstUtils::c);
    }

    public void testNaturalJoinGroupedStatic() {
        // noinspection unchecked
        testNaturalJoinSimpleStatic(TstUtils::cG);
    }

    private interface MakeLeftColumn {
        @SuppressWarnings("unchecked")
        <T> ColumnHolder make(String name, T... data);
    }

    private void testNaturalJoinSimpleStatic(MakeLeftColumn lC) {
        final Table left = testTable(lC.make("Symbol", "A", "B", "C"), c("LeftSentinel", 1, 2, 3));
        final Table right =
            newTable(c("Symbol", "A", "B", "D", "E", "F"), c("RightSentinel", 10, 11, 12, 13, 14),
                c("RightObjectSentinel", 10, 11L, "12", "13", "14"));

        final Table cj = left.naturalJoin(right, "Symbol");
        TableTools.showWithIndex(cj);
        assertEquals(new int[] {10, 11, NULL_INT}, intColumn(cj, "RightSentinel"));
        // the two wheres check for filling null keys
        final Table cjw = cj.where("RightObjectSentinel = null");
        final Table cjw2 = left.naturalJoin(SparseSelect.sparseSelect(right), "Symbol")
            .where("RightObjectSentinel = null");
        TableTools.showWithIndex(cjw);
        TableTools.showWithIndex(cjw2);

        final Table left2 =
            newTable(lC.make("Symbol", "A", "B", "C", "A"), c("LeftSentinel", 1, 2, 3, 4));
        final Table right2 = newTable(c("Symbol", "A", "B", "D"), c("RightSentinel", 10, 11, 12));

        final Table cj2 = left2.naturalJoin(right2, "Symbol");
        TableTools.showWithIndex(cj2);
        assertEquals(new int[] {10, 11, NULL_INT, 10}, intColumn(cj2, "RightSentinel"));

        final int collision = 16384;
        final Table left3 =
            newTable(lC.make("Int", 10, collision + 10, collision * 2 + 10, collision * 3 + 10),
                c("LeftSentinel", 1, 3, 3, 4));
        final Table right3 = newTable(c("Int", 10, collision + 10, collision * 4 + 10),
            c("RightSentinel", 10, 11, 13));

        TableTools.show(left3);
        TableTools.show(right3);

        final Table cj3 = left3.naturalJoin(right3, "Int");
        TableTools.showWithIndex(cj3);
        assertEquals(new int[] {10, 11, NULL_INT, NULL_INT}, intColumn(cj3, "RightSentinel"));

        final Table left4 = newTable(
            lC.make("String", "c", "e", "g"),
            c("LeftSentinel", 1, 2, 3));
        final Table right4 = newTable(c("String", "c", "e"), c("RightSentinel", 10, 11));
        final Table cj4 = left4.naturalJoin(right4, "String");
        TableTools.showWithIndex(cj4);
        assertEquals(new int[] {10, 11, NULL_INT}, intColumn(cj4, "RightSentinel"));


        final Table left5 = newTable(
            lC.make("String", "c", "e", "g"),
            c("LeftSentinel", 1, 2, 3));
        final Table right5 = newTable(c("RightSentinel", 10));
        final Table cj5 = left5.naturalJoin(right5, "");
        TableTools.showWithIndex(cj5);
        assertEquals(new int[] {10, 10, 10}, intColumn(cj5, "RightSentinel"));

        final Table left6 = newTable(
            lC.make("String", "c", "e", "g"),
            c("LeftSentinel", 1, 2, 3));
        final Table right6 = newTable(intCol("RightSentinel"));
        final Table cj6 = left6.naturalJoin(right6, "");
        TableTools.showWithIndex(cj6);
        assertEquals(new int[] {NULL_INT, NULL_INT, NULL_INT}, intColumn(cj6, "RightSentinel"));

        final Table left7 = newTable(
            lC.make("String", CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
            intCol("LeftSentinel"));
        final Table right7 = newTable(intCol("RightSentinel", 10, 11));
        final Table cj7 = left7.naturalJoin(right7, "");
        TableTools.showWithIndex(cj7);
        assertEquals(0, cj7.size());

        // inactive right hand side state, build using the left
        final Table left8 = newTable(lC.make("Symbol", "A", "B", "C"), c("LeftSentinel", 1, 2, 3));
        final Table right8 = newTable(c("Symbol", "A", "B", "D", "D", "E", "E", "D"),
            c("RightSentinel", 10, 11, 12, 13, 14, 15, 16));
        final Table cj8 = left8.naturalJoin(right8, "Symbol");
        TableTools.showWithIndex(cj8);
        assertEquals(new int[] {10, 11, NULL_INT}, intColumn(cj8, "RightSentinel"));

        // inactive right hand side state, build using the right
        final Table left9 = newTable(lC.make("Symbol", "A", "B", "C", "A", "B", "C"),
            c("LeftSentinel", 1, 2, 3, 4, 5, 6));
        final Table right9 = newTable(c("Symbol", "A", "D", "D"), c("RightSentinel", 10, 11, 12));
        final Table cj9 = left9.naturalJoin(right9, "Symbol");
        TableTools.showWithIndex(cj9);
        assertEquals(new int[] {10, NULL_INT, NULL_INT, 10, NULL_INT, NULL_INT},
            intColumn(cj9, "RightSentinel"));
    }

    public void testNaturalJoinDuplicateRights() {
        // build from right
        final Table left =
            testTable(c("Symbol", "A", "B", "C", "D"), c("LeftSentinel", 1, 2, 3, 4));
        final Table right = newTable(c("Symbol", "A", "A"), c("RightSentinel", 10, 11));
        try {
            final Table cj = left.naturalJoin(right, "Symbol");
            TableTools.showWithIndex(cj);
            fail("Expected exception.");
        } catch (IllegalStateException e) {
            assertEquals("More than one right side mapping for A", e.getMessage());
        }

        // build from left
        final Table left2 = testTable(c("Symbol", "A", "B"), c("LeftSentinel", 1, 2));
        final Table right2 =
            newTable(c("Symbol", "A", "A", "B", "C", "D"), c("RightSentinel", 10, 11, 12, 13, 14));
        try {
            final Table cj2 = left2.naturalJoin(right2, "Symbol");
            TableTools.showWithIndex(cj2);
            fail("Expected exception");
        } catch (IllegalStateException e) {
            assertEquals("More than one right side mapping for A", e.getMessage());
        }
    }

    public void testNaturalJoinDuplicateRightsRefreshingRight() {
        // initial case
        final Table left = testTable(c("Symbol", "A", "B"), c("LeftSentinel", 1, 2));
        final Table right = testRefreshingTable(c("Symbol", "A", "A"), c("RightSentinel", 10, 11));

        try {
            final Table cj = left.naturalJoin(right, "Symbol");
            TableTools.showWithIndex(cj);
            fail("Expected exception.");
        } catch (IllegalStateException rte) {
            assertEquals("Duplicate right key for A", rte.getMessage());
        }

        // bad right key added
        final QueryTable right2 = testRefreshingTable(c("Symbol", "A"), c("RightSentinel", 10));
        final Table cj2 = left.naturalJoin(right2, "Symbol");
        assertTableEquals(newTable(col("Symbol", "A", "B"), intCol("LeftSentinel", 1, 2),
            intCol("RightSentinel", 10, NULL_INT)), cj2);

        final ErrorListener listener = new ErrorListener((DynamicTable) cj2);
        ((DynamicTable) cj2).listenForUpdates(listener);

        try (final ErrorExpectation ignored = new ErrorExpectation()) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(right2, i(3), c("Symbol", "A"), intCol("RightSentinel", 10));
                right2.notifyListeners(i(3), i(), i());
            });
        }

        assertNotNull(listener.originalException);
        assertEquals("Duplicate right key for A", listener.originalException.getMessage());
    }

    public void testNaturalJoinDuplicateRightsRefreshingBoth() {
        // build from right
        final Table left = testRefreshingTable(c("Symbol", "A", "B"), c("LeftSentinel", 1, 2));
        final Table right = testRefreshingTable(c("Symbol", "A", "A"), c("RightSentinel", 10, 11));

        try {
            final Table cj = left.naturalJoin(right, "Symbol");
            TableTools.showWithIndex(cj);
            fail("Expected exception.");
        } catch (IllegalStateException rte) {
            assertEquals("Duplicate right key for A", rte.getMessage());
        }

        // bad right key added
        final QueryTable right2 = testRefreshingTable(c("Symbol", "A"), c("RightSentinel", 10));
        final Table cj2 = left.naturalJoin(right2, "Symbol");
        assertTableEquals(newTable(col("Symbol", "A", "B"), intCol("LeftSentinel", 1, 2),
            intCol("RightSentinel", 10, NULL_INT)), cj2);

        final ErrorListener listener = new ErrorListener((DynamicTable) cj2);
        ((DynamicTable) cj2).listenForUpdates(listener);

        try (final ErrorExpectation ignored = new ErrorExpectation()) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(right2, i(3), c("Symbol", "A"), intCol("RightSentinel", 10));
                right2.notifyListeners(i(3), i(), i());
            });
        }

        assertNotNull(listener.originalException);
        assertEquals("Duplicate right key for A", listener.originalException.getMessage());
    }


    public void testNaturalJoinReinterprets() {
        final Table left =
            testTable(c("JBool", true, false, null, true), c("LeftSentinel", 1, 2, 3, 4));
        final Table right = newTable(c("JBool", true, false, null), c("RightSentinel", 10, 11, 12));
        final Table cj = left.naturalJoin(right, "JBool");
        TableTools.showWithIndex(cj);
        assertEquals(new int[] {10, 11, 12, 10}, intColumn(cj, "RightSentinel"));

        final DBDateTime time1 = DBTimeUtils.convertDateTime("2019-05-10T09:45:00 NY");
        final DBDateTime time2 = DBTimeUtils.convertDateTime("2019-05-10T21:45:00 NY");

        final Table left2 =
            testTable(c("JDate", time1, time2, null, time2), c("LeftSentinel", 1, 2, 3, 4));
        final Table right2 =
            newTable(c("JDate", time2, time1, null), c("RightSentinel", 10, 11, 12));
        final Table cj2 = left2.naturalJoin(right2, "JDate");
        TableTools.showWithIndex(cj2);
        assertEquals(new int[] {11, 10, 12, 10}, intColumn(cj2, "RightSentinel"));
    }

    public void testNaturalJoinFloats() {
        final Table left =
            testTable(floatCol("JF", 1.0f, 2.0f, Float.NaN, 3.0f), c("LeftSentinel", 1, 2, 3, 4));
        final Table right =
            newTable(floatCol("JF", Float.NaN, 1.0f, 2.0f), c("RightSentinel", 10, 11, 12));
        final Table cj = left.naturalJoin(right, "JF");
        TableTools.showWithIndex(cj);
        assertEquals(new int[] {11, 12, 10, NULL_INT}, intColumn(cj, "RightSentinel"));

        final Table left2 = testTable(
            doubleCol("JD", 10.0, 20.0, Double.NaN, io.deephaven.util.QueryConstants.NULL_DOUBLE),
            c("LeftSentinel", 1, 2, 3, 4));
        final Table right2 = newTable(doubleCol("JD", QueryConstants.NULL_DOUBLE, Double.NaN, 10.0),
            c("RightSentinel", 10, 11, 12));
        final Table cj2 = left2.naturalJoin(right2, "JD");
        TableTools.showWithIndex(cj2);
        assertEquals(new int[] {12, NULL_INT, 11, 10}, intColumn(cj2, "RightSentinel"));
    }


    public void testNaturalJoinZeroKeys() {
        setExpectError(false);

        final QueryTable c0 = TstUtils.testRefreshingTable(intCol("Left", 1, 2, 3));
        final QueryTable c1 = TstUtils.testRefreshingTable(intCol("Right"));

        final Table cj = c0.naturalJoin(c1, "");

        final DynamicTable emptyRightResult =
            newTable(intCol("Left", 1, 2, 3), intCol("Right", NULL_INT, NULL_INT, NULL_INT));
        assertTableEquals(emptyRightResult, cj);

        TableTools.showWithIndex(cj);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(c1, i(1), intCol("Right", 4));
            c1.notifyListeners(i(1), i(), i());
        });

        TableTools.showWithIndex(cj);

        final DynamicTable fourRightResult =
            newTable(intCol("Left", 1, 2, 3), intCol("Right", 4, 4, 4));
        assertTableEquals(fourRightResult, cj);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            removeRows(c1, i(1));
            c1.notifyListeners(i(), i(1), i());
        });

        TableTools.showWithIndex(cj);

        assertTableEquals(emptyRightResult, cj);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(c0, i(6), intCol("Left", 6));
            addToTable(c1, i(2), intCol("Right", 5));
            c0.notifyListeners(i(6), i(), i());
            c1.notifyListeners(i(2), i(), i());
        });

        TableTools.showWithIndex(cj);

        final DynamicTable fiveResult =
            newTable(intCol("Left", 1, 2, 3, 6), intCol("Right", 5, 5, 5, 5));
        assertTableEquals(fiveResult, cj);

    }

    public void testNaturalJoinZeroKeysStaticRight() {
        setExpectError(false);

        final QueryTable c0 = TstUtils.testRefreshingTable(intCol("Left", 1, 2, 3));
        final Table c1 = newTable(intCol("Right"));
        final Table c2 = newTable(intCol("Right", 4));

        final Table cj1 = c0.naturalJoin(c1, "");
        assertTableEquals(
            newTable(intCol("Left", 1, 2, 3), intCol("Right", NULL_INT, NULL_INT, NULL_INT)), cj1);
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(c0, i(6), intCol("Left", 6));
            c0.notifyListeners(i(6), i(), i());
        });

        TableTools.showWithIndex(cj1);
        assertTableEquals(newTable(intCol("Left", 1, 2, 3, 6),
            intCol("Right", NULL_INT, NULL_INT, NULL_INT, NULL_INT)), cj1);

        final Table cj2 = c0.naturalJoin(c2, "");
        assertTableEquals(newTable(intCol("Left", 1, 2, 3, 6), intCol("Right", 4, 4, 4, 4)), cj2);
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(c0, i(7), intCol("Left", 7));
            c0.notifyListeners(i(7), i(), i());
        });

        TableTools.showWithIndex(cj1);
        assertTableEquals(newTable(intCol("Left", 1, 2, 3, 6, 7), intCol("Right", 4, 4, 4, 4, 4)),
            cj2);

    }

    public void testNaturalJoinZeroKeysStaticLeft() {
        setExpectError(false);

        final Table c0 = newTable(intCol("Left", 1, 2, 3));
        final QueryTable c1 = TstUtils.testRefreshingTable(intCol("Right"));

        final Table cj = c0.naturalJoin(c1, "");

        final DynamicTable emptyRightResult =
            newTable(intCol("Left", 1, 2, 3), intCol("Right", NULL_INT, NULL_INT, NULL_INT));
        assertTableEquals(emptyRightResult, cj);

        TableTools.showWithIndex(cj);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(c1, i(1), intCol("Right", 4));
            c1.notifyListeners(i(1), i(), i());
        });

        TableTools.showWithIndex(cj);

        final DynamicTable fourRightResult =
            newTable(intCol("Left", 1, 2, 3), intCol("Right", 4, 4, 4));
        assertTableEquals(fourRightResult, cj);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            removeRows(c1, i(1));
            c1.notifyListeners(i(), i(1), i());
        });

        TableTools.showWithIndex(cj);

        assertTableEquals(emptyRightResult, cj);

        TableTools.showWithIndex(cj);

        assertTableEquals(emptyRightResult, cj);

    }

    public void testNaturalJoin() {
        final Table c0 = TstUtils.testRefreshingTable(c("USym0", "A", "B"), intCol("X", 1, 2));
        final Table c1 = TstUtils.testRefreshingTable(c("USym1", "A", "D"), intCol("Y", 1, 2));

        Table cj = c0.naturalJoin(c1, "USym0=USym1", "Y");
        cj.select();

        cj = c0.naturalJoin(c1, "USym0=USym1", "USym1,Y");
        cj.select();


        final Table lTable = TstUtils.testRefreshingTable(
            c("String", "a", "b", "c"),
            intCol("Int", 1, 2, 3));
        final Table rTable = TstUtils.testRefreshingTable(
            c("String", "a", "b", "c"),
            intCol("Int", 10, 20, 30));
        final Table result = lTable.naturalJoin(rTable, "String", "Int2=Int");
        assertEquals(3, result.size());
        assertEquals(3, result.getColumns().length);
        assertEquals("String", result.getColumns()[0].getName());
        assertEquals("Int", result.getColumns()[1].getName());
        assertEquals("Int2", result.getColumns()[2].getName());
        assertEquals(Arrays.asList("a", "b", "c"),
            Arrays.asList(result.getColumn("String").get(0, 3)));
        assertEquals(Arrays.asList(1, 2, 3), Arrays.asList(result.getColumn("Int").get(0, 3)));
        assertEquals(Arrays.asList(10, 20, 30), Arrays.asList(result.getColumn("Int2").get(0, 3)));


        Table table1 = TstUtils.testRefreshingTable(
            c("String", "c", "e", "g"));

        Table table2 = TstUtils.testRefreshingTable(c("String", "c", "e"), c("v", 1, 2));
        Table pairMatch = table1.naturalJoin(table2, "String", "v");
        assertEquals(3, pairMatch.size());
        assertEquals(2, pairMatch.getColumns().length);
        assertEquals("String", pairMatch.getColumns()[0].getName());
        assertEquals("v", pairMatch.getColumns()[1].getName());
        assertEquals(String.class, pairMatch.getColumns()[0].getType());
        assertEquals(int.class, pairMatch.getColumns()[1].getType());
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumns()[0].getDirect()));
        assertEquals(asList(1, 2, null), asList(pairMatch.getColumn("v").get(0, 3)));


        table2 = TstUtils.testRefreshingTable(
            c("String", "c", "e", "g"), c("v", 1, 2, 3));

        pairMatch = table1.naturalJoin(table2, "String", "v");
        assertEquals(3, pairMatch.size());
        assertEquals(2, pairMatch.getColumns().length);
        assertEquals("String", pairMatch.getColumns()[0].getName());
        assertEquals("v", pairMatch.getColumns()[1].getName());
        assertEquals(String.class, pairMatch.getColumns()[0].getType());
        assertEquals(int.class, pairMatch.getColumns()[1].getType());
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumns()[0].getDirect()));
        assertEquals(asList(1, 2, 3), asList(pairMatch.getColumn("v").get(0, 3)));

        pairMatch = table2.naturalJoin(table1, "String", "");
        assertEquals(3, pairMatch.size());
        assertEquals(2, pairMatch.getColumns().length);
        assertEquals("String", pairMatch.getColumns()[0].getName());
        assertEquals("v", pairMatch.getColumns()[1].getName());
        assertEquals(String.class, pairMatch.getColumns()[0].getType());
        assertEquals(int.class, pairMatch.getColumns()[1].getType());
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumns()[0].getDirect()));
        assertEquals(asList(1, 2, 3), asList(pairMatch.getColumn("v").get(0, 3)));

        pairMatch = table1.naturalJoin(table2, "String=String", "v");
        assertEquals(3, pairMatch.size());
        assertEquals(2, pairMatch.getColumns().length);
        assertEquals("String", pairMatch.getColumns()[0].getName());
        assertEquals("v", pairMatch.getColumns()[1].getName());
        assertEquals(String.class, pairMatch.getColumns()[0].getType());
        assertEquals(int.class, pairMatch.getColumns()[1].getType());
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumns()[0].getDirect()));
        assertEquals(asList(1, 2, 3), asList(pairMatch.getColumn("v").get(0, 3)));

        pairMatch = table2.naturalJoin(table1, "String=String", "");

        assertEquals(3, pairMatch.size());
        assertEquals(2, pairMatch.getColumns().length);
        assertEquals("String", pairMatch.getColumns()[0].getName());
        assertEquals("v", pairMatch.getColumns()[1].getName());
        assertEquals(String.class, pairMatch.getColumns()[0].getType());
        assertEquals(int.class, pairMatch.getColumns()[1].getType());
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumns()[0].getDirect()));
        assertEquals(1, pairMatch.getColumn("v").getInt(0));
        assertEquals(2, pairMatch.getColumn("v").getInt(1));
        assertEquals(3, pairMatch.getColumn("v").getInt(2));


        table1 = TstUtils.testRefreshingTable(
            c("String1", "c", "e", "g"));

        table2 = TstUtils.testRefreshingTable(
            c("String2", "c", "e", "g"), c("v", 1, 2, 3));


        pairMatch = table1.naturalJoin(table2, "String1=String2", "String2,v");

        assertEquals(3, pairMatch.size());
        assertEquals(3, pairMatch.getColumns().length);
        assertEquals("String1", pairMatch.getColumns()[0].getName());
        assertEquals("String2", pairMatch.getColumns()[1].getName());
        assertEquals("v", pairMatch.getColumns()[2].getName());
        assertEquals(String.class, pairMatch.getColumns()[0].getType());
        assertEquals(String.class, pairMatch.getColumns()[1].getType());
        assertEquals(int.class, pairMatch.getColumns()[2].getType());
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumns()[0].getDirect()));
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumns()[1].getDirect()));
        assertEquals(asList(1, 2, 3), asList(pairMatch.getColumns()[2].get(0, 3)));


        pairMatch = table2.naturalJoin(table1, "String2=String1", "String1");

        assertEquals(3, pairMatch.size());
        assertEquals(3, pairMatch.getColumns().length);
        assertEquals("String2", pairMatch.getColumns()[0].getName());
        assertEquals("v", pairMatch.getColumns()[1].getName());
        assertEquals("String1", pairMatch.getColumns()[2].getName());
        assertEquals(String.class, pairMatch.getColumn("String1").getType());
        assertEquals(String.class, pairMatch.getColumn("String2").getType());
        assertEquals(int.class, pairMatch.getColumn("v").getType());
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumn("String1").getDirect()));
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumn("String2").getDirect()));
        assertEquals(asList(1, 2, 3), asList(pairMatch.getColumn("v").get(0, 3)));
    }

    public void testNaturalJoinNull() {
        final Table c0 = TstUtils.testRefreshingTable(c("USym0", "A", null), c("X", 1, 2));
        final Table c1 = TstUtils.testRefreshingTable(c("USym1", "A", null), c("Y", 3, 4));

        final Table cj = c0.naturalJoin(c1, "USym0=USym1", "Y");

        TableTools.show(cj);

        assertEquals(1, cj.getColumn("X").get(0));
        assertEquals(2, cj.getColumn("X").get(1));
        assertEquals(3, cj.getColumn("Y").get(0));
        assertEquals(4, cj.getColumn("Y").get(1));
    }

    public void testNaturalJoinInactive() {
        setExpectError(false);

        final QueryTable c0 = TstUtils.testRefreshingTable(c("USym0", "A", "C"), c("X", 1, 2));
        final QueryTable c1 =
            TstUtils.testRefreshingTable(c("USym1", "A", "B", "B"), c("Y", 3, 4, 5));

        final Table cj = c0.naturalJoin(c1, "USym0=USym1", "Y");

        System.out.println("Result:");
        TableTools.showWithIndex(cj);

        assertEquals(1, cj.getColumn("X").get(0));
        assertEquals(2, cj.getColumn("X").get(1));
        assertEquals(3, cj.getColumn("Y").get(0));
        assertNull(cj.getColumn("Y").get(1));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            removeRows(c1, i(2));
            c1.notifyListeners(i(), i(2), i());
        });
        System.out.println("Right:");
        TableTools.showWithIndex(c1);

        assertEquals(1, cj.getColumn("X").get(0));
        assertEquals(2, cj.getColumn("X").get(1));
        assertEquals(3, cj.getColumn("Y").get(0));
        assertNull(cj.getColumn("Y").get(1));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(c0, i(2), col("USym0", "B"), col("X", 6));
            c0.notifyListeners(i(2), i(), i());
        });

        System.out.println("Left:");
        TableTools.showWithIndex(c0);

        System.out.println("Result:");
        TableTools.showWithIndex(cj);

        assertEquals(1, cj.getColumn("X").get(0));
        assertEquals(2, cj.getColumn("X").get(1));
        assertEquals(6, cj.getColumn("X").get(2));
        assertEquals(3, cj.getColumn("Y").get(0));
        assertNull(cj.getColumn("Y").get(1));
        assertEquals(4, cj.getColumn("Y").get(2));
    }

    public void testNaturalJoinLeftIncrementalRightStaticSimple() {
        final QueryTable leftQueryTable = TstUtils.testRefreshingTable(i(1, 2, 4, 6),
            c("Sym", "aa", "bc", "aa", "aa"),
            c("ByteCol", (byte) 10, (byte) 20, (byte) 30, (byte) 50),
            c("DoubleCol", 0.1, 0.2, 0.3, 0.5));

        final QueryTable rightQueryTable = TstUtils.testTable(i(3, 6),
            c("RSym", "aa", "bc"),
            c("ByteCol", (byte) 10, (byte) 20),
            c("RDoubleCol", 1.1, 2.2));
        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable.naturalJoin(rightQueryTable, "ByteCol",
                            "RSym,RDoubleCol");
                    }
                }
        };
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(leftQueryTable, i(3, 9), c("Sym", "aa", "aa"),
                c("ByteCol", (byte) 20, (byte) 10), c("DoubleCol", 2.1, 2.2));
            System.out.println("Left Table Updated:");
            showWithIndex(leftQueryTable);
            leftQueryTable.notifyListeners(i(3, 9), i(), i());
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT
            .runWithinUnitTestCycle(() -> leftQueryTable.notifyListeners(i(), i(), i(1, 2, 4, 6)));
        TstUtils.validate(en);
    }

    public void testNaturalJoinIterative() {
        final QueryTable leftQueryTable = TstUtils.testRefreshingTable(i(1, 2, 4, 6),
            c("Sym", "aa", "bc", "aa", "aa"),
            c("intCol", 10, 20, 30, 50),
            c("doubleCol", 0.1, 0.2, 0.3, 0.5));

        final QueryTable rightQueryTable1 = TstUtils.testRefreshingTable(i(3, 6),
            c("Sym", "aa", "bc"),
            c("xCol", 11, 22),
            c("yCol", 1.1, 2.2));
        final QueryTable rightQueryTable2 = TstUtils.testRefreshingTable(i(10, 20, 30),
            c("Sym", "aa", "bc", "aa"),
            c("xCol", 11, 20, 20),
            c("yCol", 1.1, 2.2, 5.5));


        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable.naturalJoin(rightQueryTable1, "Sym", "xCol,yCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable.naturalJoin(rightQueryTable2, "Sym,intCol=xCol",
                            "xCol,yCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable.naturalJoin(rightQueryTable1, "Sym", "xCol,yCol")
                            .select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable
                            .naturalJoin(rightQueryTable2, "Sym,intCol=xCol", "xCol,yCol").select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable.naturalJoin(rightQueryTable1, "Sym", "xCol,yCol")
                            .update("q=xCol+yCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable
                            .naturalJoin(rightQueryTable2, "Sym,intCol=xCol", "xCol,yCol")
                            .update("q=xCol+yCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable.select().naturalJoin(rightQueryTable1, "Sym",
                            "xCol,yCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable.select().naturalJoin(rightQueryTable2,
                            "Sym,intCol=xCol", "xCol,yCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable.select()
                            .naturalJoin(rightQueryTable1, "Sym", "xCol,yCol")
                            .update("q=xCol+yCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable.select()
                            .naturalJoin(rightQueryTable2, "Sym,intCol=xCol", "xCol,yCol").select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable.select()
                            .naturalJoin(rightQueryTable1, "Sym", "xCol,yCol").select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable.select()
                            .naturalJoin(rightQueryTable2, "Sym,intCol=xCol", "xCol,yCol").select();
                    }
                },
        };

        System.out.println("Left Table:");
        TableTools.showWithIndex(leftQueryTable);
        System.out.println("Right Table 1:");
        TableTools.showWithIndex(rightQueryTable1);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(leftQueryTable, i(3, 9), c("Sym", "aa", "aa"), c("intCol", 20, 10),
                c("doubleCol", 2.1, 2.2));
            System.out.println("Left Table Updated:");
            showWithIndex(leftQueryTable);
            leftQueryTable.notifyListeners(i(3, 9), i(), i());
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(leftQueryTable, i(1, 9), c("Sym", "bc", "aa"), c("intCol", 30, 11),
                c("doubleCol", 2.1, 2.2));
            leftQueryTable.notifyListeners(i(), i(), i(1, 9));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(rightQueryTable1, i(3, 4), c("Sym", "ab", "ac"), c("xCol", 55, 33),
                c("yCol", 6.6, 7.7));
            rightQueryTable1.notifyListeners(i(4), i(), i(3));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            show(rightQueryTable2);
            addToTable(rightQueryTable2, i(20, 40), c("Sym", "aa", "bc"),
                c("xCol", 30, 50),
                c("yCol", 1.3, 1.5));
            show(rightQueryTable2);
            rightQueryTable2.notifyListeners(i(40), i(), i(20));
        });
        TstUtils.validate(en);


        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(rightQueryTable1, i(4, 6), c("Sym", "bc", "aa"), c("xCol", 66, 44),
                c("yCol", 7.6, 6.7));
            rightQueryTable1.notifyListeners(i(), i(), i(4, 6));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(rightQueryTable1, i(4, 6), c("Sym", "bc", "aa"), c("xCol", 66, 44),
                c("yCol", 7.7, 6.8));
            rightQueryTable1.notifyListeners(i(), i(), i(4, 6));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(rightQueryTable1, i(4, 31), c("Sym", "aq", "bc"), c("xCol", 66, 44),
                c("yCol", 7.5, 6.9));
            rightQueryTable1.notifyListeners(i(31), i(), i(4));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(rightQueryTable2, i(20, 30), c("Sym", "aa", "aa"),
                c("xCol", 20, 30),
                c("yCol", 3.1, 5.1));
            rightQueryTable2.notifyListeners(i(), i(), i(20, 30));
        });
        TstUtils.validate(en);


        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(rightQueryTable1, i(4));
            rightQueryTable1.notifyListeners(i(), i(4), i());
        });
        TstUtils.validate(en);


        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(rightQueryTable2, i(40), c("Sym", "bc"),
                c("xCol", 20),
                c("yCol", 3.2));
            TstUtils.removeRows(rightQueryTable2, i(20, 30));
            rightQueryTable2.notifyListeners(i(), i(20, 30), i(40));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(leftQueryTable, i(9));
            dumpComplete(leftQueryTable, "Sym", "intCol");
            leftQueryTable.notifyListeners(i(), i(9), i());
        });

        TstUtils.validate(en);
    }

    private void dumpComplete(QueryTable queryTable, String... columns) {
        final Index index = queryTable.getIndex();

        final ColumnSource[] columnSources = new ColumnSource[columns.length];
        for (int ii = 0; ii < columns.length; ++ii) {
            columnSources[ii] = queryTable.getColumnSourceMap().get(columns[ii]);
        }

        final StringBuilder sb = new StringBuilder();

        sb.append("Complete Table has ").append(index.size()).append(" rows:\n");
        sb.append("Index=").append(index).append("\n");
        for (final Index.Iterator it = index.iterator(); it.hasNext();) {
            final long value = it.nextLong();
            final Object[] keyValues = new Object[columns.length];
            for (int ii = 0; ii < columns.length; ++ii) {
                keyValues[ii] = columnSources[ii].get(value);
            }
            sb.append(value).append("=").append(new SmartKey(keyValues)).append("\n");
        }

        final Index prevIndex = index.getPrevIndex();
        sb.append("Complete Previous Table has ").append(prevIndex.size()).append(" rows:\n");
        sb.append("Index=").append(index).append("\n");
        for (final Index.Iterator it = prevIndex.iterator(); it.hasNext();) {
            final long value = it.nextLong();
            final Object[] keyValues = new Object[columns.length];
            for (int ii = 0; ii < columns.length; ++ii) {
                keyValues[ii] = columnSources[ii].getPrev(value);
            }
            sb.append(value).append("=").append(new SmartKey(keyValues)).append("\n");
        }

        System.out.println(sb);
    }

    public void testNaturalJoinIterative2() {
        final QueryTable leftQueryTable = TstUtils.testRefreshingTable(i(1, 2, 4, 6),
            c("Sym", "aa", "bc", "aa", "aa"),
            c("intCol", 10, 20, 30, 50),
            c("doubleCol", 0.1, 0.2, 0.3, 0.5));

        final QueryTable rightQueryTable2 = TstUtils.testRefreshingTable(i(10, 20, 30),
            c("Sym", "aa", "bc", "aa"),
            c("xCol", 11, 20, 20),
            c("yCol", 1.1, 2.2, 5.5));


        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable.naturalJoin(rightQueryTable2.lastBy("Sym"), "Sym",
                            "xCol,yCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftQueryTable
                            .naturalJoin(rightQueryTable2.lastBy("Sym"), "Sym", "xCol,yCol")
                            .select();
                    }
                }
        };
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(leftQueryTable, i(3, 9), c("Sym", "aa", "aa"), c("intCol", 20, 10),
                c("doubleCol", 2.1, 2.2));
            leftQueryTable.notifyListeners(i(3, 9), i(), i());
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(leftQueryTable, i(1, 9), c("Sym", "bc", "aa"), c("intCol", 30, 11),
                c("doubleCol", 2.1, 2.2));
            leftQueryTable.notifyListeners(i(), i(), i(1, 9));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            show(rightQueryTable2);
            addToTable(rightQueryTable2, i(20, 40), c("Sym", "aa", "bc"),
                c("xCol", 30, 50),
                c("yCol", 1.3, 1.5));
            show(rightQueryTable2);
            rightQueryTable2.notifyListeners(i(40), i(), i(20));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(rightQueryTable2, i(20, 30), c("Sym", "aa", "aa"),
                c("xCol", 20, 30),
                c("yCol", 3.1, 5.1));
            rightQueryTable2.notifyListeners(i(), i(), i(20, 30));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(rightQueryTable2, i(40), c("Sym", "bc"),
                c("xCol", 20),
                c("yCol", 3.2));
            TstUtils.removeRows(rightQueryTable2, i(20));
            rightQueryTable2.notifyListeners(i(), i(20), i(40));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(leftQueryTable, i(9));
            leftQueryTable.notifyListeners(i(), i(9), i());
        });
        TstUtils.validate(en);
    }

    public void testNaturalJoinSortedData() {
        final QueryTable leftTable = TstUtils.testRefreshingTable(
            c("Sym", "a", "b", "c"),
            c("Size", 1, 2, 3));
        final QueryTable rightTable = TstUtils.testRefreshingTable(
            c("Sym", "a", "b", "c"),
            c("Qty", 10, 20, 30));

        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return leftTable.naturalJoin(rightTable, "Sym", "Qty");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.sortDescending("Size").naturalJoin(rightTable, "Sym",
                            "Qty");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.sortDescending("Size")
                            .naturalJoin(rightTable.sortDescending("Qty"), "Sym", "Qty");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.naturalJoin(rightTable.sortDescending("Qty"), "Sym",
                            "Qty");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.sortDescending("Size")
                            .naturalJoin(rightTable.sortDescending("Qty"), "Sym", "Qty")
                            .update("x = Qty*Size");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.sortDescending("Size")
                            .naturalJoin(rightTable.sortDescending("Qty"), "Sym", "Qty")
                            .updateView("x = Qty*Size");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.sortDescending("Size")
                            .naturalJoin(rightTable.sortDescending("Qty"), "Sym", "Qty")
                            .view("Sym", "x = Qty*Size");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.sortDescending("Size")
                            .naturalJoin(rightTable.sortDescending("Qty"), "Sym", "Qty")
                            .select("Sym", "x = Qty*Size");
                    }
                },
        };

        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(leftTable, i(0, 1, 2),
                c("Sym", "c", "a", "b"), c("Size", 1, 2, 3));
            leftTable.notifyListeners(i(), i(), i(0, 1, 2));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(rightTable, i(0, 1, 2),
                c("Sym", "b", "c", "a"), c("Qty", 10, 20, 30));
            rightTable.notifyListeners(i(), i(), i(0, 1, 2));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(leftTable, i(0, 1, 2),
                c("Sym", "a", "b", "c"), c("Size", 3, 1, 2));
            leftTable.notifyListeners(i(), i(), i(0, 1, 2));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(rightTable, i(0, 1, 2),
                c("Sym", "a", "b", "c"), c("Qty", 30, 10, 20));
            rightTable.notifyListeners(i(), i(), i(0, 1, 2));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(leftTable, i(3, 4),
                c("Sym", "d", "e"), c("Size", -1, 100));
            leftTable.notifyListeners(i(3, 4), i(), i());
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(rightTable, i(3, 4),
                c("Sym", "e", "d"), c("Qty", -10, 50));
            rightTable.notifyListeners(i(3, 4), i(), i());
        });
        TstUtils.validate(en);

    }

    public void testExactJoin() {
        Table table1 = testRefreshingTable(
            c("String", "c", "e", "g"));

        try {
            table1.exactJoin(testRefreshingTable(c("String", "c", "e"), c("v", 1, 2)), "String");
            TestCase.fail("Previous statement should have thrown an exception");
        } catch (Exception e) {
            assertEquals("Tables don't have one-to-one mapping - no mappings for key g.",
                e.getMessage());
        }


        Table table2 = testRefreshingTable(c("String", "c", "e", "g"), c("v", 1, 2, 3));

        Table pairMatch = table1.exactJoin(table2, "String");
        assertEquals(3, pairMatch.size());
        assertEquals(2, pairMatch.getColumns().length);
        assertEquals("String", pairMatch.getColumns()[0].getName());
        assertEquals("v", pairMatch.getColumns()[1].getName());
        assertEquals(String.class, pairMatch.getColumns()[0].getType());
        assertEquals(int.class, pairMatch.getColumns()[1].getType());
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumns()[0].getDirect()));
        assertEquals(asList(1, 2, 3), asList(pairMatch.getColumn("v").get(0, 3)));

        pairMatch = table2.exactJoin(table1, "String");
        assertEquals(3, pairMatch.size());
        assertEquals(2, pairMatch.getColumns().length);
        assertEquals("String", pairMatch.getColumns()[0].getName());
        assertEquals("v", pairMatch.getColumns()[1].getName());
        assertEquals(String.class, pairMatch.getColumns()[0].getType());
        assertEquals(int.class, pairMatch.getColumns()[1].getType());
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumns()[0].getDirect()));
        assertEquals(asList(1, 2, 3), asList(pairMatch.getColumn("v").get(0, 3)));

        pairMatch = table1.exactJoin(table2, "String=String");
        assertEquals(3, pairMatch.size());
        assertEquals(2, pairMatch.getColumns().length);
        assertEquals("String", pairMatch.getColumns()[0].getName());
        assertEquals("v", pairMatch.getColumns()[1].getName());
        assertEquals(String.class, pairMatch.getColumns()[0].getType());
        assertEquals(int.class, pairMatch.getColumns()[1].getType());
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumns()[0].getDirect()));
        assertEquals(asList(1, 2, 3), asList(pairMatch.getColumn("v").get(0, 3)));

        pairMatch = table2.exactJoin(table1, "String=String");

        assertEquals(3, pairMatch.size());
        assertEquals(2, pairMatch.getColumns().length);
        assertEquals("String", pairMatch.getColumns()[0].getName());
        assertEquals("v", pairMatch.getColumns()[1].getName());
        assertEquals(String.class, pairMatch.getColumns()[0].getType());
        assertEquals(int.class, pairMatch.getColumns()[1].getType());
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumns()[0].getDirect()));
        assertEquals(1, pairMatch.getColumn("v").getInt(0));
        assertEquals(2, pairMatch.getColumn("v").getInt(1));
        assertEquals(3, pairMatch.getColumn("v").getInt(2));


        table1 = testRefreshingTable(c("String1", "c", "e", "g"));

        table2 = testRefreshingTable(c("String2", "c", "e", "g"), c("v", 1, 2, 3));

        pairMatch = table1.exactJoin(table2, "String1=String2");
        assertEquals(3, pairMatch.size());
        assertEquals(3, pairMatch.getColumns().length);
        assertEquals("String1", pairMatch.getColumns()[0].getName());
        assertEquals("String2", pairMatch.getColumns()[1].getName());
        assertEquals("v", pairMatch.getColumns()[2].getName());
        assertEquals(String.class, pairMatch.getColumns()[0].getType());
        assertEquals(String.class, pairMatch.getColumns()[1].getType());
        assertEquals(int.class, pairMatch.getColumns()[2].getType());
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumns()[0].getDirect()));
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumns()[1].getDirect()));
        assertEquals(asList(1, 2, 3), asList(pairMatch.getColumns()[2].get(0, 3)));


        pairMatch = table2.exactJoin(table1, "String2=String1");

        assertEquals(3, pairMatch.size());
        assertEquals(3, pairMatch.getColumns().length);
        assertEquals("String2", pairMatch.getColumns()[0].getName());
        assertEquals("v", pairMatch.getColumns()[1].getName());
        assertEquals("String1", pairMatch.getColumns()[2].getName());
        assertEquals(String.class, pairMatch.getColumn("String1").getType());
        assertEquals(String.class, pairMatch.getColumn("String2").getType());
        assertEquals(int.class, pairMatch.getColumn("v").getType());
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumn("String1").getDirect()));
        assertEquals(asList("c", "e", "g"),
            asList((Object[]) pairMatch.getColumn("String2").getDirect()));
        assertEquals(asList(1, 2, 3), asList(pairMatch.getColumn("v").get(0, 3)));
    }

    public void testSymbolTableJoin() throws IOException {
        diskBackedTestHarness((left, right) -> {
            final Table result = left.naturalJoin(right, "Symbol");
            TableTools.showWithIndex(result);

            final int[] rightSide = intColumn(result, "RightSentinel");
            assertEquals(new int[] {101, 102, 103, NULL_INT, 101, 103, 102, 102, 103}, rightSide);
        });
    }

    private void diskBackedTestHarness(BiConsumer<Table, Table> testFunction) throws IOException {
        final File leftDirectory = Files.createTempDirectory("QueryTableJoinTest-Left").toFile();
        final File rightDirectory = Files.createTempDirectory("QueryTableJoinTest-Right").toFile();

        try {
            final Table leftTable = makeLeftDiskTable(new File(leftDirectory, "Left.parquet"));
            final Table rightTable = makeRightDiskTable(new File(rightDirectory, "Right.parquet"));

            testFunction.accept(leftTable, rightTable);

            leftTable.close();
            rightTable.close();
        } finally {
            FileUtils.deleteRecursively(leftDirectory);
            FileUtils.deleteRecursively(rightDirectory);
        }
    }

    @NotNull
    private Table makeLeftDiskTable(File leftLocation) throws IOException {
        final TableDefinition leftDefinition = TableDefinition.of(
            ColumnDefinition.ofString("Symbol"),
            ColumnDefinition.ofInt("LeftSentinel"));
        final String[] leftSyms = new String[] {"Apple", "Banana", "Cantaloupe", "DragonFruit",
                "Apple", "Cantaloupe", "Banana", "Banana", "Cantaloupe"};
        final Table leftTable = newTable(stringCol("Symbol", leftSyms)).update("LeftSentinel=i");
        ParquetTools.writeTable(leftTable, leftLocation, leftDefinition);
        return ParquetTools.readTable(leftLocation);
    }

    @NotNull
    private Table makeRightDiskTable(File rightLocation) throws IOException {
        final TableDefinition rightDefinition = TableDefinition.of(
            ColumnDefinition.ofString("Symbol"),
            ColumnDefinition.ofInt("RightSentinel"));
        final String[] rightSyms = new String[] {"Elderberry", "Apple", "Banana", "Cantaloupe"};
        final Table rightTable =
            newTable(stringCol("Symbol", rightSyms)).update("RightSentinel=100+i");
        ParquetTools.writeTable(rightTable, rightLocation, rightDefinition);
        return ParquetTools.readTable(rightLocation);
    }
}
