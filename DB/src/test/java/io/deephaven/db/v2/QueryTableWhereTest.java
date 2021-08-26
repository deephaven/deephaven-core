/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.SleepUtil;
import io.deephaven.db.exceptions.QueryCancellationException;
import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.MatchPairFactory;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBPeriod;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.tables.verify.TableAssertions;
import io.deephaven.db.v2.select.*;
import io.deephaven.db.v2.select.chunkfilters.IntRangeComparator;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.sources.UnionRedirection;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.ReflexiveUse;

import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.IntUnaryOperator;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.tables.utils.TableTools.*;
import static io.deephaven.db.tables.utils.WhereClause.whereClause;
import static io.deephaven.db.v2.TstUtils.*;
import static java.util.Arrays.asList;

@Category(OutOfBandTest.class)
public class QueryTableWhereTest extends QueryTableTestBase {

    @Test
    public void testWhere() {

        java.util.function.Function<String, SelectFilter> filter = ConditionFilter::createConditionFilter;
        final QueryTable table = testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", 'a', 'b', 'c'));

        assertEquals("", diff(table.where(filter.apply("k%2 == 0")), table, 10));
        assertEquals("", diff(table.where(filter.apply("i%2 == 0")),
                testRefreshingTable(i(2, 6), c("x", 1, 3), c("y", 'a', 'c')), 10));
        assertEquals("", diff(table.where(filter.apply("(y-'a') = 2")),
                testRefreshingTable(i(2), c("x", 3), c("y", 'c')), 10));
        final QueryTable whereResult = (QueryTable) table.where(filter.apply("x%2 == 1"));
        final Listener whereResultListener = new ListenerWithGlobals(whereResult);
        whereResult.listenForUpdates(whereResultListener);
        assertEquals("", diff(whereResult,
                testRefreshingTable(i(2, 6), c("x", 1, 3), c("y", 'a', 'c')), 10));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(7, 9), c("x", 4, 5), c("y", 'd', 'e'));
            table.notifyListeners(i(7, 9), i(), i());
        });

        assertEquals("", diff(whereResult,
                testRefreshingTable(i(2, 6, 9), c("x", 1, 3, 5), c("y", 'a', 'c', 'e')), 10));
        assertEquals(added, i(9));
        assertEquals(removed, i());
        assertEquals(modified, i());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(7, 9), c("x", 3, 10), c("y", 'e', 'd'));
            table.notifyListeners(i(), i(), i(7, 9));
        });

        assertEquals("", diff(whereResult,
                testRefreshingTable(i(2, 6, 7), c("x", 1, 3, 3), c("y", 'a', 'c', 'e')), 10));

        assertEquals(added, i(7));
        assertEquals(removed, i(9));
        assertEquals(modified, i());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            removeRows(table, i(2, 6, 7));
            table.notifyListeners(i(), i(2, 6, 7), i());
        });

        assertTableEquals(testRefreshingTable(i(), intCol("x"), charCol("y")), whereResult);

        assertEquals(added, i());
        assertEquals(removed, i(2, 6, 7));
        assertEquals(modified, i());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            removeRows(table, i(9));
            addToTable(table, i(2, 4, 6), c("x", 1, 21, 3), c("y", 'a', 'x', 'c'));
            table.notifyListeners(i(2, 6), i(9), i(4));
        });

        assertEquals("", diff(whereResult,
                testRefreshingTable(i(2, 4, 6), c("x", 1, 21, 3), c("y", 'a', 'x', 'c')), 10));

        assertEquals(added, i(2, 4, 6));
        assertEquals(removed, i());
        assertEquals(modified, i());

    }

    public void testWhereBiggerTable() {
        final Table table = TableTools.emptyTable(100000).update("Sym=ii%2==0 ? `AAPL` : `BANANA`", "II=ii").select();
        final Table filtered = table.where("Sym = (`AAPL`)");
        assertTableEquals(TableTools.emptyTable(50000).update("Sym=`AAPL`", "II=ii*2"), filtered);
        TableTools.showWithIndex(filtered);
    }

    public void testIandK() {
        final Table table = testRefreshingTable(i(2, 4, 6), intCol("x", 1, 2, 3));

        assertEquals(newTable(intCol("x", 2)), table.where("k=4"));
        assertEquals(newTable(intCol("x", 2, 3)), table.where("ii > 0"));
        assertEquals(newTable(intCol("x", 1)), table.where("i < 1"));
    }


    // this has no changes from the original testWhere, it is just to make sure that we still work as a single clause
    @Test
    public void testWhereOneOfSingle() {
        final QueryTable table = testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", 'a', 'b', 'c'));

        assertEquals("", diff(table.whereOneOf(whereClause("k%2 == 0")), table, 10));
        assertEquals("", diff(table.whereOneOf(whereClause("i%2 == 0")),
                testRefreshingTable(i(2, 6), c("x", 1, 3), c("y", 'a', 'c')), 10));
        assertEquals("", diff(table.whereOneOf(whereClause("(y-'a') = 2")),
                testRefreshingTable(i(2), c("x", 3), c("y", 'c')), 10));

        final QueryTable whereResult = (QueryTable) table.whereOneOf(whereClause("x%2 == 1"));
        final Listener whereResultListener = new ListenerWithGlobals(whereResult);
        whereResult.listenForUpdates(whereResultListener);
        assertEquals("", diff(whereResult,
                testRefreshingTable(i(2, 6), c("x", 1, 3), c("y", 'a', 'c')), 10));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(7, 9), c("x", 4, 5), c("y", 'd', 'e'));
            table.notifyListeners(i(7, 9), i(), i());
        });

        assertEquals("", diff(whereResult,
                testRefreshingTable(i(2, 6, 9), c("x", 1, 3, 5), c("y", 'a', 'c', 'e')), 10));
        assertEquals(added, i(9));
        assertEquals(removed, i());
        assertEquals(modified, i());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(7, 9), c("x", 3, 10), c("y", 'e', 'd'));
            table.notifyListeners(i(), i(), i(7, 9));
        });

        assertEquals("", diff(whereResult,
                testRefreshingTable(i(2, 6, 7), c("x", 1, 3, 3), c("y", 'a', 'c', 'e')), 10));

        assertEquals(added, i(7));
        assertEquals(removed, i(9));
        assertEquals(modified, i());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            removeRows(table, i(2, 6, 7));
            table.notifyListeners(i(), i(2, 6, 7), i());
        });

        assertTableEquals(testRefreshingTable(i(), intCol("x"), charCol("y")), whereResult);

        assertEquals(added, i());
        assertEquals(removed, i(2, 6, 7));
        assertEquals(modified, i());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            removeRows(table, i(9));
            addToTable(table, i(2, 4, 6), c("x", 1, 21, 3), c("y", 'a', 'x', 'c'));
            table.notifyListeners(i(2, 6), i(9), i(4));
        });

        assertEquals("", diff(whereResult,
                testRefreshingTable(i(2, 4, 6), c("x", 1, 21, 3), c("y", 'a', 'x', 'c')), 10));

        assertEquals(added, i(2, 4, 6));
        assertEquals(removed, i());
        assertEquals(modified, i());

    }

    // adds a second clause
    @Test
    public void testWhereOneOfTwo() {
        final QueryTable table = testRefreshingTable(i(2, 4, 6, 8), c("x", 1, 2, 3, 4), c("y", 'a', 'b', 'c', 'f'));

        assertEquals("", diff(table.whereOneOf(whereClause("k%2 == 0")), table, 10));
        assertEquals("", diff(table.whereOneOf(whereClause("i%2 == 0")),
                testRefreshingTable(i(2, 6), c("x", 1, 3), c("y", 'a', 'c')), 10));
        assertEquals("", diff(table.whereOneOf(whereClause("(y-'a') = 2")),
                testRefreshingTable(i(2), c("x", 3), c("y", 'c')), 10));

        final QueryTable whereResult = (QueryTable) table.whereOneOf(whereClause("x%2 == 1"), whereClause("y=='f'"));
        final Listener whereResultListener = new ListenerWithGlobals(whereResult);
        whereResult.listenForUpdates(whereResultListener);
        assertEquals("", diff(whereResult,
                testRefreshingTable(i(2, 6, 8), c("x", 1, 3, 4), c("y", 'a', 'c', 'f')), 10));


        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(7, 9), c("x", 4, 5), c("y", 'd', 'e'));
            table.notifyListeners(i(7, 9), i(), i());
        });

        assertEquals("", diff(whereResult,
                testRefreshingTable(i(2, 6, 8, 9), c("x", 1, 3, 4, 5), c("y", 'a', 'c', 'f', 'e')), 10));
        assertEquals(added, i(9));
        assertEquals(removed, i());
        assertEquals(modified, i());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(7, 9), c("x", 3, 10), c("y", 'e', 'd'));
            table.notifyListeners(i(), i(), i(7, 9));
        });

        assertEquals("", diff(whereResult,
                testRefreshingTable(i(2, 6, 7, 8), c("x", 1, 3, 3, 4), c("y", 'a', 'c', 'e', 'f')), 10));

        assertEquals(added, i(7));
        assertEquals(removed, i(9));
        assertEquals(modified, i());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            removeRows(table, i(2, 6, 7));
            table.notifyListeners(i(), i(2, 6, 7), i());
        });

        assertEquals("", diff(whereResult,
                testRefreshingTable(i(8), c("x", 4), c("y", 'f')), 10));

        assertEquals(added, i());
        assertEquals(removed, i(2, 6, 7));
        assertEquals(modified, i());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            removeRows(table, i(9));
            addToTable(table, i(2, 4, 6), c("x", 1, 21, 3), c("y", 'a', 'x', 'c'));
            table.notifyListeners(i(2, 6), i(9), i(4));
        });

        assertEquals("", diff(whereResult,
                testRefreshingTable(i(2, 4, 6, 8), c("x", 1, 21, 3, 4), c("y", 'a', 'x', 'c', 'f')), 10));

        assertEquals(added, i(2, 4, 6));
        assertEquals(removed, i());
        assertEquals(modified, i());

        TableTools.showWithIndex(table);
        final Table usingStringArray = table.whereOneOf("x%3 == 0", "y=='f'");
        assertEquals("", diff(usingStringArray,
                testRefreshingTable(i(4, 6, 8), c("x", 21, 3, 4), c("y", 'x', 'c', 'f')), 10));
    }

    @Test
    public void testWhereInDependency() {
        final QueryTable tableToFilter = testRefreshingTable(i(10, 11, 12, 13, 14, 15), c("A", 1, 2, 3, 4, 5, 6),
                c("B", 2, 4, 6, 8, 10, 12), c("C", 'a', 'b', 'c', 'd', 'e', 'f'));

        final QueryTable setTable = testRefreshingTable(i(100, 101, 102), c("A", 1, 2, 3), c("B", 2, 4, 6));
        final Table setTable1 = setTable.where("A > 2");
        final Table setTable2 = setTable.where("B > 6");

        final DynamicWhereFilter dynamicFilter1 =
                new DynamicWhereFilter(setTable1, true, MatchPairFactory.getExpressions("A"));
        final DynamicWhereFilter dynamicFilter2 =
                new DynamicWhereFilter(setTable2, true, MatchPairFactory.getExpressions("B"));

        final SelectFilter composedFilter = DisjunctiveFilter.makeDisjunctiveFilter(dynamicFilter1, dynamicFilter2);
        final Table composed = tableToFilter.where(composedFilter);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TestCase.assertTrue(dynamicFilter1.satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertTrue(dynamicFilter2.satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertTrue(((QueryTable) composed).satisfied(LogicalClock.DEFAULT.currentStep()));
        });

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(setTable, i(103), c("A", 5), c("B", 8));
            setTable.notifyListeners(i(103), i(), i());

            TestCase.assertFalse(((QueryTable) setTable1).satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertFalse(((QueryTable) setTable2).satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertFalse(dynamicFilter1.satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertFalse(dynamicFilter2.satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertFalse(((QueryTable) composed).satisfied(LogicalClock.DEFAULT.currentStep()));

            // this will do the notification for table; which should first fire the recorder for setTable1
            LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();
            // this will do the notification for table; which should first fire the recorder for setTable2
            LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();
            // this will do the notification for table; which should first fire the merged listener for 1
            boolean flushed = LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);

            TestCase.assertTrue(((QueryTable) setTable1).satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertFalse(((QueryTable) setTable2).satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertFalse(dynamicFilter1.satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertFalse(dynamicFilter2.satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertFalse(((QueryTable) composed).satisfied(LogicalClock.DEFAULT.currentStep()));


            // the next notification should be the merged listener for setTable2
            flushed = LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);

            TestCase.assertTrue(((QueryTable) setTable1).satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertTrue(((QueryTable) setTable2).satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertFalse(dynamicFilter1.satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertFalse(dynamicFilter2.satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertFalse(((QueryTable) composed).satisfied(LogicalClock.DEFAULT.currentStep()));

            // the dynamicFilter1 updates
            flushed = LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);

            TestCase.assertTrue(((QueryTable) setTable1).satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertTrue(((QueryTable) setTable2).satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertTrue(dynamicFilter1.satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertFalse(dynamicFilter2.satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertFalse(((QueryTable) composed).satisfied(LogicalClock.DEFAULT.currentStep()));

            // the dynamicFilter2 updates
            flushed = LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);

            TestCase.assertTrue(((QueryTable) setTable1).satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertTrue(((QueryTable) setTable2).satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertTrue(dynamicFilter1.satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertTrue(dynamicFilter2.satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertFalse(((QueryTable) composed).satisfied(LogicalClock.DEFAULT.currentStep()));

            // now that both filters are complete, we can run the composed listener
            flushed = LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);

            TestCase.assertTrue(((QueryTable) setTable1).satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertTrue(((QueryTable) setTable2).satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertTrue(dynamicFilter1.satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertTrue(dynamicFilter2.satisfied(LogicalClock.DEFAULT.currentStep()));
            TestCase.assertTrue(((QueryTable) composed).satisfied(LogicalClock.DEFAULT.currentStep()));

            // and we are done
            flushed = LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();
            TestCase.assertFalse(flushed);
        });

        TableTools.show(composed);

        final Table expected =
                TableTools.newTable(intCol("A", 3, 4, 5), intCol("B", 6, 8, 10), charCol("C", 'c', 'd', 'e'));

        TestCase.assertEquals("", TableTools.diff(composed, expected, 10));
    }

    @Test
    public void testWhereDynamicIn() {
        final QueryTable setTable = testRefreshingTable(i(2, 4, 6, 8), c("X", "A", "B", "C", "B"));
        final QueryTable filteredTable = testRefreshingTable(i(1, 2, 3, 4, 5), c("X", "A", "B", "C", "D", "E"));

        final Table result =
                LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> filteredTable.whereIn(setTable, "X"));
        final Table resultInverse =
                LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> filteredTable.whereNotIn(setTable, "X"));
        show(result);
        assertEquals(3, result.size());
        assertEquals(asList("A", "B", "C"), asList((String[]) result.getColumn("X").getDirect()));
        assertEquals(2, resultInverse.size());
        assertEquals(asList("D", "E"), asList((String[]) resultInverse.getColumn("X").getDirect()));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(filteredTable, i(6), c("X", "A"));
            filteredTable.notifyListeners(i(6), i(), i());
        });
        show(result);
        assertEquals(4, result.size());
        assertEquals(asList("A", "B", "C", "A"), asList((String[]) result.getColumn("X").getDirect()));
        assertEquals(2, resultInverse.size());
        assertEquals(asList("D", "E"), asList((String[]) resultInverse.getColumn("X").getDirect()));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(setTable, i(7), c("X", "D"));
            setTable.notifyListeners(i(7), i(), i());
        });
        showWithIndex(result);
        assertEquals(5, result.size());
        assertEquals(asList("A", "B", "C", "D", "A"), asList((String[]) result.getColumn("X").getDirect()));
        assertEquals(1, resultInverse.size());
        assertEquals(asList("E"), asList((String[]) resultInverse.getColumn("X").getDirect()));
    }

    @Test
    public void testWhereDynamicInIncremental() {
        final ColumnInfo[] setInfo;
        final ColumnInfo[] filteredInfo;

        final int setSize = 10;
        final int filteredSize = 500;
        final Random random = new Random(0);

        final QueryTable setTable = getTable(setSize, random, setInfo = initColumnInfos(
                new String[] {"Sym", "intCol", "doubleCol", "charCol", "byteCol", "floatCol", "longCol", "shortCol"},
                new SetGenerator<>("aa", "bb", "bc", "cc", "dd"),
                new IntGenerator(-100, 100),
                new DoubleGenerator(0, 100),
                new SetGenerator<>('a', 'b', 'c', 'd', 'e', 'f'),
                new ByteGenerator((byte) 0, (byte) 64),
                new SetGenerator<>(1.0f, 2.0f, 3.3f, null),
                new LongGenerator(0, 1000),
                new ShortGenerator((short) 500, (short) 600)));
        final QueryTable filteredTable = getTable(filteredSize, random, filteredInfo = initColumnInfos(
                new String[] {"Sym", "intCol", "doubleCol", "charCol", "byteCol", "floatCol", "longCol", "shortCol"},
                new SetGenerator<>("aa", "bb", "bc", "cc", "dd", "ee", "ff", "gg", "hh", "ii"),
                new IntGenerator(-100, 100),
                new DoubleGenerator(0, 100),
                new CharGenerator('a', 'z'),
                new ByteGenerator((byte) 0, (byte) 127),
                new SetGenerator<>(1.0f, 2.0f, 3.3f, null, 4.4f, 5.5f, 6.6f),
                new LongGenerator(1500, 2500),
                new ShortGenerator((short) 400, (short) 700)));


        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "Sym")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "Sym")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "Sym", "intCol")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "charCol")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "byteCol")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "shortCol")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "intCol")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "longCol")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "floatCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "Sym", "intCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "charCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "byteCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "shortCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "intCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "longCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "floatCol")),
        };

        try {
            for (int i = 0; i < 100; i++) {
                final boolean modSet = random.nextInt(10) < 1;
                final boolean modFiltered = random.nextBoolean();

                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                    if (modSet) {
                        GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                                setSize, random, setTable, setInfo);
                    }
                });
                validate(en);

                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                    if (modFiltered) {
                        GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                                filteredSize, random, filteredTable, filteredInfo);
                    }
                });
                validate(en);
            }
        } catch (Exception e) {
            TestCase.fail(e.getMessage());
        }
    }

    @Test
    public void testWhereRefresh() {
        final Table t1 = TableTools.newTable(col("A", "b", "c", "d"));
        assertFalse(t1.isLive());
        final Table t2 = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> t1.where("A in `b`"));
        assertFalse(t2.isLive());
        final Table t3 = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> t1.whereIn(t1, "A"));
        assertFalse(t3.isLive());

        final Random random = new Random(0);
        final QueryTable t4 = getTable(10, random, initColumnInfos(new String[] {"B"}, new SetGenerator<>("a", "b")));
        assertTrue(t4.isLive());
        final Table t5 = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> t4.where("B in `b`"));
        assertTrue(t5.isLive());
        final Table t6 = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> t4.whereIn(t1, "B=A"));
        assertTrue(t6.isLive());

        final Table t7 = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> t1.whereIn(t4, "A=B"));
        assertTrue(t7.isLive());
    }

    @Test
    public void testWhereInDiamond() {
        final ColumnInfo[] filteredInfo;

        final int size = 500;
        final Random random = new Random(0);

        final QueryTable table = getTable(size, random,
                filteredInfo = initColumnInfos(new String[] {"Sym", "intCol", "intCol2"},
                        new SetGenerator<>("aa", "bb", "bc", "cc", "dd", "ee", "ff", "gg", "hh", "ii"),
                        new IntGenerator(0, 100),
                        new IntGenerator(0, 100)));

        final EvalNugget en[] = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return LiveTableMonitor.DEFAULT.exclusiveLock()
                                .computeLocked(() -> table.whereIn(table.where("intCol % 25 == 0"), "intCol2=intCol"));
                    }
                },
        };

        try {
            for (int i = 0; i < 100; i++) {
                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(
                        () -> GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                                size, random, table, filteredInfo));
                validate(en);
            }
        } catch (Exception e) {
            TestCase.fail(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    @Test
    public void testWhereInDiamond2() {
        final QueryTable table = testRefreshingTable(i(1, 2, 3), c("x", 1, 2, 3), c("y", 2, 4, 6));
        final Table setTable = table.where("x % 2 == 0").dropColumns("y");
        final Table filteredTable =
                LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> table.whereIn(setTable, "y=x"));

        TableTools.show(filteredTable);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(4), c("x", 4), c("y", 8));
            table.notifyListeners(i(4), i(), i());
        });

        TableTools.show(setTable);
        TableTools.show(filteredTable);
    }

    @Test
    public void testWhereNotInEmpty() {
        final Table x = newTable(intCol("X", 1, 2, 3));
        final Table emptyX = newTable(intCol("X"));
        final Table xWhereNotIn = x.whereNotIn(emptyX, "X");
        assertTableEquals(x, xWhereNotIn);

        final int[] emptyArray = new int[0];
        QueryScope.addParam("emptyArray", emptyArray);
        final Table expressionNot = x.where("X not in emptyArray");
        assertTableEquals(x, expressionNot);

        final Table xWhereIn = x.whereIn(emptyX, "X");
        assertTableEquals(emptyX, xWhereIn);

        final Table expressionIn = x.where("X in emptyArray");
        assertTableEquals(emptyX, expressionIn);
        QueryScope.addParam("emptyArray", null);
    }

    @Test
    public void testWhereOneOfIncremental() {

        final ColumnInfo[] filteredInfo;

        final int setSize = 10;
        final int filteredSize = 500;
        final Random random = new Random(0);

        final QueryTable filteredTable = getTable(setSize, random,
                filteredInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("aa", "bb", "bc", "cc", "dd", "ee", "ff", "gg", "hh", "ii"),
                        new IntGenerator(0, 100),
                        new DoubleGenerator(0, 100)));

        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return filteredTable.whereOneOf(whereClause("Sym in `aa`, `ee`"));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return filteredTable.whereOneOf(whereClause("Sym in `aa`, `ee`"),
                                whereClause("intCol % 2 == 0"));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return filteredTable.whereOneOf(whereClause("intCol % 2 == 0", "intCol % 2 == 1"),
                                whereClause("Sym in `aa`, `ee`"));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return filteredTable.whereOneOf(whereClause("intCol % 2 == 0", "Sym in `aa`, `ii`"),
                                whereClause("Sym in `aa`, `ee`"));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return filteredTable.whereOneOf(whereClause("intCol % 2 == 0"), whereClause("intCol % 2 == 1"),
                                whereClause("Sym in `aa`, `ee`"));
                    }
                },
        };

        try {
            for (int i = 0; i < 100; i++) {
                System.out.println("Step = " + i);

                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(
                        () -> GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                                filteredSize, random, filteredTable, filteredInfo));
                validate(en);
            }
        } catch (Exception e) {
            TestCase.fail(e.getMessage());
        }
    }

    @Test
    public void testWhereWithExcessiveShifting() {
        // Select a prime that guarantees shifts from the merge operations.
        final int PRIME = 61409;
        Assert.assertTrue(2 * PRIME > UnionRedirection.CHUNK_MULTIPLE);

        final ColumnInfo[] filteredInfo;

        final int setSize = 10;
        final int filteredSize = 500;
        final Random random = new Random(0);

        final QueryTable growingTable = testRefreshingTable(i(1), c("intCol", 1));
        final QueryTable randomTable = getTable(setSize, random, filteredInfo = initColumnInfos(new String[] {"intCol"},
                new IntGenerator(0, 1 << 8)));
        final Table m2 = TableTools.merge(growingTable, randomTable).updateView("intCol=intCol*53");

        final EvalNugget en[] = new EvalNugget[] {
                EvalNugget.from(() -> TableTools.merge(growingTable, randomTable)),
                EvalNugget.from(() -> TableTools.merge(growingTable, m2).where("intCol % 3 == 0")),
        };

        for (int ii = 1; ii < 100; ++ii) {
            final int fii = PRIME * ii;
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                addToTable(growingTable, i(fii), c("intCol", fii));
                growingTable.notifyListeners(i(fii), i(), i());
                GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, filteredSize,
                        random, randomTable, filteredInfo);
            });
            validate(en);
        }
    }

    @Test
    public void testEmptyWhere() {
        final QueryTable source = testRefreshingTable(i(2, 4, 6, 8),
                c("X", "A", "B", "C", "B"),
                c("I", 1, 2, 4, 8));
        final Table filtered = source.where();

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            ShiftAwareListener.Update update = new ShiftAwareListener.Update();
            update.added = update.removed = i();
            update.modified = source.getIndex().clone();
            update.modifiedColumnSet = source.newModifiedColumnSet("X");
            update.shifted = IndexShiftData.EMPTY;
            source.notifyListeners(update);
        });

        TstUtils.assertTableEquals(source, filtered);
    }

    @Test
    public void testWhereBoolean() {
        final Random random = new Random(0);
        final int size = 10;

        final ColumnInfo[] columnInfo;
        final QueryTable filteredTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sentinel", "boolCol", "nullBoolCol"},
                        new IntGenerator(0, 10000),
                        new BooleanGenerator(0.5),
                        new BooleanGenerator(0.5, 0.2)));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> filteredTable.where("boolCol")),
                EvalNugget.from(() -> filteredTable.where("!boolCol")),
                EvalNugget.from(() -> SparseSelect.sparseSelect(filteredTable).where("boolCol")),
                EvalNugget.from(() -> SparseSelect.sparseSelect(filteredTable).sort("Sentinel").where("boolCol")),
                EvalNugget.from(
                        () -> SparseSelect.sparseSelect(filteredTable).sort("Sentinel").reverse().where("boolCol")),
                EvalNugget.from(() -> filteredTable.updateView("boolCol2=!!boolCol").where("boolCol2")),
        };

        for (int step = 0; step < 100; ++step) {
            simulateShiftAwareStep(size, random, filteredTable, columnInfo, en);
        }
    }

    private static class SleepCounter implements IntUnaryOperator {
        final int sleepDurationNanos;
        long invokes = 0;
        CountDownLatch latch = new CountDownLatch(1);

        private SleepCounter(int sleepDurationNanos) {
            this.sleepDurationNanos = sleepDurationNanos;
        }

        @Override
        public int applyAsInt(int value) {
            if (sleepDurationNanos > 0) {
                final long start = System.nanoTime();
                final long end = start + sleepDurationNanos;
                // noinspection StatementWithEmptyBody
                while (System.nanoTime() < end);
            }
            if (++invokes == 1) {
                latch.countDown();
            }
            return value;
        }

        void reset() {
            invokes = 0;
            latch = new CountDownLatch(1);
        }
    }

    private static class TestChunkFilter implements io.deephaven.db.v2.select.ChunkFilter {
        final CountDownLatch latch = new CountDownLatch(1);
        final ChunkFilter actualFilter;
        final long sleepDurationNanos;
        long invokes;
        long invokedValues;

        private TestChunkFilter(ChunkFilter actualFilter, long sleepDurationNanos) {
            this.actualFilter = actualFilter;
            this.sleepDurationNanos = sleepDurationNanos;
        }

        @Override
        public void filter(Chunk<? extends Attributes.Values> values, LongChunk<Attributes.OrderedKeyIndices> keys,
                WritableLongChunk<Attributes.OrderedKeyIndices> results) {
            if (++invokes == 1) {
                latch.countDown();
            }
            invokedValues += values.size();
            if (sleepDurationNanos > 0) {
                long nanos = sleepDurationNanos * values.size();
                final long start = System.nanoTime();
                final long end = start + nanos;
                // noinspection StatementWithEmptyBody
                while (System.nanoTime() < end);
            }
            actualFilter.filter(values, keys, results);
        }

        void reset() {
            invokes = invokedValues = 0;
        }
    }

    @Test
    public void testInterFilterInterruption() {
        final Table tableToFilter = TableTools.emptyTable(2_000_000).update("X=i");

        final SleepCounter slowCounter = new SleepCounter(2);
        final SleepCounter fastCounter = new SleepCounter(0);

        QueryScope.addParam("slowCounter", slowCounter);
        QueryScope.addParam("fastCounter", fastCounter);

        final long start = System.currentTimeMillis();
        final Table filtered =
                tableToFilter.where("slowCounter.applyAsInt(X) % 2 == 0", "fastCounter.applyAsInt(X) % 3 == 0");
        final long end = System.currentTimeMillis();
        System.out.println("Duration: " + (end - start));

        assertTableEquals(tableToFilter.where("X%6==0"), filtered);

        assertEquals(2_000_000, slowCounter.invokes);
        assertEquals(1_000_000, fastCounter.invokes);

        fastCounter.reset();
        slowCounter.reset();

        final MutableObject<Exception> caught = new MutableObject<>();
        final Thread t = new Thread(() -> {
            final long start1 = System.currentTimeMillis();
            try {
                tableToFilter.where("slowCounter.applyAsInt(X) % 2 == 0", "fastCounter.applyAsInt(X) % 3 == 0");
            } catch (Exception e) {
                caught.setValue(e);
            }
            final long end1 = System.currentTimeMillis();
            System.out.println("Duration: " + (end1 - start1));
        });
        t.start();

        try {
            if (!slowCounter.latch.await(5000, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Latch never reached zero!");
            }
        } catch (InterruptedException ignored) {
        }

        t.interrupt();

        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertEquals(2_000_000, slowCounter.invokes);
        assertEquals(0, fastCounter.invokes);
        assertNotNull(caught.getValue());
        assertEquals(QueryCancellationException.class, caught.getValue().getClass());

        QueryScope.addParam("slowCounter", null);
        QueryScope.addParam("fastCounter", null);
    }

    @Test
    public void testChunkFilterInterruption() {
        final Table tableToFilter = TableTools.emptyTable(2_000_000).update("X=i");

        final TestChunkFilter slowCounter =
                new TestChunkFilter(IntRangeComparator.makeIntFilter(0, 1_000_000, true, false), 100);

        QueryScope.addParam("slowCounter", slowCounter);

        final long start = System.currentTimeMillis();
        final Index result = ChunkFilter.applyChunkFilter(tableToFilter.getIndex(), tableToFilter.getColumnSource("X"),
                false, slowCounter);
        final long end = System.currentTimeMillis();
        System.out.println("Duration: " + (end - start));

        assertEquals(Index.FACTORY.getIndexByRange(0, 999_999), result);

        assertEquals(2_000_000, slowCounter.invokedValues);
        slowCounter.reset();

        final MutableObject<Exception> caught = new MutableObject<>();
        final Thread t = new Thread(() -> {
            final long start1 = System.currentTimeMillis();
            try {
                ChunkFilter.applyChunkFilter(tableToFilter.getIndex(), tableToFilter.getColumnSource("X"), false,
                        slowCounter);
            } catch (Exception e) {
                caught.setValue(e);
            }
            final long end1 = System.currentTimeMillis();
            System.out.println("Duration: " + (end1 - start1));
        });
        t.start();

        try {
            if (!slowCounter.latch.await(5000, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Latch never reached zero!");
            }
        } catch (InterruptedException ignored) {
        }

        t.interrupt();

        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Invoked Values: " + slowCounter.invokedValues);
        System.out.println("Invokes: " + slowCounter.invokes);

        assertTrue(slowCounter.invokedValues < 2_000_000L);
        assertEquals(1 << 20, slowCounter.invokedValues);
        assertNotNull(caught.getValue());
        assertEquals(QueryCancellationException.class, caught.getValue().getClass());

        QueryScope.addParam("slowCounter", null);
    }

    @ReflexiveUse(referrers = "QueryTableWhereTest.class")
    public static BigInteger convertToBigInteger(long value) {
        return value == QueryConstants.NULL_LONG ? null : BigInteger.valueOf(value);
    }

    private static Table multiplyAssertSorted(Table table, SortingOrder order, String... columns) {
        for (String colName : columns) {
            table = TableAssertions.assertSorted(table, colName, order);
        }
        return table;
    }

    public void testComparableBinarySearch() {
        final Random random = new Random(0);

        final int size = 100;

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"BD1", "D2", "L3", "CH", "DT"},
                        new BigDecimalGenerator(BigInteger.ONE, BigInteger.TEN),
                        new DoubleGenerator(0.0, 100.0, 0, 0, 0, 0),
                        new LongGenerator(-100, 100, 0.01),
                        new CharGenerator('A', 'Z', 0.1),
                        new UnsortedDateTimeGenerator(DBTimeUtils.convertDateTime("2020-01-01T00:00:00 NY"),
                                DBTimeUtils.convertDateTime("2020-01-01T01:00:00 NY"))));
        final String bigIntConversion = "BI4=" + getClass().getCanonicalName() + ".convertToBigInteger(L3)";
        final Table augmentedInts =
                table.update(bigIntConversion, "D5=(double)L3", "I6=(int)L3", "S7=(short)L3", "B8=(byte)L3");
        final Table augmentedFloats = table.update("F6=(float)D2");

        final Table sortedBD1 = table.sort("BD1");
        final Table sortedDT = table.sort("DT");
        final Table sortedCH = table.sort("CH");
        final Table sortedD2 = multiplyAssertSorted(augmentedFloats.sort("D2"), SortingOrder.Ascending, "F6");
        final Table sortedL3 =
                multiplyAssertSorted(augmentedInts.sort("L3"), SortingOrder.Ascending, "BI4", "D5", "I6", "S7", "B8");
        final Table sortedBD1R = table.sortDescending("BD1");
        final Table sortedD2R =
                multiplyAssertSorted(augmentedFloats.sortDescending("D2"), SortingOrder.Descending, "F6");
        final Table sortedL3R = multiplyAssertSorted(augmentedInts.sortDescending("L3"), SortingOrder.Descending, "BI4",
                "D5", "I6", "S7", "B8");

        final BigDecimal two = BigDecimal.valueOf(2);
        final BigDecimal nine = BigDecimal.valueOf(9);
        final String filterTimeString = "2020-01-01T00:30:00 NY";
        final DBDateTime filterTime = DBTimeUtils.convertDateTime(filterTimeString);

        QueryScope.addParam("two", two);
        QueryScope.addParam("nine", nine);

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new TableComparator(sortedBD1.where("BD1.compareTo(two) > 0 && BD1.compareTo(nine) < 0"),
                        sortedBD1.where(ComparableRangeFilter.makeForTest("BD1", two, nine, false, false))),
                new TableComparator(sortedD2.where("D2 > 50 && D2 < 75"),
                        sortedD2.where(new DoubleRangeFilter("D2", 50.0, 75.0, false, false))),
                new TableComparator(sortedL3.where("L3 > -50 && L3 < 80"),
                        sortedL3.where(new LongRangeFilter("L3", -50, 80, false, false))),
                new TableComparator(sortedL3.where("L3 > -50 && L3 <= 80"),
                        sortedL3.where(new LongRangeFilter("L3", -50, 80, false, true))),
                new TableComparator(sortedL3.where("L3 >= -50 && L3 < 80"),
                        sortedL3.where(new LongRangeFilter("L3", -50, 80, true, false))),
                new TableComparator(sortedL3.where("L3 >= -50 && L3 <= 80"),
                        sortedL3.where(new LongRangeFilter("L3", -50, 80, true, true))),
                new TableComparator(sortedL3.where("L3 * 2 >= -100"), sortedL3.where("L3 >= -50")),
                new TableComparator(sortedL3.where("L3 * 2 > -100"), sortedL3.where("L3 > -50")),
                new TableComparator(sortedL3.where("L3 * 2 < -100"), sortedL3.where("L3 < -50")),
                new TableComparator(sortedL3.where("L3 * 2 <= -100"), sortedL3.where("L3 <= -50")),
                new TableComparator(sortedBD1R.where("BD1.compareTo(two) > 0 && BD1.compareTo(nine) < 0"),
                        sortedBD1R.where(ComparableRangeFilter.makeForTest("BD1", two, nine, false, false))),
                new TableComparator(sortedD2R.where("D2 > 50 && D2 < 75"),
                        sortedD2R.where(new DoubleRangeFilter("D2", 50.0, 75.0, false, false))),
                new TableComparator(sortedD2R.where("D2 > 50 && D2 <= 75"), sortedD2R.where("F6 > 50", "F6 <= 75")),
                new TableComparator(sortedL3R.where("L3 > -50 && L3 < 80"),
                        sortedL3R.where(new LongRangeFilter("L3", -50, 80, false, false))),
                new TableComparator(sortedL3R.where("L3 > -50 && L3 <= 80"),
                        sortedL3R.where(new LongRangeFilter("L3", -50, 80, false, true))),
                new TableComparator(sortedL3R.where("L3 >= -50 && L3 < 80"),
                        sortedL3R.where(new LongRangeFilter("L3", -50, 80, true, false))),
                new TableComparator(sortedL3R.where("L3 >= -50 && L3 <= 80"),
                        sortedL3R.where(new LongRangeFilter("L3", -50, 80, true, true))),
                new TableComparator(sortedL3R.where("L3 * 2 >= -100"), sortedL3R.where("L3 >= -50")),
                new TableComparator(sortedL3R.where("L3 * 2 > -100"), sortedL3R.where("L3 > -50")),
                new TableComparator(sortedL3R.where("L3 * 2 < -100"), sortedL3R.where("L3 < -50")),
                new TableComparator(sortedL3R.where("L3 * 2 <= -100"), sortedL3R.where("L3 <= -50")),
                new TableComparator(sortedL3.where("L3 >= -50"), sortedL3.where("D5 >= -50")),
                new TableComparator(sortedL3.where("L3 > -100"), sortedL3.where("D5 > -100")),
                new TableComparator(sortedL3.where("L3 < -50"), sortedL3.where("D5 < -50")),
                new TableComparator(sortedL3.where("L3 <= -50"), sortedL3.where("D5 <= -50")),
                new TableComparator(sortedL3.where("L3 > 10 && L3 < 20"),
                        sortedL3.where(ComparableRangeFilter.makeForTest("BI4", BigInteger.valueOf(10),
                                BigInteger.valueOf(20), false, false))),
                new TableComparator(sortedL3R.where("L3 > 10 && L3 < 20"),
                        sortedL3R.where(ComparableRangeFilter.makeForTest("BI4", BigInteger.valueOf(10),
                                BigInteger.valueOf(20), false, false))),
                new TableComparator(sortedL3.where("L3 <= 20"), "L3", sortedL3.where("BI4 <= 20"), "BI4"),
                new TableComparator(sortedL3R.where("L3 > 20"), "L3", sortedL3R.where("BI4 > 20"), "BI4"),
                new TableComparator(sortedL3.where("L3 < 20"), "L3", sortedL3.where("BI4 < 20"), "BI4"),
                new TableComparator(sortedL3R.where("L3 >= 20"), "L3", sortedL3R.where("BI4 >= 20"), "BI4"),
                new TableComparator(sortedL3R.where("L3 >= 20 && true"), sortedL3R.where("I6 >= 20")),
                new TableComparator(sortedL3R.where("L3 >= 20 && true"), sortedL3R.where("B8 >= 20")),
                new TableComparator(sortedL3R.where("L3 >= 20 && true"), sortedL3R.where("S7 >= 20")),
                new TableComparator(sortedL3R.where("L3 < 20 && true"), sortedL3R.where("I6 < 20")),
                new TableComparator(sortedL3R.where("L3 < 20 && true"), sortedL3R.where("B8 < 20")),
                new TableComparator(sortedL3R.where("L3 < 20 && true"), sortedL3R.where("S7 < 20")),
                new TableComparator(sortedDT.where("DT == null || DT.getNanos() < " + filterTime.getNanos()),
                        sortedDT.where("DT < '" + filterTimeString + "'")),
                new TableComparator(sortedDT.where("DT != null && DT.getNanos() >= " + filterTime.getNanos()),
                        sortedDT.where("DT >= '" + filterTimeString + "'")),
                new TableComparator(sortedCH.where("true && CH > 'M'"), sortedCH.where("CH > 'M'")),
                new TableComparator(sortedCH.where("CH==null || CH <= 'O'"), sortedCH.where("CH <= 'O'")),
                new TableComparator(sortedCH.where("true && CH >= 'Q'"), sortedCH.where("CH >= 'Q'")),
                new TableComparator(sortedCH.where("true && CH < 'F'"), sortedCH.where("CH < 'F'")),
        };

        for (int step = 0; step < 500; step++) {
            if (printTableUpdates) {
                System.out.print("Step = " + step);
            }
            simulateShiftAwareStep(size, random, table, columnInfo, en);
        }

        QueryScope.addParam("two", null);
        QueryScope.addParam("nine", null);
    }


    public void testDbDateTimeRangeFilter() {
        final DBDateTime startTime = DBTimeUtils.convertDateTime("2021-04-23T09:30 NY");
        final DBDateTime[] array = new DBDateTime[10];
        for (int ii = 0; ii < array.length; ++ii) {
            array[ii] = DBTimeUtils.plus(startTime, 60_000_000_000L * ii);
        }
        final Table table = TableTools.newTable(col("DT", array));
        TableTools.showWithIndex(table);

        final Table sorted = table.sort("DT");
        final Table backwards = table.sort("DT");

        assertTableEquals(sorted.where("DT < '" + array[5] + "'"), sorted.where("ii < 5"));
        assertTableEquals(backwards.where("DT < '" + array[5] + "'"), backwards.where("ii < 5"));
    }

    public void testCharRangeFilter() {
        char[] array = new char[10];
        for (int ii = 0; ii < array.length; ++ii) {
            if (ii % 3 == 0) {
                array[ii] = (char) ('Z' - ii);
            } else if (ii % 2 == 0) {
                array[ii] = QueryConstants.NULL_CHAR;
            } else {
                array[ii] = (char) ('A' + ii);
            }
        }
        final Table table = TableTools.newTable(charCol("CH", array));
        TableTools.showWithIndex(table);

        final Table sorted = table.sort("CH");
        final Table backwards = table.sort("CH");

        TableTools.showWithIndex(sorted);
        System.out.println("Pivot: " + array[5]);

        final Table rangeFiltered = sorted.where("CH < '" + array[5] + "'");
        final Table standardFiltered = sorted.where("'" + array[5] + "' > CH");

        TableTools.showWithIndex(rangeFiltered);
        TableTools.showWithIndex(standardFiltered);
        assertTableEquals(rangeFiltered, standardFiltered);
        assertTableEquals(backwards.where("CH < '" + array[5] + "'"), backwards.where("'" + array[5] + "' > CH"));
        assertTableEquals(backwards.where("CH <= '" + array[5] + "'"), backwards.where("'" + array[5] + "' >= CH"));
        assertTableEquals(backwards.where("CH > '" + array[5] + "'"), backwards.where("'" + array[5] + "' < CH"));
        assertTableEquals(backwards.where("CH >= '" + array[5] + "'"), backwards.where("'" + array[5] + "' <= CH"));
    }

    public void testSingleSidedRangeFilterSimple() {
        final Table table = TableTools.emptyTable(10).update("L1=ii");
        final String bigIntConversion = "BI2=" + getClass().getCanonicalName() + ".convertToBigInteger(L1)";
        final Table augmented = table.update(bigIntConversion).sort("BI2");
        final Table augmentedBackwards = table.update(bigIntConversion).sortDescending("BI2");

        assertTableEquals(augmented.where("L1 < 5"), augmented.where("BI2 < 5"));
        assertTableEquals(augmented.where("L1 <= 5"), augmented.where("BI2 <= 5"));
        assertTableEquals(augmented.where("L1 > 5"), augmented.where("BI2 > 5"));
        assertTableEquals(augmented.where("L1 >= 5"), augmented.where("BI2 >= 5"));

        assertTableEquals(augmentedBackwards.where("L1 < 5"), augmentedBackwards.where("BI2 < 5"));
        assertTableEquals(augmentedBackwards.where("L1 <= 5"), augmentedBackwards.where("BI2 <= 5"));
        assertTableEquals(augmentedBackwards.where("L1 > 5"), augmentedBackwards.where("BI2 > 5"));
        assertTableEquals(augmentedBackwards.where("L1 >= 5"), augmentedBackwards.where("BI2 >= 5"));
    }

    public void testComparableRangeFilter() {
        final Random random = new Random(0);

        final int size = 100;

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"L1"}, new LongGenerator(90, 110, 0.1)));

        final String bigIntConversion = "BI2=" + getClass().getCanonicalName() + ".convertToBigInteger(L1)";
        final Table augmented = table.update(bigIntConversion);

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new TableComparator(augmented.where("L1 > 100"), augmented.where("BI2 > 100")),
                new TableComparator(augmented.where("L1 < 100"), augmented.where("BI2 < 100")),
                new TableComparator(augmented.where("L1 >= 100"), augmented.where("BI2 >= 100")),
                new TableComparator(augmented.where("L1 <= 100"), augmented.where("BI2 <= 100")),
                new TableComparator(augmented.where("L1 > 95 && L1 <= 100"),
                        augmented.where(ComparableRangeFilter.makeForTest("BI2", BigInteger.valueOf(95),
                                BigInteger.valueOf(100), false, true))),
                new TableComparator(augmented.where("L1 > 95 && L1 < 100"),
                        augmented.where(ComparableRangeFilter.makeForTest("BI2", BigInteger.valueOf(95),
                                BigInteger.valueOf(100), false, false))),
                new TableComparator(augmented.where("L1 >= 95 && L1 < 100"),
                        augmented.where(ComparableRangeFilter.makeForTest("BI2", BigInteger.valueOf(95),
                                BigInteger.valueOf(100), true, false))),
                new TableComparator(augmented.where("L1 >= 95 && L1 <= 100"),
                        augmented.where(ComparableRangeFilter.makeForTest("BI2", BigInteger.valueOf(95),
                                BigInteger.valueOf(100), true, true))),
        };

        for (int i = 0; i < 500; i++) {
            simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }
}
