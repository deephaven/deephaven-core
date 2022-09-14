/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.impl.RowSetTstUtils;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.time.DateTime;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.table.impl.sources.UnionRedirection;
import io.deephaven.chunk.IntChunk;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.ExceptionDetails;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.junit.*;

import java.io.*;
import java.util.*;
import java.util.function.Consumer;

import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.table.impl.TstUtils.*;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_INT;

import org.junit.experimental.categories.Category;

/**
 * Unit tests for {@link TableTools}.
 */
@Category(OutOfBandTest.class)
public class TestTableTools extends TestCase implements UpdateErrorReporter {

    private static final boolean ENABLE_QUERY_COMPILER_LOGGING = Configuration.getInstance()
            .getBooleanForClassWithDefault(TestTableTools.class, "QueryCompiler.logEnabled", false);

    private UpdateErrorReporter oldReporter;

    private boolean oldCheckUgp;
    private boolean oldLogEnabled;

    private LivenessScope scope;
    private SafeCloseable executionContext;

    private Table table1;
    private Table table2;
    private Table emptyTable;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        oldCheckUgp = UpdateGraphProcessor.DEFAULT.setCheckTableOperations(false);
        oldLogEnabled = QueryCompiler.setLogEnabled(ENABLE_QUERY_COMPILER_LOGGING);
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false);
        UpdatePerformanceTracker.getInstance().enableUnitTestMode();

        scope = new LivenessScope();
        executionContext = ExecutionContext.createForUnitTests().open();
        LivenessScopeStack.push(scope);

        oldReporter = AsyncClientErrorNotifier.setReporter(this);

        table1 = testRefreshingTable(TstUtils.i(2, 3, 6, 7, 8, 10, 12, 15, 16).toTracking(),
                TstUtils.c("StringKeys", "key1", "key1", "key1", "key1", "key2", "key2", "key2", "key2", "key2"),
                TstUtils.c("GroupedInts", 1, 1, 2, 2, 2, 3, 3, 3, 3));
        table2 = testRefreshingTable(TstUtils.i(1, 3, 5, 10, 20, 30, 31, 32, 33).toTracking(),
                TstUtils.c("StringKeys1", "key1", "key1", "key1", "key1", "key2", "key2", "key2", "key2", "key2"),
                TstUtils.c("GroupedInts1", 1, 1, 2, 2, 2, 3, 3, 3, 3));
        emptyTable = testRefreshingTable(TstUtils.c("StringKeys", (Object) CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                TstUtils.c("GroupedInts", (Object) CollectionUtil.ZERO_LENGTH_BYTE_ARRAY));

    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();

        LivenessScopeStack.pop(scope);
        scope.release();
        executionContext.close();
        QueryCompiler.setLogEnabled(oldLogEnabled);
        UpdateGraphProcessor.DEFAULT.setCheckTableOperations(oldCheckUgp);
        AsyncClientErrorNotifier.setReporter(oldReporter);
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(true);
    }

    @Override
    public void reportUpdateError(Throwable t) throws IOException {
        System.err.println("Received error notification: " + new ExceptionDetails(t).getFullStackTrace());
        TestCase.fail(t.getMessage());
    }

    @Test
    public void testMerge() {
        final Table result = TableTools.merge(table1, table1, table1);
        tableRangesAreEqual(table1, result, 0, 0, table1.size());
        tableRangesAreEqual(table1, result, 0, table1.size(), table1.size());
        tableRangesAreEqual(table1, result, 0, table1.size() * 2, table1.size());
    }

    private static void assertThrows(final Runnable runnable) {
        boolean threwException = false;
        try {
            runnable.run();
        } catch (final Exception ignored) {
            threwException = true;
        }
        TestCase.assertTrue(threwException);
    }

    @Test
    public void testMergeWithNullTables() {
        TestTableTools.assertThrows(TableTools::merge);
        TestTableTools.assertThrows(() -> TableTools.merge(null, null));
        TestTableTools.assertThrows(() -> TableTools.merge(null, null, null));

        Table result = TableTools.merge(null, table1, null, null, null);
        tableRangesAreEqual(table1, result, 0, 0, table1.size());

        result = TableTools.merge(table2, null);
        tableRangesAreEqual(table2, result, 0, 0, table2.size());

        result = TableTools.merge(null, table2);
        tableRangesAreEqual(table2, result, 0, 0, table2.size());
    }

    @Test
    public void testMergeOfMismatchedTables() {
        try {
            TableTools.merge(table1, table2);
            TestCase.fail("Expected exception");
        } catch (TableDefinition.IncompatibleTableDefinitionException expected) {
        }

        try {
            TableTools.merge(table2, table1);
            TestCase.fail("Expected exception");
        } catch (TableDefinition.IncompatibleTableDefinitionException expected) {
        }

        try {
            TableTools.merge(table2, emptyTable);
            TestCase.fail("Expected exception");
        } catch (TableDefinition.IncompatibleTableDefinitionException expected) {
        }

        try {
            TableTools.merge(table2, table2.updateView("S2=StringKeys1"));
            TestCase.fail("Expected exception");
        } catch (TableDefinition.IncompatibleTableDefinitionException expected) {
        }

        try {
            TableTools.merge(table2, table2.dropColumns("StringKeys1"));
            TestCase.fail("Expected exception");
        } catch (TableDefinition.IncompatibleTableDefinitionException expected) {
        }
    }

    @Test
    public void testMergeWithWhere() {
        Table t1 = TableTools.emptyTable(1).update("Col=`A`");
        Table t2 = TableTools.emptyTable(1).update("Col=`B`");
        Table t3 = TableTools.emptyTable(1).update("Col=`C`");
        Table t4 = TableTools.emptyTable(1).update("Col=`D`");

        Table t_1_2 = TableTools.merge(t1, t2);
        Table t_3_4 = TableTools.merge(t3, t4);

        Table t_1_2_filtered = t_1_2.where("Col!=`C`");
        Table t_3_4_filtered = t_3_4.where("Col!=`C`");

        // Note that now we still have isUnionedTable(t_3_4_filtered) == true...

        Table t_all = TableTools.merge( // This will still include Col=`C`!!!
                t_1_2_filtered,
                t_3_4_filtered);

        TableTools.show(t_1_2);
        TableTools.show(t_3_4);
        TableTools.show(t_1_2_filtered);
        TableTools.show(t_3_4_filtered);
        TableTools.show(t_all);

        assertEquals(t_all.size(), 3);
        assertTrue(Arrays.equals((Object[]) t_all.getColumn("Col").getDirect(), new String[] {"A", "B", "D"}));
    }

    @Test
    public void testDiff() {
        assertEquals(
                "Column x different from the expected set, first difference at row 1 encountered 2 expected null\n",
                TableTools.diff(TableTools.newTable(intCol("x", 1, 2, 3)),
                        TableTools.newTable(intCol("x", 1, NULL_INT, NULL_INT)), 10));
        assertEquals(
                "Column x different from the expected set, first difference at row 1 encountered null expected 2\n",
                TableTools.diff(TableTools.newTable(intCol("x", 1, NULL_INT, NULL_INT)),
                        TableTools.newTable(intCol("x", 1, 2, 3)), 10));

        assertEquals("",
                TableTools.diff(TableTools.newTable(col("x", 1, 2, 3)), TableTools.newTable(col("x", 1, 2, 3)), 10));
        assertEquals(
                "Column x different from the expected set, first difference at row 1 encountered 2.0 expected null\n",
                TableTools.diff(TableTools.newTable(col("x", 1.0, 2.0, 3.0)),
                        TableTools.newTable(col("x", 1.0, null, null)), 10));
        assertEquals(
                "Column x different from the expected set, first difference at row 1 encountered null expected 2.0\n",
                TableTools.diff(TableTools.newTable(col("x", 1.0, null, null)),
                        TableTools.newTable(col("x", 1.0, 2.0, 3.0)), 10));
        assertEquals(
                "Column x different from the expected set, first difference at row 1 encountered 2.0E-12 expected null\n",
                TableTools.diff(TableTools.newTable(col("x", 0.000000000001, 0.000000000002, 0.000000000003)),
                        TableTools.newTable(col("x", 0.000000000001, null, null)), 10));
        assertEquals(
                "Column x different from the expected set, first difference at row 1 encountered 2.0E-12 expected null\n",
                TableTools.diff(TableTools.newTable(col("x", 0.000000000001, 0.000000000002, 0.000000000003)),
                        TableTools.newTable(col("x", 0.000000000002, null, null)), 10,
                        EnumSet.of(TableDiff.DiffItems.DoublesExact)));
        assertEquals(
                "Column x different from the expected set, first difference at row 0 encountered 1.0E-12 expected 2.0E-12 (difference = 1.0E-12)\n",
                TableTools.diff(TableTools.newTable(col("x", 0.000000000001, 0.000000000002, 0.000000000003)),
                        TableTools.newTable(col("x", 0.000000000002, null, null)), 10));

        assertEquals(
                "Column x different from the expected set, first difference at row 1 encountered null expected 2.0\n",
                TableTools.diff(TableTools.newTable(floatCol("x", 1.0f, NULL_FLOAT, NULL_FLOAT)),
                        TableTools.newTable(floatCol("x", 1.0f, 2.0f, 3.0f)), 10));
        assertEquals("", TableTools.diff(TableTools.newTable(floatCol("x", 1, 2, 3)),
                TableTools.newTable(floatCol("x", 1.0f, 2.0f, 3.0f)), 10));
        assertEquals(
                "Column x different from the expected set, first difference at row 1 encountered 2.0 expected null\n",
                TableTools.diff(TableTools.newTable(floatCol("x", 1.0f, 2.0f, 3.0f)),
                        TableTools.newTable(floatCol("x", 1.0f, NULL_FLOAT, NULL_FLOAT)), 10));
        assertEquals(
                "Column x different from the expected set, first difference at row 1 encountered null expected 2.0\n",
                TableTools.diff(TableTools.newTable(floatCol("x", 1.0f, NULL_FLOAT, NULL_FLOAT)),
                        TableTools.newTable(floatCol("x", 1.0f, 2.0f, 3.0f)), 10));
        assertEquals(
                "Column x different from the expected set, first difference at row 1 encountered 2.0E-12 expected null\n",
                TableTools.diff(TableTools.newTable(floatCol("x", 0.000000000001f, 0.000000000002f, 0.000000000003f)),
                        TableTools.newTable(floatCol("x", 0.000000000001f, NULL_FLOAT, NULL_FLOAT)), 10));
        assertEquals(
                "Column x different from the expected set, first difference at row 1 encountered 2.0E-12 expected null\n",
                TableTools.diff(TableTools.newTable(floatCol("x", 0.000000000001f, 0.000000000002f, 0.000000000003f)),
                        TableTools.newTable(floatCol("x", 0.000000000002f, NULL_FLOAT, NULL_FLOAT)), 10,
                        EnumSet.of(TableDiff.DiffItems.DoublesExact)));
        assertEquals(
                "Column x different from the expected set, first difference at row 0 encountered 1.0E-12 expected 2.0E-12 (difference = 1.0E-12)\n",
                TableTools.diff(TableTools.newTable(floatCol("x", 0.000000000001f, 0.000000000002f, 0.000000000003f)),
                        TableTools.newTable(floatCol("x", 0.000000000002f, NULL_FLOAT, NULL_FLOAT)), 10));
    }

    @Test
    public void testRoundDecimalColumns() {
        Table table = newTable(
                col("String", "c", "e", "g"),
                col("Int", 2, 4, 6),
                col("Double", 1.2, 2.6, Double.NaN),
                col("Float", 1.2f, 2.6f, Float.NaN));


        // Test whether we're rounding all columns properly
        Table roundedColumns = TableTools.roundDecimalColumns(table);
        assertTrue(Arrays.equals((String[]) roundedColumns.getColumn("String").getDirect(),
                (String[]) table.getColumn("String").getDirect()));
        assertTrue(Arrays.equals((int[]) roundedColumns.getColumn("Int").getDirect(),
                (int[]) table.getColumn("Int").getDirect()));
        assertEquals(Math.round((double) table.getColumn("Double").get(0)), roundedColumns.getColumn("Double").get(0));
        assertEquals(Math.round((double) table.getColumn("Double").get(1)), roundedColumns.getColumn("Double").get(1));
        assertEquals(Math.round((double) table.getColumn("Double").get(2)), roundedColumns.getColumn("Double").get(2));
        // Cast these cause the DB rounds floats to longs
        assertEquals((long) Math.round((float) table.getColumn("Float").get(0)),
                roundedColumns.getColumn("Float").get(0));
        assertEquals((long) Math.round((float) table.getColumn("Float").get(1)),
                roundedColumns.getColumn("Float").get(1));
        assertEquals((long) Math.round((float) table.getColumn("Float").get(2)),
                roundedColumns.getColumn("Float").get(2));

        // Test whether it works when we specify the columns, by comparing to the validated results from before
        Table specificRoundedColums = TableTools.roundDecimalColumns(table, "Double", "Float");
        assertTrue(Arrays.equals((String[]) roundedColumns.getColumn("String").getDirect(),
                (String[]) specificRoundedColums.getColumn("String").getDirect()));
        assertTrue(Arrays.equals((int[]) roundedColumns.getColumn("Int").getDirect(),
                (int[]) specificRoundedColums.getColumn("Int").getDirect()));
        assertTrue(Arrays.equals((long[]) roundedColumns.getColumn("Double").getDirect(),
                (long[]) specificRoundedColums.getColumn("Double").getDirect()));
        assertTrue(Arrays.equals((long[]) roundedColumns.getColumn("Float").getDirect(),
                (long[]) specificRoundedColums.getColumn("Float").getDirect()));

        // Test whether it works properly when we specify what NOT to round
        Table onlyOneRoundedColumn = TableTools.roundDecimalColumnsExcept(table, "Float");
        assertTrue(Arrays.equals((String[]) roundedColumns.getColumn("String").getDirect(),
                (String[]) onlyOneRoundedColumn.getColumn("String").getDirect()));
        assertTrue(Arrays.equals((int[]) table.getColumn("Int").getDirect(),
                (int[]) onlyOneRoundedColumn.getColumn("Int").getDirect()));
        assertTrue(Arrays.equals((long[]) roundedColumns.getColumn("Double").getDirect(),
                (long[]) onlyOneRoundedColumn.getColumn("Double").getDirect()));
        assertTrue(Arrays.equals((float[]) table.getColumn("Float").getDirect(),
                (float[]) onlyOneRoundedColumn.getColumn("Float").getDirect()));


        try { // Make sure we complain if you try to round the unroundable
            TableTools.roundDecimalColumns(table, "String");
            fail("Expected exception: trying to round a String column");
        } catch (Exception ignored) {
        }
    }

    @Test
    public void testDateTimeColumnHolder() throws Exception {

        // create two columns with the same data
        final DateTime[] data = new DateTime[] {new DateTime(100), new DateTime(100), null};
        final long[] longData =
                new long[] {data[0] == null ? io.deephaven.util.QueryConstants.NULL_LONG : data[0].getNanos(),
                        data[1] == null ? io.deephaven.util.QueryConstants.NULL_LONG : data[1].getNanos(),
                        data[2] == null ? QueryConstants.NULL_LONG : data[2].getNanos()};

        final ColumnHolder dateTimeCol = c("DateTimeColumn", data);
        final ColumnHolder dateTimeCol2 = ColumnHolder.getDateTimeColumnHolder("DateTimeColumn2", false, longData);

        final Table table = TableTools.newTable(dateTimeCol, dateTimeCol2);

        // make sure both columns are in fact DateTime columns
        final Table meta = table.getMeta();
        Assert.assertEquals(DateTime.class.getCanonicalName(), meta.getColumn("DataType").get(0));
        Assert.assertEquals(DateTime.class.getCanonicalName(), meta.getColumn("DataType").get(1));

        // make sure this doesn't crash
        showWithRowSet(table);

        // validate column1 (backed with DateTime objects)
        Assert.assertEquals(data[0], table.getColumn(0).get(0));
        Assert.assertEquals(data[1], table.getColumn(0).get(1));
        Assert.assertEquals(data[2], table.getColumn(0).get(2));

        // validate column2 (backed with longs, but should be get-able as DateTimes as well)
        Assert.assertEquals(data[0], table.getColumn(1).get(0));
        Assert.assertEquals(data[1], table.getColumn(1).get(1));
        Assert.assertEquals(data[2], table.getColumn(1).get(2));
        Assert.assertEquals(longData[0], table.getColumn(1).getLong(0));
        Assert.assertEquals(longData[1], table.getColumn(1).getLong(1));
        Assert.assertEquals(longData[2], table.getColumn(1).getLong(2));
    }

    @Test
    public void testSimpleDiffRegression() {
        final Table expected = emptyTable(1).update("Sym=`AXP`");
        final Table result = emptyTable(1).update("Sym=`BAC`");
        showWithRowSet(expected);
        showWithRowSet(result);
        final String diffInfo = TableTools.diff(result, expected, 1);
        Assert.assertNotEquals(0, diffInfo.length());
    }

    @Test
    public void testMerge2() {
        Random random = new Random(0);
        int size = random.nextInt(10);
        final QueryTable table1 =
                TstUtils.testRefreshingTable(RowSetTstUtils.getRandomRowSet(0, size, random).toTracking(),
                        getRandomStringCol("Sym", size, random),
                        getRandomIntCol("intCol", size, random),
                        getRandomDoubleCol("doubleCol", size, random));
        size = random.nextInt(10);
        final QueryTable table2 =
                TstUtils.testRefreshingTable(RowSetTstUtils.getRandomRowSet(0, size, random).toTracking(),
                        getRandomStringCol("Sym", size, random),
                        getRandomIntCol("intCol", size, random),
                        getRandomDoubleCol("doubleCol", size, random));
        size = random.nextInt(10);
        final QueryTable table3 =
                TstUtils.testRefreshingTable(RowSetTstUtils.getRandomRowSet(0, size, random).toTracking(),
                        getRandomStringCol("Sym", size, random),
                        getRandomIntCol("intCol", size, random),
                        getRandomDoubleCol("doubleCol", size, random));


        Table result = TableTools.merge(table1, table2, table3);
        tableRangesAreEqual(table1, result, 0, 0, table1.size());
        tableRangesAreEqual(table2, result, 0, table1.size(), table2.size());
        tableRangesAreEqual(table3, result, 0, table1.size() + table2.size(), table3.size());
    }

    @Test
    public void testMergeIterative() {
        Random random = new Random(0);
        int size = 3;
        final QueryTable table1 =
                TstUtils.testRefreshingTable(RowSetTstUtils.getRandomRowSet(0, size, random).toTracking(),
                        getRandomStringCol("Sym", size, random),
                        getRandomIntCol("intCol", size, random),
                        getRandomDoubleCol("doubleCol", size, random));
        size = 3;
        final QueryTable table2 =
                TstUtils.testRefreshingTable(RowSetTstUtils.getRandomRowSet(0, size, random).toTracking(),
                        getRandomStringCol("Sym", size, random),
                        getRandomIntCol("intCol", size, random),
                        getRandomDoubleCol("doubleCol", size, random));
        size = 3;
        final QueryTable table3 =
                TstUtils.testRefreshingTable(RowSetTstUtils.getRandomRowSet(0, size, random).toTracking(),
                        getRandomStringCol("Sym", size, random),
                        getRandomIntCol("intCol", size, random),
                        getRandomDoubleCol("doubleCol", size, random));
        size = 50;
        final QueryTable staticTable = TstUtils.testTable(RowSetTstUtils.getRandomRowSet(0, size, random).toTracking(),
                getRandomStringCol("Sym", size, random),
                getRandomIntCol("intCol", size, random),
                getRandomDoubleCol("doubleCol", size, random));

        EvalNugget en[] = new EvalNugget[] {
                new EvalNugget("Single Table Merge") {
                    protected Table e() {
                        return TableTools.merge(table1);
                    }
                },
                new EvalNuggetSet("Merge No Sort") {
                    protected Table e() {
                        return TableTools.merge(
                                table1.updateView("lk=k"), staticTable.updateView("lk=k+100000000L"),
                                table2.updateView("lk=k+200000000L"), table3.updateView("lk=k+300000000L"));
                    }
                },
                new EvalNuggetSet("Merge Plus Sort") {
                    protected Table e() {
                        return TableTools.merge(
                                table1.updateView("lk=k"), staticTable.updateView("lk=k+100000000L"),
                                table2.updateView("lk=k+200000000L"), table3.updateView("lk=k+300000000L")).sort("lk");
                    }
                },
                new EvalNuggetSet("Double Merge Plus Sort") {
                    protected Table e() {
                        return TableTools.merge(
                                table1.updateView("lk=k"), staticTable.updateView("lk=k+100000000L"),
                                table2.updateView("lk=k+200000000L"), table3.updateView("lk=k+300000000L"),
                                table3.updateView("lk=k+400000000L")).sort("lk");
                    }
                },
                new EvalNuggetSet("Triple Double Merge Plus Sort") {
                    protected Table e() {
                        return TableTools.merge(
                                table1.updateView("lk=k"), table1.updateView("lk=k+100000000L"),
                                staticTable.updateView("lk=k+200000000L"), staticTable.updateView("lk=k+300000000L"),
                                table2.updateView("lk=k+400000000L"), table2.updateView("lk=k+500000000L"),
                                table3.updateView("lk=k+600000000L"), table3.updateView("lk=k+700000000L")).sort("lk");
                    }
                },
                EvalNugget.from(() -> TableTools
                        .merge(TableTools.emptyTable(10), table1.dropColumns("Sym", "intCol", "doubleCol"))
                        .update("A=1"))
        };

        for (int i = 0; i < 20; i++) {
            System.out.println("Step = " + i);
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> addRows(random, table1));
            TstUtils.validate(en);

            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> addRows(random, table2));
            TstUtils.validate(en);

            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> addRows(random, table3));
            TstUtils.validate(en);

            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> addRows(random, table1));

            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> addRows(random, table2));

            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> addRows(random, table3));

            TstUtils.validate(en);
        }
    }

    @Test
    public void testMergeIterative2() {
        LogicalClock clock = LogicalClock.DEFAULT;
        Random random = new Random(0);

        TstUtils.ColumnInfo[] info1;
        final QueryTable table1 = getTable(random.nextInt(20), random,
                info1 = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new StringGenerator(),
                        new IntGenerator(10, 100),
                        new DoubleGenerator(0, 100)));

        ColumnInfo[] info2;
        final QueryTable table2 = getTable(random.nextInt(10), random,
                info2 = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new StringGenerator(),
                        new IntGenerator(10, 100),
                        new DoubleGenerator(0, 100)));

        ColumnInfo[] info3;
        final int size = random.nextInt(40);
        final QueryTable table3 = getTable(size, random,
                info3 = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new StringGenerator(),
                        new IntGenerator(10, 100),
                        new DoubleGenerator(0, 100)));

        EvalNugget en[] = new EvalNugget[] {
                new EvalNugget("Single table merge") {
                    protected Table e() {
                        return TableTools.merge(table1);
                    }
                },
                new EvalNuggetSet("Merge 3") {
                    protected Table e() {
                        return TableTools.merge(table1.updateView("lk=k"),
                                table2.updateView("lk=k+100000000L"), table3.updateView("lk=k+200000000L"));
                    }
                },
                new EvalNuggetSet("Merge Plus Sort") {
                    protected Table e() {
                        return TableTools.merge(table1.updateView("lk=k"),
                                table2.updateView("lk=k+100000000L"), table3.updateView("lk=k+200000000L")).sort("lk");
                    }
                },
                new EvalNuggetSet("Double Merge 3") {
                    protected Table e() {
                        return TableTools.merge(table1.updateView("lk=k"),
                                table2.updateView("lk=k+100000000L"), table3.updateView("lk=k+200000000L"),
                                table1.updateView("lk=k+300000000L"), table2.updateView("lk=k+400000000L"),
                                table3.updateView("lk=k+500000000L"));
                    }
                },
                new EvalNuggetSet("Merge With Views") {
                    protected Table e() {
                        // noinspection ConstantConditions
                        return TableTools.merge(
                                TableTools.merge(table1.updateView("lk=k"), table2.updateView("lk=k+100000000L"))
                                        .view("Sym", "intCol", "lk"),
                                table3.updateView("lk=k+200000000L").view("Sym", "intCol", "lk"));
                    }
                },
        }; // TODO add a new comparison tool that matches rows by key and allows for random order

        try {
            for (int i = 0; i < 100; i++) {
                System.out.println("Step = " + i);

                // Each table has a 50/50 chance of getting modified on this step
                boolean mod1 = random.nextBoolean();
                boolean mod2 = random.nextBoolean();
                boolean mod3 = random.nextBoolean();

                if (mod1) {
                    UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
                            () -> GenerateTableUpdates.generateTableUpdates(size, random, table1, info1));
                } else {
                    clock.startUpdateCycle();
                    clock.completeUpdateCycle();
                }

                if (mod2) {
                    UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
                            () -> GenerateTableUpdates.generateTableUpdates(size, random, table2, info2));
                } else {
                    clock.startUpdateCycle();
                    clock.completeUpdateCycle();
                }

                if (mod3) {
                    UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
                            () -> GenerateTableUpdates.generateTableUpdates(size, random, table3, info3));
                } else {
                    clock.startUpdateCycle();
                    clock.completeUpdateCycle();
                }

                TstUtils.validate(en);
            }
        } catch (Exception e) {
            TestCase.fail(e.getMessage());
        }
    }


    // This merge should work out nicely, we'll end up collapsing it into a single broad merge.
    @Test
    public void testMergeRecursive() {
        Table result = null;

        for (int ii = 0; ii < 250; ++ii) {
            System.out.println("Testing merge " + ii);

            if (result == null)
                result = table1;
            else
                result = TableTools.merge(result, table1);

            Assert.assertEquals(table1.size() * (ii + 1), result.size());

            for (int jj = 0; jj <= ii; ++jj)
                tableRangesAreEqual(table1, result, 0, table1.size() * jj, table1.size());
        }
    }

    // This test does a merge, followed by a view, then another merge.
    @Test
    public void testMergeRecursive2() {
        Table merge1 =
                TableTools.merge(table1, table2.renameColumns("GroupedInts=GroupedInts1", "StringKeys=StringKeys1"))
                        .view("StringKeys");
        Table merge2 = TableTools.merge(merge1, table1.view("StringKeys"));

        Assert.assertEquals(table1.size() * 2 + table2.size(), merge2.size());

        tableRangesAreEqual(table1.view("StringKeys"), merge1, 0, 0, table1.size());
        tableRangesAreEqual(table1.view("StringKeys"), merge2, 0, 0, table1.size());

        tableRangesAreEqual(table2.view("StringKeys=StringKeys1"), merge1, 0, table1.size(), table2.size());
        tableRangesAreEqual(table2.view("StringKeys=StringKeys1"), merge2, 0, table1.size(), table2.size());

        tableRangesAreEqual(table1.view("StringKeys"), merge2, 0, table1.size() + table2.size(), table1.size());
    }

    @Test
    public void testUncollapsableMerge() {
        final int numRecursions = 128;

        Table result = null;
        for (int ii = 0; ii < numRecursions; ++ii) {
            System.out.println("Testing merge " + ii);

            if (result == null)
                result = table1;
            else
                result = TableTools.merge(result, table1).updateView("GroupedInts=GroupedInts+1")
                        .updateView("GroupedInts=GroupedInts-1");

            Assert.assertEquals(table1.size() * (ii + 1), result.size());
        }

        for (int jj = 0; jj < numRecursions; ++jj) {
            tableRangesAreEqual(table1, result, 0, table1.size() * jj, table1.size());
        }
    }

    @Test
    public void testMergeWithNestedShift() {
        // Test that an outer shift properly shifts RowSet when inner shifts are also propagated to the RowSet.
        final QueryTable table = testRefreshingTable(i(1).toTracking(), c("Sentinel", 1));
        // must be uncollapsable s.t. inner table shifts at the same time as outer table
        final Table m2 = TableTools.merge(table, table).updateView("Sentinel=Sentinel+1");
        final Table result = TableTools.merge(table, m2);

        // Select a prime that guarantees shifts from the merge operations.
        final int PRIME = 61409;
        Assert.assertTrue(2 * PRIME > UnionRedirection.ALLOCATION_UNIT_ROW_KEYS);

        for (int ii = 1; ii < 10; ++ii) {
            final int fii = PRIME * ii;
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                addToTable(table, i(fii), c("Sentinel", fii));
                table.notifyListeners(i(fii), i(), i());
            });
        }

        TableTools.show(result, 100);
    }

    @Test
    public void testMergeWithShiftBoundary() {
        // Test that an outer shift properly shifts RowSet when inner shifts are also propagated to the RowSet.
        final int ONE_MILLION = 1024 * 1024;
        final QueryTable table = testRefreshingTable(i(ONE_MILLION - 1).toTracking(), c("Sentinel", 1));
        final QueryTable table2 = testRefreshingTable(i(0).toTracking(), c("Sentinel", 2));
        final Table result = TableTools.merge(table, table2);

        showWithRowSet(result);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(ONE_MILLION - 11), c("Sentinel", 1));
            removeRows(table, i(ONE_MILLION - 1));
            final TableUpdateImpl update = new TableUpdateImpl();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            update.added = i();
            update.removed = i();
            update.modified = i();
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(ONE_MILLION - 4096, ONE_MILLION - 1, -10);
            update.shifted = builder.build();
            table.notifyListeners(update);
        });

        showWithRowSet(result);
    }

    @Test
    public void testMergeShiftsEmptyTable() {
        // Test that an outer shift properly shifts RowSet when inner shifts are also propagated to the RowSet.
        final QueryTable table = testRefreshingTable(i(1).toTracking(), c("Sentinel", 1));
        final QueryTable emptyTable = testRefreshingTable(i().toTracking(), TstUtils.<Integer>c("Sentinel"));
        final Table m2 = TableTools.merge(table, emptyTable, emptyTable).updateView("Sentinel=Sentinel+1");

        final EvalNugget[] ev = new EvalNugget[] {
                EvalNugget.from(() -> table),
                EvalNugget.from(() -> TableTools.merge(table, emptyTable, table, emptyTable)),
                EvalNugget.from(() -> m2),
                EvalNugget.from(() -> TableTools.merge(m2, m2)),
        };

        // Select a prime that guarantees shifts from the merge operations.
        final int PRIME = 61409;
        Assert.assertTrue(2 * PRIME > UnionRedirection.ALLOCATION_UNIT_ROW_KEYS);

        for (int ii = 1; ii < 10; ++ii) {
            final int fii = 2 * PRIME * ii + 1;
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                final long currKey = table.getRowSet().lastRowKey();
                removeRows(table, i(currKey));
                addToTable(table, i(fii), c("Sentinel", 1));

                TableUpdateImpl update = new TableUpdateImpl();
                update.added = i(fii);
                update.removed = i(currKey);
                update.modified = i();
                update.shifted = RowSetShiftData.EMPTY;
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                table.notifyListeners(update);
            });
            validate(ev);
        }
    }

    @Test
    public void testMergeShiftBoundary() {
        // DH-11032
        // Test that when our inner table has a shift that is begins beyond the last key for our subtable (because
        // it has been filtered and the reserved address space is less than the address space of the full unfiltered
        // table) we do not remove elements that should not be removed. This is distilled from a broken fuzzer test.
        final QueryTable table1 = testRefreshingTable(i(10000, 65538).toTracking(), c("Sentinel", 1, 2));
        final QueryTable table2 = testRefreshingTable(i(2).toTracking(), c("Sentinel", 3));
        final Table table1Filtered = table1.where("Sentinel == 1");
        final Table m2 = TableTools.merge(table1Filtered, table2);

        showWithRowSet(m2);

        final Table expected = TableTools.newTable(intCol("Sentinel", 1, 3));
        assertTableEquals(expected, m2);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            removeRows(table1, i(65538));
            addToTable(table1, i(65537), c("Sentinel", 2));

            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
            shiftBuilder.shiftRange(65538, 65539, +1);

            TableUpdateImpl update = new TableUpdateImpl();
            update.added = i();
            update.removed = i();
            update.modified = i();
            update.shifted = shiftBuilder.build();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            table1.notifyListeners(update);
        });

        showWithRowSet(m2);
        assertTableEquals(expected, m2);
    }

    @Test
    public void testMergeDeepShifts() {
        // Test that an outer shift properly shifts RowSet when inner shifts are also propagated to the RowSet.
        final QueryTable table = testRefreshingTable(i(1).toTracking(), c("Sentinel", 1));
        final QueryTable emptyTable = testRefreshingTable(i().toTracking(), TstUtils.<Integer>c("Sentinel"));
        final Table m2 = TableTools.merge(table, emptyTable, emptyTable, emptyTable, emptyTable, emptyTable)
                .updateView("Sentinel=Sentinel+1");

        final EvalNugget[] ev = new EvalNugget[] {
                EvalNugget.from(() -> table),
                EvalNugget.from(() -> TableTools.merge(table, emptyTable, table, emptyTable)),
                EvalNugget
                        .from(() -> TableTools.merge(table, emptyTable, emptyTable, emptyTable, emptyTable, emptyTable)
                                .updateView("Sentinel=Sentinel+1")),
                EvalNugget.from(() -> TableTools.merge(m2, m2)),
        };

        // Select a prime that guarantees shifts from the merge operations.
        final int SHIFT_SIZE = 4 * 61409;
        Assert.assertTrue(SHIFT_SIZE > UnionRedirection.ALLOCATION_UNIT_ROW_KEYS);

        for (int ii = 1; ii < 10; ++ii) {
            final int fii = SHIFT_SIZE * ii + 1;
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                final long currKey = table.getRowSet().lastRowKey();
                // Manually apply shift.
                removeRows(table, i(currKey));
                addToTable(table, i(fii), c("Sentinel", 1));

                TableUpdateImpl update = new TableUpdateImpl();
                update.added = i();
                update.removed = i();
                update.modified = i();
                final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
                builder.shiftRange(0, currKey, SHIFT_SIZE);
                update.shifted = builder.build();
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                table.notifyListeners(update);
            });
            validate(ev);
        }
    }

    private void addRows(Random random, QueryTable table1) {
        int size;
        size = random.nextInt(10);
        final RowSet newRowSet = RowSetTstUtils.getRandomRowSet(table1.getRowSet().lastRowKey(), size, random);
        TstUtils.addToTable(table1, newRowSet, getRandomStringCol("Sym", size, random),
                getRandomIntCol("intCol", size, random),
                getRandomDoubleCol("doubleCol", size, random));
        table1.notifyListeners(newRowSet, TstUtils.i(), TstUtils.i());
    }

    public static void tableRangesAreEqual(Table table1, Table table2, long from1, long from2, long size) {
        Assert.assertEquals("",
                TableTools.diff(table1.tail(table1.size() - from1).head(size),
                        table2.tail(table2.size() - from2).head(size), 10));
    }

    @Test
    public void testMergeWithEmptyTables() {
        Table emptyLikeTable1 = TableTools.newTable(table1.getDefinition());
        Table result = TableTools.merge(table1, emptyLikeTable1);
        tableRangesAreEqual(table1, result, 0, 0, table1.size());
        result = TableTools.merge(TableTools.newTable(table1.getDefinition()), table1);
        tableRangesAreEqual(table1, result, 0, 0, table1.size());
        result = TableTools.merge(TableTools.newTable(table1.getDefinition()), emptyLikeTable1, emptyLikeTable1);
        TestCase.assertEquals(0, result.size());
    }

    @Test
    public void testMergeSorted() throws IOException {
        Table table1 = testTable(i(1, 3, 5, 6, 7).toTracking(), c("Key", "a", "c", "d", "e", "f"))
                .updateView("Sentinel=k");
        Table table2 = testTable(i(2, 4, 8, 9).toTracking(), c("Key", "b", "c", "g", "h"))
                .updateView("Sentinel=k");
        Table merged = TableTools.mergeSorted("Key", table1, table2);
        showWithRowSet(merged);

        // noinspection ConstantConditions
        Table standardWay = TableTools.merge(table1, table2).sort("Key");

        String diff = TableTools.diff(merged, standardWay, 10);
        TestCase.assertEquals("", diff);
    }

    @Test
    public void testMergeSorted2() throws IOException {
        Random random = new Random(42);
        List<Table> tables = new ArrayList<>();

        int size = 50;

        for (int ii = 0; ii < 10; ++ii) {
            final QueryTable table = getTable(false, size, random, initColumnInfos(new String[] {"Key", "doubleCol"},
                    new SortedIntGenerator(0, 100),
                    new DoubleGenerator(0, 100)));
            tables.add(table.update("TableI=" + ii));
        }

        Table merged = TableTools.mergeSorted("Key", tables);
        showWithRowSet(merged);

        Table standardWay = TableTools.merge(tables).sort("Key");

        String diff = TableTools.diff(merged, standardWay, 10);
        TestCase.assertEquals("", diff);
    }

    @Test
    public void testMergeGetChunk() {
        final QueryTable table = testRefreshingTable(i(1).toTracking(), c("Sentinel", 1));
        final Table m2 = TableTools.merge(table, table).updateView("Sentinel=Sentinel+1");
        final QueryTable result = (QueryTable) TableTools.merge(table, m2);

        // Select a prime that guarantees shifts from the merge operations.
        final int PRIME = 61409;
        Assert.assertTrue(2 * PRIME > UnionRedirection.ALLOCATION_UNIT_ROW_KEYS);

        final Consumer<Boolean> validate = (usePrev) -> {
            final RowSet origRowSet = usePrev ? table.getRowSet().copyPrev() : table.getRowSet();
            final RowSet resRowSet = usePrev ? result.getRowSet().copyPrev() : result.getRowSet();
            final int numElements = origRowSet.intSize();

            // noinspection unchecked
            final ColumnSource<Integer> origCol = table.getColumnSource("Sentinel");
            final ColumnSource.GetContext origContext = origCol.makeGetContext(numElements);
            final IntChunk<? extends Values> origContent = usePrev
                    ? origCol.getPrevChunk(origContext, origRowSet).asIntChunk()
                    : origCol.getChunk(origContext, origRowSet).asIntChunk();

            // noinspection unchecked
            final ColumnSource<Integer> resCol = result.getColumnSource("Sentinel");
            final ColumnSource.GetContext resContext = resCol.makeGetContext(numElements * 3);
            final IntChunk<? extends Values> resContent = usePrev
                    ? resCol.getPrevChunk(resContext, resRowSet).asIntChunk()
                    : resCol.getChunk(resContext, resRowSet).asIntChunk();

            Assert.assertEquals(numElements, origContent.size());
            Assert.assertEquals(3 * numElements, resContent.size());

            for (int ii = 0; ii < numElements; ++ii) {
                Assert.assertEquals(origContent.get(ii), resContent.get(ii));
                Assert.assertEquals(origContent.get(ii), resContent.get(ii + numElements) - 1);
                Assert.assertEquals(origContent.get(ii), resContent.get(ii + 2 * numElements) - 1);
            }
        };

        result.listenForUpdates(new InstrumentedTableUpdateListener("") {
            @Override
            public void onUpdate(final TableUpdate upstream) {
                Assert.assertTrue(table.getRowSet().intSize() > table.getRowSet().intSizePrev());
                validate.accept(false);
                validate.accept(true);
            }

            @Override
            protected void onFailureInternal(Throwable originalException, Entry sourceEntry) {}
        });

        for (int ii = 1; ii < 100; ++ii) {
            final int fii = PRIME * ii;
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                addToTable(table, i(fii), c("Sentinel", fii));
                table.notifyListeners(i(fii), i(), i());
            });
        }
    }

    @Test
    public void testMergeGetChunkEmpty() {
        final QueryTable table = testRefreshingTable(i(1).toTracking(), c("Sentinel", 1));
        final Table m2 = TableTools.merge(table, table).updateView("Sentinel=Sentinel+1");
        final QueryTable result = (QueryTable) TableTools.merge(table, m2);

        final Consumer<Boolean> validate = (usePrev) -> {
            final RowSet rowSet = RowSetFactory.empty();
            final int numElements = 1024;

            // noinspection unchecked
            final ColumnSource<Integer> origCol = table.getColumnSource("Sentinel");
            final ColumnSource.GetContext origContext = origCol.makeGetContext(numElements);
            final IntChunk<? extends Values> origContent = usePrev
                    ? origCol.getPrevChunk(origContext, rowSet).asIntChunk()
                    : origCol.getChunk(origContext, rowSet).asIntChunk();

            // noinspection unchecked
            final ColumnSource<Integer> resCol = result.getColumnSource("Sentinel");
            final ColumnSource.GetContext resContext = resCol.makeGetContext(numElements * 3);
            final IntChunk<? extends Values> resContent = usePrev
                    ? resCol.getPrevChunk(resContext, rowSet).asIntChunk()
                    : resCol.getChunk(resContext, rowSet).asIntChunk();

            Assert.assertEquals(0, origContent.size());
            Assert.assertEquals(0, resContent.size());
        };

        validate.accept(false);
        validate.accept(true);
    }

    @Test
    public void testEmptyTable() throws IOException {
        Table emptyTable = TableTools.emptyTable(2);
        TestCase.assertEquals(2, emptyTable.size());

        Table emptyTable2 = TableTools.emptyTable(2).update("col=1");
        TestCase.assertEquals(2, emptyTable2.size());
        DataColumn dataColumn = emptyTable2.getColumn("col");
        TestCase.assertEquals(2, dataColumn.size());
        TestCase.assertEquals(1, dataColumn.get(0));
        TestCase.assertEquals(1, dataColumn.get(1));

        TableTools.show(emptyTable2);

        Table emptyTable3 = TableTools.emptyTable(2).updateView("col=1");
        TestCase.assertEquals(2, emptyTable3.size());
        dataColumn = emptyTable3.getColumn("col");
        TestCase.assertEquals(2, dataColumn.size());
        TestCase.assertEquals(1, dataColumn.get(0));
        TestCase.assertEquals(1, dataColumn.get(1));

        TableTools.show(emptyTable3);
    }

    @Test
    public void testMergeIndexShiftingPerformance() {
        final QueryTable testRefreshingTable =
                TstUtils.testRefreshingTable(i(0).toTracking(), intCol("IntCol", 0), charCol("CharCol", 'a'));

        final Table joined = testRefreshingTable.view("CharCol").join(testRefreshingTable, "CharCol", "IntCol");
        final PartitionedTable partitionedTable = joined.partitionBy("IntCol");
        final Table merged = partitionedTable.merge();

        final long start = System.currentTimeMillis();
        long stepStart = start;

        for (int step = 0; step < 150; ++step) {
            final int stepSize = 20;
            final int firstNextIdx = (step * stepSize) + 1;
            final int lastNextIdx = ((step + 1) * stepSize);
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                final RowSet addRowSet = RowSetFactory.fromRange(firstNextIdx, lastNextIdx);

                final int[] addInts = new int[stepSize];
                final char[] addChars = new char[stepSize];

                for (int ii = 0; ii < stepSize; ++ii) {
                    addInts[ii] = firstNextIdx + ii;
                    addChars[ii] = (char) ('a' + ((firstNextIdx + ii) % 26));
                }

                addToTable(testRefreshingTable, addRowSet, intCol("IntCol", addInts), charCol("CharCol", addChars));
                testRefreshingTable.notifyListeners(addRowSet, i(), i());
            });

            final long end = System.currentTimeMillis();
            final long stepDuration = end - stepStart;
            final long duration = end - start;
            stepStart = end;
            System.out.println("Step=" + step + ", duration=" + duration + "ms, stepDuration=" + stepDuration + "ms");
            if (duration > 30_000) {
                TestCase.fail(
                        "This test is expected to take around 5 seconds on a Mac with the new shift behavior, something is not right.");
            }
        }

        final Table check = joined.sort("IntCol");
        final Table mergeSort = merged.sort("IntCol");
        assertTableEquals(check, mergeSort);
    }
}
