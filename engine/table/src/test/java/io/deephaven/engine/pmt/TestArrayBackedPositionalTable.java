//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.pmt;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.engine.context.QueryCompilerImpl;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateValidator;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.impl.BaseUpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.PrintListener;
import io.deephaven.engine.util.TableTools;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.deephaven.api.agg.Aggregation.AggLast;
import static io.deephaven.api.agg.Aggregation.AggSum;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_INT;


public class TestArrayBackedPositionalTable {

    /**
     * Reference time used throughout tests.
     */
    private static final String BASE_TIME_STRING = "2025-01-01T12:00:00Z";

    final TableDefinition testDefinition = TableDefinition.from(Arrays.asList(
            ColumnHeader.of("Sentinel", int.class),
            ColumnHeader.of("Value", String.class),
            ColumnHeader.of("Dub", double.class)));

    final TableDefinition positionsDefinition = TableDefinition.from(Arrays.asList(
            ColumnHeader.of("Group", String.class),
            ColumnHeader.of("Symbol", String.class),
            ColumnHeader.of("Quantity", int.class)));

    final TableDefinition priceDefinition = TableDefinition.from(Arrays.asList(
            ColumnHeader.of("Symbol", String.class),
            ColumnHeader.of("Bid", double.class),
            ColumnHeader.of("Ask", double.class),
            ColumnHeader.of("Time", Instant.class)));

    ExecutionContext executionContext = null;
    SafeCloseable contextCloseable = null;
    PeriodicUpdateGraph updateGraph = null;

    @BeforeClass
    public static void setupGlobal() {
        AsyncClientErrorNotifier.setReporter(t -> {
            t.printStackTrace(System.err);
            TestCase.fail(t.getMessage());
        });
    }

    @Before
    public void setup() {
        // TODO: ?? can't reuse graph name?
        // updateGraph = PeriodicUpdateGraph.newBuilder("testUpdateGraph").build();
        updateGraph = PeriodicUpdateGraph.newBuilder("testUpdateGraph").existingOrBuild();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);
        try {
            executionContext = ExecutionContext.newBuilder()
                    .newQueryScope()
                    .newQueryLibrary()
                    .setUpdateGraph(updateGraph)
                    .setQueryCompiler(QueryCompilerImpl.create(
                            Files.createTempDirectory("CommandLineDriver").resolve("cache").toFile(),
                            ClassLoader.getSystemClassLoader()))
                    .setOperationInitializer(OperationInitializer.NON_PARALLELIZABLE)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // noinspection resource
        contextCloseable = executionContext.open();
    }

    @After
    public void cleanup() {
        updateGraph.resetForUnitTests(true);
        contextCloseable.close();

        // TODO: ??? test hangs if I have this here. but if I don't, then I get 'duplicate updategraph' exceptions
        BaseUpdateGraph.removeInstance(updateGraph.getName());
    }

    @Test
    public void testSimple() throws InterruptedException, ExecutionException {

        updateGraph.sharedLock().lock();
        final ArrayBackedPositionalMutableTable table;
        final InstrumentedTableUpdateListenerAdapter validatorListener;
        final PrintListener printListener;
        try (final SafeCloseable closeable = () -> updateGraph.sharedLock().unlock()) {
            table = new ArrayBackedPositionalMutableTable(testDefinition);
            final TableUpdateValidator tuv = TableUpdateValidator.make(table);
            final QueryTable validatorResult = tuv.getResultTable();
            validatorListener =
                    new InstrumentedTableUpdateListenerAdapter("Validator Result Listener", validatorResult, false) {
                        @Override
                        public void onUpdate(TableUpdate upstream) {
                            // uninteresting, we just want the failure behavior
                        }
                    };
            validatorResult.addUpdateListener(validatorListener);
            printListener = new PrintListener("table", table);
        }

        table.startBundle();
        table.addRow(0, 2);
        table.setCell(0, 0, 6);
        table.setCell(0, 1, "Gilbert");
        table.setCell(0, 2, Math.E);
        table.setCell(1, 0, 8);
        table.setCell(1, 1, "Sullivan");
        table.setCell(1, 2, Math.PI);
        table.endBundle();

        updateGraph.runWithinUnitTestCycle(table::run);
        TableTools.show(table);

        final Table expected = TableTools.newTable(intCol("Sentinel", 6, 8),
                stringCol("Value", "Gilbert", "Sullivan"),
                doubleCol("Dub", Math.E, Math.PI));

        TestCase.assertEquals("", TableTools.diff(table, expected, 10));

        table.startBundle();
        table.addRow(1, 1);
        table.setCell(1, 0, 7);
        table.setCell(1, 1, "Miranda");
        table.endBundle();

        updateGraph.runWithinUnitTestCycle(table::run);

        TableTools.show(table);

        final Table newRow = TableTools.newTable(intCol("Sentinel", 7),
                stringCol("Value", "Miranda"),
                doubleCol("Dub", QueryConstants.NULL_DOUBLE));

        TestCase.assertEquals("",
                TableTools.diff(table, TableTools.merge(expected.head(1), newRow, expected.tail(1)), 10));

        table.startBundle();
        table.deleteRow(0, 2);
        table.endBundle();

        updateGraph.runWithinUnitTestCycle(table::run);

        TestCase.assertEquals("", TableTools.diff(table, expected.tail(1), 10));
    }

    @Test
    public void testDelete1() {
        final ArrayBackedPositionalMutableTable table = makeDefaultDef();

        final PrintListener listener = new PrintListener("table", table, 10);

        final TableUpdateValidator tuv = TableUpdateValidator.make(table);
        final PrintListener tuvListener = new PrintListener("tuv", tuv.getResultTable());

        initTable(table, 6);

        updateGraph.runWithinUnitTestCycle(table::run);

        table.startBundle();
        table.deleteRow(0, 1);
        table.deleteRow(2, 1);
        table.endBundle();

        updateGraph.runWithinUnitTestCycle(table::run);

        TstUtils.assertTableEquals(getExpectedBase(6).where("Value not in 0, 30"), table);

        TestCase.assertFalse(tuv.getResultTable().isFailed());
    }

    @Test
    public void testDeleteAtEnd() {
        testDeleteSimple(4, 2, "Value not in 40, 50");
        testDeleteSimple(5, 1, "Value not in 50");
    }

    @Test
    public void testDeleteAtStart() {
        testDeleteSimple(0, 2, "Value not in 0, 10");
        testDeleteSimple(0, 1, "Value not in 0");
        testDeleteSimple(0, 6, "false");
    }

    public void testDeleteSimple(int start, int end, String filter) {
        final ArrayBackedPositionalMutableTable table = makeDefaultDef();

        final PrintListener listener = new PrintListener("table", table, 10);

        final TableUpdateValidator tuv = TableUpdateValidator.make(table);
        final PrintListener tuvListener = new PrintListener("tuv", tuv.getResultTable());

        initTable(table, 6);

        updateGraph.runWithinUnitTestCycle(table::run);

        table.startBundle();
        table.deleteRow(start, end);
        table.endBundle();

        updateGraph.runWithinUnitTestCycle(table::run);

        TstUtils.assertTableEquals(getExpectedBase(6).where(filter), table);

        TestCase.assertFalse(tuv.getResultTable().isFailed());
    }

    @Test
    public void testDelete2a() {
        test2(true);
    }

    @Test
    public void testDelete2b() {
        test2(false);
    }

    public void test2(boolean case1) {
        final ArrayBackedPositionalMutableTable table = makeDefaultDef();

        final PrintListener listener = new PrintListener("table", table, 10);

        final TableUpdateValidator tuv = TableUpdateValidator.make(table);
        final PrintListener tuvListener = new PrintListener("tuv", tuv.getResultTable());


        final int tableSize = 7;

        initTable(table, tableSize);

        // I can consistently replicate the error by deleting rows 2 and then 4, or 5 and then 2 (in both cases the same
        // two rows as row 5 becomes row 4 after deleting row 2).

        table.startBundle();
        if (case1) {
            table.deleteRow(2, 1);
            table.deleteRow(4, 1);
        } else {
            table.deleteRow(5, 1);
            table.deleteRow(2, 1);
        }
        table.endBundle();

        updateGraph.runWithinUnitTestCycle(table::run);

        TstUtils.assertTableEquals(getExpectedBase(tableSize).where("Value not in 20, 50"), table);

        TestCase.assertFalse(tuv.getResultTable().isFailed());
    }

    private static Table getExpectedBase(int tableSize) {
        return TableTools.emptyTable(tableSize).update("Value=i*10", "SV=Integer.toString(Value)",
                "Time=Instant.parse(`" + BASE_TIME_STRING + "`).plusMillis(Value)");
    }

    @NotNull
    private static ArrayBackedPositionalMutableTable makeDefaultDef() {
        final TableDefinition tableDefinition = TableDefinition.from(Arrays.asList(
                ColumnHeader.of("Value", int.class),
                ColumnHeader.of("SV", String.class),
                ColumnHeader.of("Time", Instant.class)));
        final ArrayBackedPositionalMutableTable table = new ArrayBackedPositionalMutableTable(tableDefinition);
        return table;
    }

    private void initTable(ArrayBackedPositionalMutableTable table, int tableSize) {
        table.startBundle();
        table.addRow(0, tableSize);

        final Instant baseTime = Instant.parse(BASE_TIME_STRING);

        for (int ii = 0; ii < tableSize; ++ii) {
            table.setCell(ii, 0, 10 * ii);
            table.setCell(ii, 1, Integer.toString(10 * ii));
            table.setCell(ii, 2, baseTime.plusMillis(10L * ii));
        }
        table.endBundle();

        updateGraph.runWithinUnitTestCycle(table::run);
    }

    @Test
    public void testInsert() {
        testInsert(true);
        testInsert(false);
    }

    private void testInsert(final boolean allInOne) {
        final ArrayBackedPositionalMutableTable table = makeDefaultDef();

        final PrintListener listener = new PrintListener("table", table, 10);

        final TableUpdateValidator tuv = TableUpdateValidator.make(table);
        final PrintListener tuvListener = new PrintListener("tuv", tuv.getResultTable());

        initTable(table, 6);

        TstUtils.assertTableEquals(getExpectedBase(6), table);

        table.startBundle();
        table.addRow(3, 2);

        if (!allInOne) {
            table.endBundle();
            updateGraph.runWithinUnitTestCycle(table::run);
            TstUtils.assertTableEquals(
                    TableTools.newTable(intCol("Value", 0, 10, 20, NULL_INT, NULL_INT, 30, 40, 50)).update(
                            "SV=isNull(Value) ? null : Integer.toString(Value)",
                            "Time=isNull(Value) ? null : Instant.parse(`" + BASE_TIME_STRING + "`).plusMillis(Value)"),
                    table);

            table.startBundle();
        }

        table.setCell(3, 0, 100);
        table.setCell(4, 0, 200);
        table.setCell(3, 1, "100");
        table.setCell(4, 1, "200");
        table.setCell(3, 2, Instant.parse("2025-01-01T12:00:00.100Z"));
        table.setCell(4, 2, Instant.parse("2025-01-01T12:00:00.200Z"));
        table.endBundle();

        updateGraph.runWithinUnitTestCycle(table::run);

        TableTools.showWithRowSet(table);

        TestCase.assertFalse(tuv.getResultTable().isFailed());

        TstUtils.assertTableEquals(
                TableTools.newTable(intCol("Value", 0, 10, 20, 100, 200, 30, 40, 50))
                        .update(
                                "SV=Integer.toString(Value)",
                                "Time=Instant.parse(`" + BASE_TIME_STRING + "`).plusMillis(Value)"),
                table);
    }

    @Test
    public void testAddAtEnd() {
        testAddAtEnd(true);
        testAddAtEnd(false);
    }

    private void testAddAtEnd(final boolean allInOne) {
        final ArrayBackedPositionalMutableTable table = makeDefaultDef();

        final PrintListener listener = new PrintListener("table", table, 10);

        final TableUpdateValidator tuv = TableUpdateValidator.make(table);
        final PrintListener tuvListener = new PrintListener("tuv", tuv.getResultTable());

        initTable(table, 6);

        TstUtils.assertTableEquals(getExpectedBase(6), table);

        table.startBundle();
        table.addRow(6, 1);
        table.setCell(6, 0, 60);
        table.setCell(6, 1, "60");
        table.setCell(6, 2, Instant.parse("2025-01-01T12:00:00.060Z"));

        if (!allInOne) {
            table.endBundle();
            updateGraph.runWithinUnitTestCycle(table::run);
            TstUtils.assertTableEquals(getExpectedBase(7), table);

            table.startBundle();
        }

        table.addRow(7, 1);

        table.setCell(7, 0, 70);
        table.setCell(7, 1, "70");
        table.setCell(7, 2, Instant.parse("2025-01-01T12:00:00.070Z"));

        table.endBundle();

        updateGraph.runWithinUnitTestCycle(table::run);

        TableTools.showWithRowSet(table);

        TestCase.assertFalse(tuv.getResultTable().isFailed());

        TstUtils.assertTableEquals(getExpectedBase(8), table);
    }

    @Test
    public void testJoiningTables() throws ExecutionException, InterruptedException, TimeoutException {
        final ArrayBackedPositionalMutableTable positions;
        final ArrayBackedPositionalMutableTable prices;
        final Table decorated;
        final PrintListener printListener;

        final Instant baseTime = Instant.parse(BASE_TIME_STRING);

        updateGraph.sharedLock().lock();
        try (final SafeCloseable closeable = () -> updateGraph.sharedLock().unlock()) {
            positions = new ArrayBackedPositionalMutableTable(positionsDefinition);
            prices = new ArrayBackedPositionalMutableTable(priceDefinition);

            decorated = positions.naturalJoin(prices, "Symbol", "Bid,Ask,QuoteTime=Time").update("Mid=(Bid+Ask)/2",
                    "Notional=Mid*Quantity");

            printListener = new PrintListener("Positions Decorated with Prices", decorated);
        }

        final Table aggregated =
                decorated.aggBy(List.of(AggSum("Notional", "Quantity"), AggLast("Mid")), "Symbol", "Group");

        TstUtils.assertTableEquals(
                TableTools.newTable(
                        TableTools.stringCol("Group"),
                        TableTools.stringCol("Symbol"),
                        intCol("Quantity"),
                        TableTools.doubleCol("Bid"),
                        TableTools.doubleCol("Ask"),
                        TableTools.instantCol("QuoteTime"),
                        TableTools.doubleCol("Mid"),
                        TableTools.doubleCol("Notional")),
                decorated);

        TstUtils.assertTableEquals(
                TableTools.newTable(
                        TableTools.stringCol("Symbol"),
                        TableTools.stringCol("Group"),
                        TableTools.doubleCol("Notional"),
                        TableTools.longCol("Quantity"),
                        TableTools.doubleCol("Mid")),
                aggregated);


        positions.startBundle();
        positions.addRow(0, 1);
        positions.setCell(0, 0, "Avengers");
        positions.setCell(0, 1, "SPY");
        positions.setCell(0, 2, 1000);
        Future<Void> endBundleFuture = positions.endBundle();

        updateGraph.runWithinUnitTestCycle(() -> {
            prices.run();
            positions.run();
        });
        endBundleFuture.get(0, TimeUnit.MILLISECONDS);

        // there are a few reasons that this lock matters here
        // first, we are reading data and don't want it to be inconsistent
        // second, we know that our bundles were processed, but those are root nodes
        updateGraph.sharedLock().doLocked(() -> {
            TableTools.show(decorated);
            TableTools.show(aggregated);
        });

        TstUtils.assertTableEquals(
                TableTools.newTable(
                        TableTools.stringCol("Group", "Avengers"),
                        TableTools.stringCol("Symbol", "SPY"),
                        intCol("Quantity", 1000),
                        TableTools.doubleCol("Bid", NULL_DOUBLE),
                        TableTools.doubleCol("Ask", NULL_DOUBLE),
                        TableTools.instantCol("QuoteTime", (Instant) null),
                        TableTools.doubleCol("Mid", NULL_DOUBLE),
                        TableTools.doubleCol("Notional", NULL_DOUBLE)),
                decorated);

        TstUtils.assertTableEquals(
                TableTools.newTable(
                        TableTools.stringCol("Symbol", "SPY"),
                        TableTools.stringCol("Group", "Avengers"),
                        TableTools.doubleCol("Notional", NULL_DOUBLE),
                        TableTools.longCol("Quantity", 1000),
                        TableTools.doubleCol("Mid", NULL_DOUBLE)),
                aggregated);


        prices.startBundle();
        prices.addRow(0, 1);
        prices.setCell(0, 0, "SPY");
        prices.setCell(0, 1, 411.40);
        prices.setCell(0, 2, 411.42);
        prices.setCell(0, 3, baseTime);
        endBundleFuture = prices.endBundle();

        updateGraph.runWithinUnitTestCycle(() -> {
            prices.run();
            positions.run();
        });
        endBundleFuture.get(0, TimeUnit.MILLISECONDS);

        updateGraph.sharedLock().doLocked(() -> {
            TableTools.show(decorated);
            TableTools.show(aggregated);
        });

        final double spyMid = (411.40 + 411.42) / 2d; // need to calc exact; assertTableEquals has no epsilon
        TstUtils.assertTableEquals(
                TableTools.newTable(
                        TableTools.stringCol("Group", "Avengers"),
                        TableTools.stringCol("Symbol", "SPY"),
                        intCol("Quantity", 1000),
                        TableTools.doubleCol("Bid", 411.40),
                        TableTools.doubleCol("Ask", 411.42),
                        TableTools.instantCol("QuoteTime", baseTime),
                        TableTools.doubleCol("Mid", spyMid),
                        TableTools.doubleCol("Notional", spyMid * 1000)),
                decorated);

        TstUtils.assertTableEquals(
                TableTools.newTable(
                        TableTools.stringCol("Symbol", "SPY"),
                        TableTools.stringCol("Group", "Avengers"),
                        TableTools.doubleCol("Notional", spyMid * 1000),
                        TableTools.longCol("Quantity", 1000),
                        TableTools.doubleCol("Mid", spyMid)),
                aggregated);

        positions.startBundle();
        positions.addRow(0, 4);
        positions.set2D(0, TableTools.newTable(
                stringCol("Group", "Avengers", "Justice League", "Avengers", "Valhalla"),
                stringCol("Symbol", "SPY", "AAPL", "AAPL", "AAPL"),
                intCol("Quantity", -1500, 100, 200, 1500)));
        endBundleFuture = positions.endBundle();

        updateGraph.runWithinUnitTestCycle(positions::run);
        endBundleFuture.get(0, TimeUnit.MILLISECONDS);

        TstUtils.assertTableEquals(
                TableTools.newTable(
                        TableTools.stringCol("Group", "Avengers", "Justice League", "Avengers", "Valhalla", "Avengers"),
                        TableTools.stringCol("Symbol", "SPY", "AAPL", "AAPL", "AAPL", "SPY"),
                        intCol("Quantity", -1500, 100, 200, 1500, 1000),
                        TableTools.doubleCol("Bid", 411.40, NULL_DOUBLE, NULL_DOUBLE, NULL_DOUBLE, 411.40),
                        TableTools.doubleCol("Ask", 411.42, NULL_DOUBLE, NULL_DOUBLE, NULL_DOUBLE, 411.42),
                        TableTools.instantCol("QuoteTime", baseTime, null, null, null, baseTime),
                        TableTools.doubleCol("Mid", spyMid, NULL_DOUBLE, NULL_DOUBLE, NULL_DOUBLE, spyMid),
                        TableTools.doubleCol("Notional", spyMid * -1500, NULL_DOUBLE, NULL_DOUBLE, NULL_DOUBLE,
                                spyMid * 1000)),
                decorated);

        TstUtils.assertTableEquals(
                TableTools.newTable(
                        TableTools.stringCol("Symbol", "SPY", "AAPL", "AAPL", "AAPL"),
                        TableTools.stringCol("Group", "Avengers", "Justice League", "Avengers", "Valhalla"),
                        TableTools.doubleCol("Notional", (spyMid * -1500 + spyMid * 1000), NULL_DOUBLE, NULL_DOUBLE,
                                NULL_DOUBLE),
                        longCol("Quantity", -500, 100, 200, 1500),
                        TableTools.doubleCol("Mid", spyMid, NULL_DOUBLE, NULL_DOUBLE, NULL_DOUBLE)),
                aggregated);

        prices.startBundle();
        prices.addRow(1, 1);
        prices.set2D(1, TableTools.newTable(
                stringCol("Symbol", "AAPL"),
                doubleCol("Bid", 122.60),
                doubleCol("Ask", 122.65),
                instantCol("Time", baseTime.plusSeconds(1))));
        endBundleFuture = prices.endBundle();

        updateGraph.runWithinUnitTestCycle(prices::run);
        endBundleFuture.get(0, TimeUnit.MILLISECONDS);

        updateGraph.sharedLock().doLocked(() -> {
            TableTools.show(decorated);
            TableTools.show(aggregated);
        });

        TstUtils.assertTableEquals(
                TableTools.newTable(
                        TableTools.stringCol("Group", "Avengers", "Justice League", "Avengers", "Valhalla", "Avengers"),
                        TableTools.stringCol("Symbol", "SPY", "AAPL", "AAPL", "AAPL", "SPY"),
                        intCol("Quantity", -1500, 100, 200, 1500, 1000),
                        TableTools.doubleCol("Bid", 411.40, 122.60, 122.60, 122.60, 411.40),
                        TableTools.doubleCol("Ask", 411.42, 122.65, 122.65, 122.65, 411.42),
                        TableTools.instantCol("QuoteTime", baseTime, baseTime.plusSeconds(1), baseTime.plusSeconds(1),
                                baseTime.plusSeconds(1), baseTime),
                        TableTools.doubleCol("Mid", spyMid, 122.625, 122.625, 122.625, spyMid),
                        TableTools.doubleCol("Notional", spyMid * -1500, 12262.5, 24525, 183937.5, spyMid * 1000)),
                decorated);

        TstUtils.assertTableEquals(
                TableTools.newTable(
                        TableTools.stringCol("Symbol", "SPY", "AAPL", "AAPL", "AAPL"),
                        TableTools.stringCol("Group", "Avengers", "Justice League", "Avengers", "Valhalla"),
                        TableTools.doubleCol("Notional", (spyMid * -1500 + spyMid * 1000), 12262.5, 24525, 183937.5),
                        longCol("Quantity", -500, 100, 200, 1500),
                        TableTools.doubleCol("Mid", spyMid, 122.625, 122.625, 122.625)),
                aggregated);
    }

    @Test
    public void testAllDataTypes() {
        new ArrayBackedPositionalMutableTable(TableDefinition.from(Arrays.asList(
                ColumnHeader.of("String", String.class),
                ColumnHeader.of("Long", long.class),
                ColumnHeader.of("Int", int.class),
                ColumnHeader.of("Double", double.class),
                ColumnHeader.of("Float", float.class),
                ColumnHeader.of("Short", short.class),
                ColumnHeader.of("Boolean", boolean.class),
                ColumnHeader.of("Timestamp", Instant.class))));
    }

    @Test
    public void testIllegalStartBundle() throws InterruptedException, ExecutionException {

        updateGraph.sharedLock().lock();
        final ArrayBackedPositionalMutableTable table;
        final InstrumentedTableUpdateListenerAdapter validatorListener;
        final PrintListener printListener;
        try (final SafeCloseable closeable = () -> updateGraph.sharedLock().unlock()) {
            table = new ArrayBackedPositionalMutableTable(testDefinition);
            final TableUpdateValidator tuv = TableUpdateValidator.make(table);
            final QueryTable validatorResult = tuv.getResultTable();
            validatorListener =
                    new InstrumentedTableUpdateListenerAdapter("Validator Result Listener", validatorResult, false) {
                        @Override
                        public void onUpdate(TableUpdate upstream) {
                            // uninteresting, we just want the failure behavior
                        }
                    };
            validatorResult.addUpdateListener(validatorListener);
            printListener = new PrintListener("table", table);
        }

        table.startBundle();
        table.startBundle();

        try {
            updateGraph.runWithinUnitTestCycle(table::run);
            Assert.statementNeverExecuted("Should have thrown an exception");
        } catch (IllegalStateException ise) {
            Assert.equals(ise.getMessage(), "ise.getMessage()",
                    "Attempt to start a bundle while a bundle is already in progress");
        }
    }
}
