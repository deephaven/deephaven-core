//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.UpdateErrorReporter;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.table.impl.TableDefaults;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.testutil.generator.SortedInstantGenerator;
import io.deephaven.engine.updategraph.TerminalNotification;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.ExceptionDetails;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.*;

import static io.deephaven.api.updateby.UpdateByOperation.*;
import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.time.DateTimeUtils.MINUTE;

@Category(OutOfBandTest.class)
public class TestUpdateByGeneral extends BaseUpdateByTest implements UpdateErrorReporter {

    private UpdateErrorReporter oldReporter;

    @Before
    public void setUp() throws Exception {
        oldReporter = AsyncClientErrorNotifier.setReporter(this);
    }

    @After
    public void tearDown() throws Exception {
        AsyncClientErrorNotifier.setReporter(oldReporter);
        oldReporter = null;
    }

    @Test
    public void testMixedAppendOnlyZeroKey() {
        for (int size = 10; size <= 10000; size *= 10) {
            for (int seed = 10; seed < 20; seed++) {
                doTestTicking(seed > 15, false, true, 20, size, seed);
            }
        }
    }

    @Test
    public void testMixedAppendOnlyBucketed() {
        for (int size = 10; size <= 10000; size *= 10) {
            for (int seed = 10; seed < 20; seed++) {
                doTestTicking(seed > 15, true, true, 20, size, seed);
            }
        }
    }

    @Test
    public void testMixedGeneralZeroKey() {
        for (int size = 10; size <= 10000; size *= 10) {
            for (int seed = 10; seed < 20; seed++) {
                doTestTicking(seed > 15, false, false, 20, size, seed);
            }
        }
    }

    @Test
    public void testMixedGeneralBucketed() {
        for (int size = 10; size <= 10000; size *= 10) {
            for (int seed = 10; seed < 20; seed++) {
                doTestTicking(seed > 15, true, false, 20, size, seed);
            }
        }
    }

    private void doTestTicking(boolean redirected, boolean bucketed, boolean appendOnly, int steps, int size,
            int seed) {
        final CreateResult result = createTestTable(size, bucketed, false, true, seed,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});

        if (appendOnly) {
            result.t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        }

        final OperationControl skipControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.SKIP)
                .onNanValue(BadDataBehavior.SKIP).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        TableDefaults base = result.t;
                        if (!appendOnly) {
                            base = (TableDefaults) base.sort("ts");
                        }

                        final String[] columnNamesArray = base.getDefinition().getColumnNamesArray();
                        final Collection<? extends UpdateByOperation> clauses = List.of(
                                UpdateByOperation.Fill(),

                                UpdateByOperation.RollingSum(50, 50,
                                        makeOpColNames(columnNamesArray, "_rollsumticksrev", "Sym", "ts", "boolCol")),
                                UpdateByOperation.RollingSum("ts", Duration.ofMinutes(5), Duration.ofMinutes(5),
                                        makeOpColNames(columnNamesArray, "_rollsumtimerev", "Sym", "ts", "boolCol")),

                                UpdateByOperation.RollingAvg(50, 50,
                                        makeOpColNames(columnNamesArray, "_rollavgticksrev", "Sym", "ts", "boolCol")),
                                UpdateByOperation.RollingAvg("ts", Duration.ofMinutes(5), Duration.ofMinutes(5),
                                        makeOpColNames(columnNamesArray, "_rollavgtimerev", "Sym", "ts", "boolCol")),

                                UpdateByOperation.RollingMin(50, 50,
                                        makeOpColNames(columnNamesArray, "_rollminticksrev", "Sym", "ts", "boolCol")),
                                UpdateByOperation.RollingMin("ts", Duration.ofMinutes(5), Duration.ofMinutes(5),
                                        makeOpColNames(columnNamesArray, "_rollmintimerev", "Sym", "ts", "boolCol")),

                                RollingMax(50, 50,
                                        makeOpColNames(columnNamesArray, "_rollmaxticksrev", "Sym", "ts", "boolCol")),
                                RollingMax("ts", Duration.ofMinutes(5), Duration.ofMinutes(5),
                                        makeOpColNames(columnNamesArray, "_rollmaxtimerev", "Sym", "ts", "boolCol")),

                                // Excluding 'bigDecimalCol' because we need fuzzy matching which doesn't exist for BD
                                UpdateByOperation.RollingProduct(50, 50,
                                        makeOpColNames(columnNamesArray, "_rollprodticksrev", "Sym", "ts", "boolCol",
                                                "bigDecimalCol")),
                                UpdateByOperation.RollingProduct("ts", Duration.ofMinutes(5), Duration.ofMinutes(5),
                                        makeOpColNames(columnNamesArray, "_rollprodtimerev", "Sym", "ts", "boolCol",
                                                "bigDecimalCol")),

                                UpdateByOperation.Ema(skipControl, "ts", 10 * MINUTE,
                                        makeOpColNames(columnNamesArray, "_ema", "Sym", "ts", "boolCol")),
                                UpdateByOperation.CumSum(makeOpColNames(columnNamesArray, "_sum", "Sym", "ts")),
                                CumMin(makeOpColNames(columnNamesArray, "_min", "boolCol")),
                                CumMax(makeOpColNames(columnNamesArray, "_max", "boolCol")),
                                UpdateByOperation
                                        .CumProd(makeOpColNames(columnNamesArray, "_prod", "Sym", "ts", "boolCol")));
                        final UpdateByControl control = UpdateByControl.builder().useRedirection(redirected).build();
                        return bucketed
                                ? base.updateBy(control, clauses, ColumnName.from("Sym"))
                                : base.updateBy(control, clauses);
                    }

                    @Override
                    @NotNull
                    public EnumSet<TableDiff.DiffItems> diffItems() {
                        return EnumSet.of(TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);
                    }
                },
        };

        final int stepSize = Math.max(5, size / 10);

        for (int step = 0; step < steps; step++) {
            try {
                if (appendOnly) {
                    ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                            () -> {
                                generateAppends(stepSize, result.random, result.t, result.infos);
                            });
                    validate("Table", nuggets);
                } else {
                    simulateShiftAwareStep(stepSize, result.random, result.t, result.infos, nuggets);
                }
            } catch (Throwable t) {
                System.out
                        .println("Crapped out on step " + step + " steps " + steps + " size " + size + " seed " + seed);
                throw t;
            }
        }
    }

    private String[] makeOpColNames(String[] colNames, String suffix, String... toOmit) {
        final Set<String> omissions = new HashSet<>(Arrays.asList(toOmit));
        return Arrays.stream(colNames)
                .filter(cn -> !omissions.contains(cn))
                .map(cn -> cn + suffix + "=" + cn)
                .toArray(String[]::new);
    }

    @Test
    public void testNewBuckets() {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6).toTracking(),
                col("Key", "A", "B", "A"),
                intCol("Int", 2, 4, 6));
        final QueryTable result = (QueryTable) table.updateBy(
                List.of(UpdateByOperation.Fill("Filled=Int"), UpdateByOperation.RollingSum(2, "Sum=Int")), "Key");

        // Add to "B" bucket
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 8)); // Add to "B" bucket
            table.notifyListeners(i(8), i(), i());
        });

        // New "C" bucket in isolation
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(9), col("Key", "C"), intCol("Int", 10)); // New "C" bucket in isolation
            table.notifyListeners(i(9), i(), i());
        });

        // Row from "B" bucket to "C" bucket
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(8), col("Key", "C"), intCol("Int", 11)); // Row from "B" bucket to "C" bucket
            table.notifyListeners(i(), i(), i(8));
        });

        // New "D" bucket
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(10, 11), col("Key", "D", "C"), intCol("Int", 10, 11)); // New "D" bucket
            table.notifyListeners(i(10, 11), i(), i());
        });

        TableTools.show(result);
    }

    @Test
    public void testStaticRedirected() {
        final int prevTicks = 100;
        final int postTicks = 0;

        final CreateResult result = createTestTable(100, true, false, false, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final UpdateByControl control = UpdateByControl.builder()
                .useRedirection(true)
                // Provide a low memory overhead to force use of redirection.
                .maxStaticSparseMemoryOverhead(0.1)
                .build();

        Table ignored = t.updateBy(control, List.of(UpdateByOperation.RollingCount(prevTicks, postTicks)));
    }

    @Test
    public void testInMemoryColumn() {
        final CreateResult result = createTestTable(1000, true, false, false, 0xFEEDFACE,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});

        final OperationControl skipControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.SKIP)
                .onNanValue(BadDataBehavior.SKIP).build();

        final String[] columnNamesArray = result.t.getDefinition().getColumnNamesArray();
        final Collection<? extends UpdateByOperation> clauses = List.of(
                UpdateByOperation.Fill(),

                RollingGroup(50, 50,
                        makeOpColNames(columnNamesArray, "_rollgroupfwdrev", "Sym", "ts")),
                RollingGroup("ts", Duration.ofMinutes(5), Duration.ofMinutes(5),
                        makeOpColNames(columnNamesArray, "_rollgrouptimefwdrev", "Sym", "ts")),

                RollingSum(100, 0,
                        makeOpColNames(columnNamesArray, "_rollsumticksrev", "Sym", "ts", "boolCol")),
                RollingSum("ts", Duration.ofMinutes(15), Duration.ofMinutes(0),
                        makeOpColNames(columnNamesArray, "_rollsumtimerev", "Sym", "ts", "boolCol")),

                RollingAvg(100, 0,
                        makeOpColNames(columnNamesArray, "_rollavgticksrev", "Sym", "ts", "boolCol")),
                RollingAvg("ts", Duration.ofMinutes(15), Duration.ofMinutes(0),
                        makeOpColNames(columnNamesArray, "_rollavgtimerev", "Sym", "ts", "boolCol")),

                RollingMin(100, 0,
                        makeOpColNames(columnNamesArray, "_rollminticksrev", "Sym", "ts", "boolCol")),
                RollingMin("ts", Duration.ofMinutes(5), Duration.ofMinutes(0),
                        makeOpColNames(columnNamesArray, "_rollmintimerev", "Sym", "ts", "boolCol")),

                RollingMax(100, 0,
                        makeOpColNames(columnNamesArray, "_rollmaxticksrev", "Sym", "ts", "boolCol")),
                RollingMax("ts", Duration.ofMinutes(5), Duration.ofMinutes(0),
                        makeOpColNames(columnNamesArray, "_rollmaxtimerev", "Sym", "ts", "boolCol")),

                Ema(skipControl, "ts", 10 * MINUTE, makeOpColNames(columnNamesArray, "_ema", "Sym", "ts", "boolCol")),
                CumSum(makeOpColNames(columnNamesArray, "_sum", "Sym", "ts")),
                CumMin(makeOpColNames(columnNamesArray, "_min", "boolCol")),
                CumMax(makeOpColNames(columnNamesArray, "_max", "boolCol")),
                CumProd(makeOpColNames(columnNamesArray, "_prod", "Sym", "ts", "boolCol")));
        final UpdateByControl control = UpdateByControl.builder().useRedirection(false).build();

        final Table table = result.t.updateBy(control, clauses, ColumnName.from("Sym"));
        final Table memoryTable = result.t.select().updateBy(control, clauses, ColumnName.from("Sym"));

        TstUtils.assertTableEquals("msg", table, memoryTable, TableDiff.DiffItems.DoublesExact);
    }

    @Override
    public void reportUpdateError(Throwable t) {
        ExecutionContext.getContext().getUpdateGraph().addNotification(new TerminalNotification() {
            @Override
            public void run() {
                System.err.println("Received error notification: " + new ExceptionDetails(t).getFullStackTrace());
                TestCase.fail(t.getMessage());
            }
        });
    }

    @Test
    public void testResultColumnOrdering() {
        final Table source = emptyTable(5).update("X=ii");

        final ColumnHolder<?> x = longCol("X", 0, 1, 2, 3, 4);
        final ColumnHolder<?> cumMin = longCol("cumMin", 0, 0, 0, 0, 0);
        final ColumnHolder<?> cumMax = longCol("cumMax", 0, 1, 2, 3, 4);
        final ColumnHolder<?> rollingMin = longCol("rollingMin", 0, 0, 1, 2, 3);
        final ColumnHolder<?> rollingMax = longCol("rollingMax", 0, 1, 2, 3, 4);

        final Table result_1 = source.updateBy(List.of(
                CumMin("cumMin=X"),
                CumMax("cumMax=X"),
                RollingMin(2, "rollingMin=X"),
                RollingMax(2, "rollingMax=X")));
        final Table expected_1 = TableTools.newTable(x, cumMin, cumMax, rollingMin, rollingMax);
        Assert.assertEquals("", diff(result_1, expected_1, 10));

        final Table result_2 = source.updateBy(List.of(
                CumMax("cumMax=X"),
                CumMin("cumMin=X"),
                RollingMax(2, "rollingMax=X"),
                RollingMin(2, "rollingMin=X")));
        final Table expected_2 = TableTools.newTable(x, cumMax, cumMin, rollingMax, rollingMin);
        Assert.assertEquals("", diff(result_2, expected_2, 10));

        final Table result_3 = source.updateBy(List.of(
                RollingMin(2, "rollingMin=X"),
                RollingMax(2, "rollingMax=X"),
                CumMin("cumMin=X"),
                CumMax("cumMax=X")));
        final Table expected_3 = TableTools.newTable(x, rollingMin, rollingMax, cumMin, cumMax);
        Assert.assertEquals("", diff(result_3, expected_3, 10));

        final Table result_4 = source.updateBy(List.of(
                RollingMax(2, "rollingMax=X"),
                RollingMin(2, "rollingMin=X"),
                CumMax("cumMax=X"),
                CumMin("cumMin=X")));
        final Table expected_4 = TableTools.newTable(x, rollingMax, rollingMin, cumMax, cumMin);
        Assert.assertEquals("", diff(result_4, expected_4, 10));

        final Table result_5 = source.updateBy(List.of(
                CumMin("cumMin=X"),
                RollingMin(2, "rollingMin=X"),
                CumMax("cumMax=X"),
                RollingMax(2, "rollingMax=X")));
        final Table expected_5 = TableTools.newTable(x, cumMin, rollingMin, cumMax, rollingMax);
        Assert.assertEquals("", diff(result_5, expected_5, 10));

        // Trickiest one, since we internally combine groupBy operations.
        final Table source_2 = source.update("Y=ii % 2");
        final Table result_6 = source_2.updateBy(List.of(
                CumMin("cumMin=X"),
                RollingGroup(2, "rollingGroupY=Y"),
                RollingMin(2, "rollingMin=X"),
                CumMax("cumMax=X"),
                RollingGroup(2, "rollingGroupX=X"),
                RollingMax(2, "rollingMax=X")));

        Assert.assertArrayEquals(result_6.getDefinition().getColumnNamesArray(),
                new String[] {
                        "X",
                        "Y",
                        "cumMin",
                        "rollingGroupY",
                        "rollingMin",
                        "cumMax",
                        "rollingGroupX",
                        "rollingMax"});
    }
}
