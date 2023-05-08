package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.UpdateErrorReporter;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.table.impl.TableDefaults;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.testutil.generator.SortedDateTimeGenerator;
import io.deephaven.engine.updategraph.TerminalNotification;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.ExceptionDetails;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.*;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.time.DateTimeUtils.MINUTE;
import static io.deephaven.time.DateTimeUtils.convertDateTime;

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
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))});

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

                                UpdateByOperation.RollingMax(50, 50,
                                        makeOpColNames(columnNamesArray, "_rollmaxticksrev", "Sym", "ts", "boolCol")),
                                UpdateByOperation.RollingMax("ts", Duration.ofMinutes(5), Duration.ofMinutes(5),
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
                                UpdateByOperation.CumMin(makeOpColNames(columnNamesArray, "_min", "boolCol")),
                                UpdateByOperation.CumMax(makeOpColNames(columnNamesArray, "_max", "boolCol")),
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
                    UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
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

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 8)); // Add to "B" bucket
            table.notifyListeners(i(8), i(), i());
        });

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(9), col("Key", "C"), intCol("Int", 10)); // New "C" bucket in isolation
            table.notifyListeners(i(9), i(), i());
        });

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(8), col("Key", "C"), intCol("Int", 11)); // Row from "B" bucket to "C" bucket
            table.notifyListeners(i(), i(), i(8));
        });

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
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
                new String[] {"ts"}, new TestDataGenerator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))});

        final OperationControl skipControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.SKIP)
                .onNanValue(BadDataBehavior.SKIP).build();

        final String[] columnNamesArray = result.t.getDefinition().getColumnNamesArray();
        final Collection<? extends UpdateByOperation> clauses = List.of(
                UpdateByOperation.Fill(),

                UpdateByOperation.RollingGroup(50, 50,
                        makeOpColNames(columnNamesArray, "_rollgroupfwdrev", "Sym", "ts")),
                UpdateByOperation.RollingGroup("ts", Duration.ofMinutes(5), Duration.ofMinutes(5),
                        makeOpColNames(columnNamesArray, "_rollgrouptimefwdrev", "Sym", "ts")),

                UpdateByOperation.RollingSum(100, 0,
                        makeOpColNames(columnNamesArray, "_rollsumticksrev", "Sym", "ts", "boolCol")),
                UpdateByOperation.RollingSum("ts", Duration.ofMinutes(15), Duration.ofMinutes(0),
                        makeOpColNames(columnNamesArray, "_rollsumtimerev", "Sym", "ts", "boolCol")),

                UpdateByOperation.RollingAvg(100, 0,
                        makeOpColNames(columnNamesArray, "_rollavgticksrev", "Sym", "ts", "boolCol")),
                UpdateByOperation.RollingAvg("ts", Duration.ofMinutes(15), Duration.ofMinutes(0),
                        makeOpColNames(columnNamesArray, "_rollavgtimerev", "Sym", "ts", "boolCol")),

                UpdateByOperation.RollingMin(100, 0,
                        makeOpColNames(columnNamesArray, "_rollminticksrev", "Sym", "ts", "boolCol")),
                UpdateByOperation.RollingMin("ts", Duration.ofMinutes(5), Duration.ofMinutes(0),
                        makeOpColNames(columnNamesArray, "_rollmintimerev", "Sym", "ts", "boolCol")),

                UpdateByOperation.RollingMax(100, 0,
                        makeOpColNames(columnNamesArray, "_rollmaxticksrev", "Sym", "ts", "boolCol")),
                UpdateByOperation.RollingMax("ts", Duration.ofMinutes(5), Duration.ofMinutes(0),
                        makeOpColNames(columnNamesArray, "_rollmaxtimerev", "Sym", "ts", "boolCol")),

                UpdateByOperation.Ema(skipControl, "ts", 10 * MINUTE,
                        makeOpColNames(columnNamesArray, "_ema", "Sym", "ts", "boolCol")),
                UpdateByOperation.CumSum(makeOpColNames(columnNamesArray, "_sum", "Sym", "ts")),
                UpdateByOperation.CumMin(makeOpColNames(columnNamesArray, "_min", "boolCol")),
                UpdateByOperation.CumMax(makeOpColNames(columnNamesArray, "_max", "boolCol")),
                UpdateByOperation
                        .CumProd(makeOpColNames(columnNamesArray, "_prod", "Sym", "ts", "boolCol")));
        final UpdateByControl control = UpdateByControl.builder().useRedirection(false).build();

        final Table table = result.t.updateBy(control, clauses, ColumnName.from("Sym"));
        final Table memoryTable = result.t.select().updateBy(control, clauses, ColumnName.from("Sym"));

        TstUtils.assertTableEquals("msg", table, memoryTable, TableDiff.DiffItems.DoublesExact);
    }

    @Override
    public void reportUpdateError(Throwable t) {
        UpdateGraphProcessor.DEFAULT.addNotification(new TerminalNotification() {
            @Override
            public void run() {
                System.err.println("Received error notification: " + new ExceptionDetails(t).getFullStackTrace());
                TestCase.fail(t.getMessage());
            }
        });
    }
}
