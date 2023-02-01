package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.table.impl.TableDefaults;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.testutil.generator.SortedDateTimeGenerator;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
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
public class TestUpdateByGeneral extends BaseUpdateByTest {

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
                                UpdateByOperation.RollingSum(100, 0,
                                        makeOpColNames(columnNamesArray, "_rollsumticksrev", "Sym", "ts", "boolCol")),
                                UpdateByOperation.RollingSum("ts", Duration.ofMinutes(15), Duration.ofMinutes(0),
                                        makeOpColNames(columnNamesArray, "_rollsumtimerev", "Sym", "ts", "boolCol")),
                                UpdateByOperation.RollingSum(0, 100,
                                        makeOpColNames(columnNamesArray, "_rollsumticksfwd", "Sym", "ts", "boolCol")),
                                UpdateByOperation.RollingSum(-50, 100,
                                        makeOpColNames(columnNamesArray, "_rollsumticksfwdex", "Sym", "ts", "boolCol")),
                                UpdateByOperation.RollingSum("ts", Duration.ofMinutes(0), Duration.ofMinutes(15),
                                        makeOpColNames(columnNamesArray, "_rollsumtimefwd", "Sym", "ts", "boolCol")),
                                UpdateByOperation.RollingSum("ts", Duration.ofMinutes(-10), Duration.ofMinutes(15),
                                        makeOpColNames(columnNamesArray, "_rollsumtimefwdex", "Sym", "ts", "boolCol")),
                                UpdateByOperation.RollingSum(50, 50,
                                        makeOpColNames(columnNamesArray, "_rollsumticksfwdrev", "Sym", "ts",
                                                "boolCol")),
                                UpdateByOperation.RollingSum("ts", Duration.ofMinutes(5), Duration.ofMinutes(5),
                                        makeOpColNames(columnNamesArray, "_rollsumtimebothfwdrev", "Sym", "ts",
                                                "boolCol")),

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
                },
        };

        for (int step = 0; step < steps; step++) {
            try {
                if (appendOnly) {
                    UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                        generateAppends(100, result.random, result.t, result.infos);
                    });
                    validate("Table", nuggets);
                } else {
                    simulateShiftAwareStep(size, result.random, result.t, result.infos, nuggets);
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
}
