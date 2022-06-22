package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.Selectable;
import io.deephaven.engine.table.BadDataBehavior;
import io.deephaven.engine.table.EmaControl;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.UpdateByClause;
import io.deephaven.engine.table.impl.EvalNugget;
import io.deephaven.engine.table.impl.TableWithDefaults;
import io.deephaven.engine.table.impl.TstUtils;
import io.deephaven.engine.table.impl.UpdateByControl;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.test.types.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static io.deephaven.engine.table.impl.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.table.impl.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.table.impl.TstUtils.validate;
import static io.deephaven.time.DateTimeUtils.MINUTE;
import static io.deephaven.time.DateTimeUtils.convertDateTime;

@Category(ParallelTest.class)
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
                new String[] {"ts"}, new TstUtils.Generator[] {new TstUtils.SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))});

        if (appendOnly) {
            result.t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        }

        final EmaControl skipControl = EmaControl.builder()
                .onNullValue(BadDataBehavior.Skip)
                .onNanValue(BadDataBehavior.Skip).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    Table base;

                    @Override
                    protected Table e() {
                        TableWithDefaults base = result.t;
                        if (!appendOnly) {
                            base = (TableWithDefaults) base.sort("ts");
                        }

                        final String[] columnNamesArray = base.getDefinition().getColumnNamesArray();
                        final Collection<UpdateByClause> clauses = UpdateByClause.of(
                                UpdateByClause.fill(),
                                UpdateByClause.ema("ts", 10 * MINUTE, skipControl,
                                        makeOpColNames(columnNamesArray, "_ema", "Sym", "ts", "boolCol")),
                                UpdateByClause.sum(makeOpColNames(columnNamesArray, "_sum", "Sym", "ts")),
                                UpdateByClause.min(makeOpColNames(columnNamesArray, "_min", "boolCol")),
                                UpdateByClause.max(makeOpColNames(columnNamesArray, "_max", "boolCol")),
                                UpdateByClause.prod(makeOpColNames(columnNamesArray, "_prod", "Sym", "ts", "boolCol")));

                        final UpdateByControl control;
                        if (redirected) {
                            control = new UpdateByControl() {
                                @Override
                                public boolean useRedirection() {
                                    return true;
                                }
                            };
                        } else {
                            control = UpdateByControl.DEFAULT;
                        }

                        return bucketed
                                ? base.updateBy(control, clauses, Selectable.from("Sym"))
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
}
