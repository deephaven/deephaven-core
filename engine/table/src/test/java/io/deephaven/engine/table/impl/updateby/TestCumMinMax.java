//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.function.Numeric;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static org.junit.Assert.assertArrayEquals;

@Category(OutOfBandTest.class)
public class TestCumMinMax extends BaseUpdateByTest {
    // region Zero Key Tests

    @Test
    public void testStaticZeroKey() {
        final QueryTable t = createTestTable(100000, false, false, false, 0x2134BCFA).t;

        final Table result = t.updateBy(List.of(
                UpdateByOperation.CumMin("byteColMin=byteCol", "shortColMin=shortCol", "intColMin=intCol",
                        "longColMin=longCol", "floatColMin=floatCol", "doubleColMin=doubleCol",
                        "bigIntColMin=bigIntCol", "bigDecimalColMin=bigDecimalCol"),
                UpdateByOperation.CumMax("byteColMax=byteCol", "shortColMax=shortCol", "intColMax=intCol",
                        "longColMax=longCol", "floatColMax=floatCol", "doubleColMax=doubleCol",
                        "bigIntColMax=bigIntCol", "bigDecimalColMax=bigDecimalCol")));
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if ("boolCol".equals(col)) {
                continue;
            }
            assertWithCumMin(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(result, col + "Min").toArray());
            assertWithCumMax(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(result, col + "Max").toArray());
        }
    }

    @Test
    public void testStaticZeroKeyAllNulls() {
        final QueryTable t = createTestTableAllNull(100000, false, false, false, 0x31313131,
                ArrayTypeUtils.EMPTY_STRING_ARRAY, new TestDataGenerator[0]).t;

        final Table result = t.updateBy(List.of(
                UpdateByOperation.CumMin("byteColMin=byteCol", "shortColMin=shortCol", "intColMin=intCol",
                        "longColMin=longCol", "floatColMin=floatCol", "doubleColMin=doubleCol",
                        "bigIntColMin=bigIntCol", "bigDecimalColMin=bigDecimalCol"),
                UpdateByOperation.CumMax("byteColMax=byteCol", "shortColMax=shortCol", "intColMax=intCol",
                        "longColMax=longCol", "floatColMax=floatCol", "doubleColMax=doubleCol",
                        "bigIntColMax=bigIntCol", "bigDecimalColMax=bigDecimalCol")));
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if ("boolCol".equals(col)) {
                continue;
            }
            assertWithCumMin(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(result, col + "Min").toArray());
            assertWithCumMax(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(result, col + "Max").toArray());
        }
    }

    // endregion

    // region Bucketed Tests

    @Test
    public void testStaticBucketed() {
        doTestStaticBucketed(false);
    }

    @Test
    public void testStaticGroupedBucketed() {
        doTestStaticBucketed(true);
    }

    private void doTestStaticBucketed(boolean grouped) {
        final QueryTable t = createTestTable(100000, true, grouped, false, 0xACDB4321).t;

        final Table result = t.updateBy(List.of(
                UpdateByOperation.CumMin("byteColMin=byteCol", "shortColMin=shortCol", "intColMin=intCol",
                        "longColMin=longCol", "floatColMin=floatCol", "doubleColMin=doubleCol",
                        "bigIntColMin=bigIntCol", "bigDecimalColMin=bigDecimalCol"),
                UpdateByOperation.CumMax("byteColMax=byteCol", "shortColMax=shortCol", "intColMax=intCol",
                        "longColMax=longCol", "floatColMax=floatCol", "doubleColMax=doubleCol",
                        "bigIntColMax=bigIntCol", "bigDecimalColMax=bigDecimalCol")),
                "Sym");

        final PartitionedTable preOp = t.partitionBy("Sym");
        final PartitionedTable postOp = result.partitionBy("Sym");

        String[] columns = Arrays.stream(t.getDefinition().getColumnNamesArray())
                .filter(col -> !col.equals("Sym") && !col.equals("boolCol")).toArray(String[]::new);

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithCumMin(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col + "Min").toArray());
                assertWithCumMax(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col + "Max").toArray());
            });
            return source;
        });
    }

    // endregion

    // region Live Tests

    @Test
    public void testZeroKeyAppendOnly() {
        doTestTicking(false, true);
    }

    @Test
    public void testBucketedAppendOnly() {
        doTestTicking(true, true);
    }

    @Test
    public void testZeroKeyGeneral() {
        doTestTicking(false, false);
    }

    @Test
    public void testBucketedGeneral() {
        doTestTicking(true, false);
    }

    private void doTestTicking(boolean bucketed, boolean appendOnly) {
        final CreateResult result = createTestTable(10000, bucketed, false, true, 0x31313131);
        final QueryTable t = result.t;

        if (appendOnly) {
            t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        }

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(UpdateByOperation.CumMin(), "Sym")
                                : t.updateBy(UpdateByOperation.CumMin());
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(UpdateByOperation.CumMax(), "Sym")
                                : t.updateBy(UpdateByOperation.CumMax());
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < 100; ii++) {
            if (appendOnly) {
                ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                        () -> generateAppends(100, billy, t, result.infos));
                TstUtils.validate("Table", nuggets);
            } else {
                simulateShiftAwareStep(100, billy, t, result.infos, nuggets);
            }
        }
    }
    // endregion

    public static Object[] cumMin(Object... values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new Object[0];
        }

        Object[] result = new Object[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (result[i - 1] == null) {
                result[i] = values[i];
            } else if (values[i] == null) {
                result[i] = result[i - 1];
            } else {
                result[i] = ((Comparable) result[i - 1]).compareTo(values[i]) < 0 ? result[i - 1] : values[i];
            }
        }

        return result;
    }

    public static Object[] cumMax(Object... values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new Object[0];
        }

        Object[] result = new Object[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (result[i - 1] == null) {
                result[i] = values[i];
            } else if (values[i] == null) {
                result[i] = result[i - 1];
            } else {
                result[i] = ((Comparable) result[i - 1]).compareTo(values[i]) > 0 ? result[i - 1] : values[i];
            }
        }

        return result;
    }

    final void assertWithCumMin(@NotNull final Object expected, @NotNull final Object actual) {
        if (expected instanceof byte[]) {
            assertArrayEquals(Numeric.cummin((byte[]) expected), (byte[]) actual);
        } else if (expected instanceof short[]) {
            assertArrayEquals(Numeric.cummin((short[]) expected), (short[]) actual);
        } else if (expected instanceof int[]) {
            assertArrayEquals(Numeric.cummin((int[]) expected), (int[]) actual);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Numeric.cummin((long[]) expected), (long[]) actual);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Numeric.cummin((float[]) expected), (float[]) actual, .001f);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Numeric.cummin((double[]) expected), (double[]) actual, .001d);
        } else {
            assertArrayEquals(cumMin((Object[]) expected), (Object[]) actual);
        }
    }

    final void assertWithCumMax(@NotNull final Object expected, @NotNull final Object actual) {
        if (expected instanceof byte[]) {
            assertArrayEquals(Numeric.cummax((byte[]) expected), (byte[]) actual);
        } else if (expected instanceof short[]) {
            assertArrayEquals(Numeric.cummax((short[]) expected), (short[]) actual);
        } else if (expected instanceof int[]) {
            assertArrayEquals(Numeric.cummax((int[]) expected), (int[]) actual);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Numeric.cummax((long[]) expected), (long[]) actual);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Numeric.cummax((float[]) expected), (float[]) actual, .001f);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Numeric.cummax((double[]) expected), (double[]) actual, .001d);
        } else {
            assertArrayEquals(cumMax((Object[]) expected), (Object[]) actual);
        }
    }
}
