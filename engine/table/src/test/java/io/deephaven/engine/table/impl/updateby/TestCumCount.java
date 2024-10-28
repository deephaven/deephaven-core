//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableDefaults;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.function.Numeric;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.testTable;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.*;
import static org.junit.Assert.assertArrayEquals;

@Category(OutOfBandTest.class)
public class TestCumCount extends BaseUpdateByTest {
    // region Zero Key Tests

    @Test
    public void testStaticZeroKey() {
        final QueryTable t = createTestTable(100000, false, false, false, 0x31313131).t;

        t.setRefreshing(false);

        final Table count_non_null = t.updateBy(UpdateByOperation.CumCount());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithCumCount(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_non_null, col).toArray());
        }

        final Table count_null = t.updateBy(UpdateByOperation.CumCountNull());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithCumCountNull(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_null, col).toArray());
        }

        final Table count_neg = t.updateBy(UpdateByOperation.CumCountNegative());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if (col.equals("boolCol")) {
                continue;
            }
            assertWithCumCountNegative(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_neg, col).toArray(),
                    t.getDefinition().getColumn(col).getDataType());
        }

        final Table count_pos = t.updateBy(UpdateByOperation.CumCountPositive());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if (col.equals("boolCol")) {
                continue;
            }
            assertWithCumCountPositive(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_pos, col).toArray(),
                    t.getDefinition().getColumn(col).getDataType());
        }

        final Table count_zero = t.updateBy(UpdateByOperation.CumCountZero());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if (col.equals("boolCol")) {
                continue;
            }
            assertWithCumCountZero(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_zero, col).toArray(),
                    t.getDefinition().getColumn(col).getDataType());
        }

        final Table count_nan = t.updateBy(UpdateByOperation.CumCountNaN());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if (col.equals("boolCol")) {
                continue;
            }
            assertWithCumCountNaN(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_nan, col).toArray(),
                    t.getDefinition().getColumn(col).getDataType());
        }

        final Table count_inf = t.updateBy(UpdateByOperation.CumCountInfinite());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if (col.equals("boolCol")) {
                continue;
            }
            assertWithCumCountInfinite(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_inf, col).toArray(),
                    t.getDefinition().getColumn(col).getDataType());
        }

        final Table count_finite = t.updateBy(UpdateByOperation.CumCountFinite());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if (col.equals("boolCol")) {
                continue;
            }
            assertWithCumCountFinite(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_finite, col).toArray(),
                    t.getDefinition().getColumn(col).getDataType());
        }
    }

    @Test
    public void testStaticZeroKeyAllNulls() {
        final QueryTable t = createTestTableAllNull(100000, false, false, false, 0x31313131,
                ArrayTypeUtils.EMPTY_STRING_ARRAY, new TestDataGenerator[0]).t;

        t.setRefreshing(false);

        final Table count_non_null = t.updateBy(UpdateByOperation.CumCount());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithCumCount(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_non_null, col).toArray());
        }

        final Table count_null = t.updateBy(UpdateByOperation.CumCountNull());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithCumCountNull(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_null, col).toArray());
        }

        final Table count_neg = t.updateBy(UpdateByOperation.CumCountNegative());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if (col.equals("boolCol")) {
                continue;
            }
            assertWithCumCountNegative(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_neg, col).toArray(),
                    t.getDefinition().getColumn(col).getDataType());
        }

        final Table count_pos = t.updateBy(UpdateByOperation.CumCountPositive());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if (col.equals("boolCol")) {
                continue;
            }
            assertWithCumCountPositive(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_pos, col).toArray(),
                    t.getDefinition().getColumn(col).getDataType());
        }

        final Table count_zero = t.updateBy(UpdateByOperation.CumCountZero());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if (col.equals("boolCol")) {
                continue;
            }
            assertWithCumCountZero(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_zero, col).toArray(),
                    t.getDefinition().getColumn(col).getDataType());
        }

        final Table count_nan = t.updateBy(UpdateByOperation.CumCountNaN());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if (col.equals("boolCol")) {
                continue;
            }
            assertWithCumCountNaN(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_nan, col).toArray(),
                    t.getDefinition().getColumn(col).getDataType());
        }

        final Table count_inf = t.updateBy(UpdateByOperation.CumCountInfinite());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if (col.equals("boolCol")) {
                continue;
            }
            assertWithCumCountInfinite(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_inf, col).toArray(),
                    t.getDefinition().getColumn(col).getDataType());
        }

        final Table count_finite = t.updateBy(UpdateByOperation.CumCountFinite());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if (col.equals("boolCol")) {
                continue;
            }
            assertWithCumCountFinite(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(count_finite, col).toArray(),
                    t.getDefinition().getColumn(col).getDataType());
        }
    }

    // endregion

    // region Bucketed Tests

    @Test
    public void testNullOnBucketChange() {
        final TableDefaults t = testTable(stringCol("Sym", "A", "A", "B", "B"),
                byteCol("ByteVal", (byte) 1, (byte) 2, NULL_BYTE, (byte) 3),
                shortCol("ShortVal", (short) 1, (short) 2, NULL_SHORT, (short) 3),
                intCol("IntVal", 1, 2, NULL_INT, 3));

        final TableDefaults expected = testTable(stringCol("Sym", "A", "A", "B", "B"),
                byteCol("ByteVal", (byte) 1, (byte) 2, NULL_BYTE, (byte) 3),
                shortCol("ShortVal", (short) 1, (short) 2, NULL_SHORT, (short) 3),
                intCol("IntVal", 1, 2, NULL_INT, 3),
                longCol("ByteValSum", 1, 2, 0, 1),
                longCol("ShortValSum", 1, 2, 0, 1),
                longCol("IntValSum", 1, 2, 0, 1));

        final Table r = t.updateBy(List.of(
                UpdateByOperation.CumCount("ByteValSum=ByteVal"),
                UpdateByOperation.CumCount("ShortValSum=ShortVal"),
                UpdateByOperation.CumCount("IntValSum=IntVal")), "Sym");

        assertTableEquals(expected, r);
    }

    @Test
    public void testStaticBucketed() {
        doTestStaticBucketed(false);
    }

    @Test
    public void testStaticGroupedBucketed() {
        doTestStaticBucketed(true);
    }

    private void doTestStaticBucketed(boolean grouped) {
        final QueryTable t = createTestTable(100000, true, grouped, false, 0x31313131).t;
        final PartitionedTable preOp = t.partitionBy("Sym");

        final Table count_non_null =
                t.updateBy(UpdateByOperation.CumCount("byteCol", "shortCol", "intCol", "longCol", "floatCol",
                        "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"), "Sym");

        PartitionedTable postOp = count_non_null.partitionBy("Sym");

        final String[] columns = Arrays.stream(t.getDefinition().getColumnNamesArray())
                .filter(col -> !col.equals("Sym") && !col.equals("boolCol")).toArray(String[]::new);

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithCumCount(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray());
            });
            return source;
        });

        final Table count_null =
                t.updateBy(UpdateByOperation.CumCountNull("byteCol", "shortCol", "intCol", "longCol", "floatCol",
                        "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"), "Sym");

        postOp = count_null.partitionBy("Sym");

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithCumCountNull(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray());
            });
            return source;
        });

        final Table count_neg =
                t.updateBy(UpdateByOperation.CumCountNegative("byteCol", "shortCol", "intCol", "longCol", "floatCol",
                        "doubleCol", "bigIntCol", "bigDecimalCol"), "Sym");

        postOp = count_neg.partitionBy("Sym");

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                if (col.equals("boolCol")) {
                    return;
                }
                assertWithCumCountNegative(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        t.getDefinition().getColumn(col).getDataType());
            });
            return source;
        });

        final Table count_pos =
                t.updateBy(UpdateByOperation.CumCountPositive("byteCol", "shortCol", "intCol", "longCol", "floatCol",
                        "doubleCol", "bigIntCol", "bigDecimalCol"), "Sym");

        postOp = count_pos.partitionBy("Sym");

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                if (col.equals("boolCol")) {
                    return;
                }
                assertWithCumCountPositive(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        t.getDefinition().getColumn(col).getDataType());
            });
            return source;
        });

        final Table count_zero =
                t.updateBy(UpdateByOperation.CumCountZero("byteCol", "shortCol", "intCol", "longCol", "floatCol",
                        "doubleCol", "bigIntCol", "bigDecimalCol"), "Sym");

        postOp = count_zero.partitionBy("Sym");

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                if (col.equals("boolCol")) {
                    return;
                }
                assertWithCumCountZero(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        t.getDefinition().getColumn(col).getDataType());
            });
            return source;
        });

        final Table count_nan =
                t.updateBy(UpdateByOperation.CumCountNaN("byteCol", "shortCol", "intCol", "longCol", "floatCol",
                        "doubleCol", "bigIntCol", "bigDecimalCol"), "Sym");

        postOp = count_nan.partitionBy("Sym");

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                if (col.equals("boolCol")) {
                    return;
                }
                assertWithCumCountNaN(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        t.getDefinition().getColumn(col).getDataType());
            });
            return source;
        });

        final Table count_inf =
                t.updateBy(UpdateByOperation.CumCountInfinite("byteCol", "shortCol", "intCol", "longCol", "floatCol",
                        "doubleCol", "bigIntCol", "bigDecimalCol"), "Sym");

        postOp = count_inf.partitionBy("Sym");

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                if (col.equals("boolCol")) {
                    return;
                }
                assertWithCumCountInfinite(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        t.getDefinition().getColumn(col).getDataType());
            });
            return source;
        });

        final Table count_finite =
                t.updateBy(UpdateByOperation.CumCountFinite("byteCol", "shortCol", "intCol", "longCol", "floatCol",
                        "doubleCol", "bigIntCol", "bigDecimalCol"), "Sym");

        postOp = count_finite.partitionBy("Sym");

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                if (col.equals("boolCol")) {
                    return;
                }
                assertWithCumCountFinite(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        t.getDefinition().getColumn(col).getDataType());
            });
            return source;
        });
    }

    // endregion

    // region Live Tests

    @Test
    public void testZeroKeyAppendOnly() {
        doTestAppendOnly(false);
    }

    @Test
    public void testBucketedAppendOnly() {
        doTestAppendOnly(true);
    }

    private void doTestAppendOnly(boolean bucketed) {
        final CreateResult result = createTestTable(10000, bucketed, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;
        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCount(), "Sym")
                        : t.updateBy(UpdateByOperation.CumCount())),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountNull(), "Sym")
                        : t.updateBy(UpdateByOperation.CumCountNull())),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountNegative(), "Sym")
                        : t.updateBy(UpdateByOperation.CumCountNegative())),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountPositive(), "Sym")
                        : t.updateBy(UpdateByOperation.CumCountPositive())),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountZero(), "Sym")
                        : t.updateBy(UpdateByOperation.CumCountZero())),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountNaN(), "Sym")
                        : t.updateBy(UpdateByOperation.CumCountNaN())),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountInfinite(), "Sym")
                        : t.updateBy(UpdateByOperation.CumCountInfinite())),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountFinite(), "Sym")
                        : t.updateBy(UpdateByOperation.CumCountFinite())),
        };

        final Random billy = new Random(0xB177B177L);
        for (int ii = 0; ii < 100; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(100, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    @Test
    public void testZeroKeyGeneralTicking() {
        final CreateResult result = createTestTable(100, false, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCount())),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCountNull())),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCountNegative())),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCountPositive())),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCountZero())),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCountNaN())),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCountInfinite())),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCountFinite())),
        };

        final Random billy = new Random(0xB177B177L);
        for (int ii = 0; ii < 100; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(100, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testBucketedGeneralTicking() {
        final CreateResult result = createTestTable(100, true, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCount(), "Sym")),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCountNull(), "Sym")),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCountNegative(), "Sym")),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCountPositive(), "Sym")),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCountZero(), "Sym")),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCountNaN(), "Sym")),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCountInfinite(), "Sym")),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.CumCountFinite(), "Sym")),
        };

        final Random billy = new Random(0xB177B177L);
        for (int ii = 0; ii < 100; ii++) {
            try {
                simulateShiftAwareStep(100, billy, t, result.infos, nuggets);
            } catch (Throwable ex) {
                System.out.println("Crapped out on step " + ii);
                throw ex;
            }
        }
    }

    /*
     * Ideas for specialized tests: 1) Remove first index 2) Removed everything, add some back 3) Make sandwiches
     */
    // endregion

    // region Helpers for Objects and floating point types
    @FunctionalInterface
    private interface ObjectFilter {
        boolean isCounted(Object value);
    }

    @FunctionalInterface
    private interface BigDecimalFilter {
        boolean isCounted(BigDecimal value);
    }

    @FunctionalInterface
    private interface BigIntegerFilter {
        boolean isCounted(BigInteger value);
    }

    @FunctionalInterface
    private interface DoubleFilter {
        boolean isCounted(double value);
    }

    @FunctionalInterface
    private interface FloatFilter {
        boolean isCounted(float value);
    }

    private static long[] object_cumcount(
            final Object[] values,
            final ObjectFilter filter) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        final long[] result = new long[values.length];
        long count = 0;

        for (int i = 0; i < values.length; i++) {
            if (filter.isCounted(values[i])) {
                count++;
            }
            result[i] = count;
        }

        return result;
    }

    private static long[] bd_cumcount(
            final BigDecimal[] values,
            final BigDecimalFilter filter) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        final long[] result = new long[values.length];
        long count = 0;

        for (int i = 0; i < values.length; i++) {
            if (filter.isCounted(values[i])) {
                count++;
            }
            result[i] = count;
        }

        return result;
    }

    private static long[] bi_cumcount(
            final BigInteger[] values,
            final BigIntegerFilter filter) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        final long[] result = new long[values.length];
        long count = 0;

        for (int i = 0; i < values.length; i++) {
            if (filter.isCounted(values[i])) {
                count++;
            }
            result[i] = count;
        }

        return result;
    }

    private static long[] double_cumcount(
            final double[] values,
            final DoubleFilter filter) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        final long[] result = new long[values.length];
        long count = 0;

        for (int i = 0; i < values.length; i++) {
            if (filter.isCounted(values[i])) {
                count++;
            }
            result[i] = count;
        }

        return result;
    }

    private static long[] float_cumcount(
            final float[] values,
            final FloatFilter filter) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        final long[] result = new long[values.length];
        long count = 0;

        for (int i = 0; i < values.length; i++) {
            if (filter.isCounted(values[i])) {
                count++;
            }
            result[i] = count;
        }

        return result;
    }
    // endregion

    // region External verification methods for the supported types
    final void assertWithCumCount(
            @NotNull final Object expected,
            @NotNull final Object actual) {
        final long[] actualValues = (long[]) actual;

        if (expected instanceof byte[]) {
            assertArrayEquals(Numeric.cumcount((byte[]) expected), actualValues);
        } else if (expected instanceof short[]) {
            assertArrayEquals(Numeric.cumcount((short[]) expected), actualValues);
        } else if (expected instanceof int[]) {
            assertArrayEquals(Numeric.cumcount((int[]) expected), actualValues);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Numeric.cumcount((long[]) expected), actualValues);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Numeric.cumcount((float[]) expected), actualValues);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Numeric.cumcount((double[]) expected), actualValues);
        } else {
            assertArrayEquals(object_cumcount((Object[]) expected, Objects::nonNull), actualValues);
        }
    }

    final void assertWithCumCountNull(
            @NotNull final Object expected,
            @NotNull final Object actual) {
        final long[] actualValues = (long[]) actual;

        if (expected instanceof byte[]) {
            assertArrayEquals(Numeric.cumcountnull((byte[]) expected), actualValues);
        } else if (expected instanceof short[]) {
            assertArrayEquals(Numeric.cumcountnull((short[]) expected), actualValues);
        } else if (expected instanceof int[]) {
            assertArrayEquals(Numeric.cumcountnull((int[]) expected), actualValues);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Numeric.cumcountnull((long[]) expected), actualValues);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Numeric.cumcountnull((float[]) expected), actualValues);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Numeric.cumcountnull((double[]) expected), actualValues);
        } else {
            assertArrayEquals(object_cumcount((Object[]) expected, Objects::isNull), actualValues);
        }
    }

    final void assertWithCumCountNegative(
            @NotNull final Object expected,
            @NotNull final Object actual,
            @NotNull final Class<?> type) {
        final long[] actualValues = (long[]) actual;

        if (expected instanceof byte[]) {
            assertArrayEquals(Numeric.cumcountneg((byte[]) expected), actualValues);
        } else if (expected instanceof short[]) {
            assertArrayEquals(Numeric.cumcountneg((short[]) expected), actualValues);
        } else if (expected instanceof int[]) {
            assertArrayEquals(Numeric.cumcountneg((int[]) expected), actualValues);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Numeric.cumcountneg((long[]) expected), actualValues);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Numeric.cumcountneg((float[]) expected), actualValues);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Numeric.cumcountneg((double[]) expected), actualValues);
        } else if (type == BigInteger.class) {
            final long[] expectedLong =
                    bi_cumcount((BigInteger[]) expected, value -> value != null && value.signum() < 0);
            assertArrayEquals(expectedLong, actualValues);
        } else if (type == BigDecimal.class) {
            final long[] expectedLong =
                    bd_cumcount((BigDecimal[]) expected, value -> value != null && value.signum() < 0);
            assertArrayEquals(expectedLong, actualValues);
        } else {
            Assert.statementNeverExecuted();
        }
    }

    final void assertWithCumCountPositive(
            @NotNull final Object expected,
            @NotNull final Object actual,
            @NotNull final Class<?> type) {
        final long[] actualValues = (long[]) actual;

        if (expected instanceof byte[]) {
            assertArrayEquals(Numeric.cumcountpos((byte[]) expected), actualValues);
        } else if (expected instanceof short[]) {
            assertArrayEquals(Numeric.cumcountpos((short[]) expected), actualValues);
        } else if (expected instanceof int[]) {
            assertArrayEquals(Numeric.cumcountpos((int[]) expected), actualValues);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Numeric.cumcountpos((long[]) expected), actualValues);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Numeric.cumcountpos((float[]) expected), actualValues);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Numeric.cumcountpos((double[]) expected), actualValues);
        } else if (type == BigInteger.class) {
            final long[] expectedLong =
                    bi_cumcount((BigInteger[]) expected, value -> value != null && value.signum() > 0);
            assertArrayEquals(expectedLong, actualValues);
        } else if (type == BigDecimal.class) {
            final long[] expectedLong =
                    bd_cumcount((BigDecimal[]) expected, value -> value != null && value.signum() > 0);
            assertArrayEquals(expectedLong, actualValues);
        } else {
            Assert.statementNeverExecuted();
        }
    }

    final void assertWithCumCountZero(
            @NotNull final Object expected,
            @NotNull final Object actual,
            @NotNull final Class<?> type) {
        final long[] actualValues = (long[]) actual;

        if (expected instanceof byte[]) {
            assertArrayEquals(Numeric.cumcountzero((byte[]) expected), actualValues);
        } else if (expected instanceof short[]) {
            assertArrayEquals(Numeric.cumcountzero((short[]) expected), actualValues);
        } else if (expected instanceof int[]) {
            assertArrayEquals(Numeric.cumcountzero((int[]) expected), actualValues);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Numeric.cumcountzero((long[]) expected), actualValues);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Numeric.cumcountzero((float[]) expected), actualValues);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Numeric.cumcountzero((double[]) expected), actualValues);
        } else if (type == BigInteger.class) {
            final long[] expectedLong =
                    bi_cumcount((BigInteger[]) expected, value -> value != null && value.signum() == 0);
            assertArrayEquals(expectedLong, actualValues);
        } else if (type == BigDecimal.class) {
            final long[] expectedLong =
                    bd_cumcount((BigDecimal[]) expected, value -> value != null && value.signum() == 0);
            assertArrayEquals(expectedLong, actualValues);
        } else {
            Assert.statementNeverExecuted();
        }
    }

    final void assertWithCumCountNaN(
            @NotNull final Object expected,
            @NotNull final Object actual,
            @NotNull final Class<?> type) {
        final long[] actualValues = (long[]) actual;

        if (expected instanceof byte[]) {
            assertArrayEquals(Numeric.cumcountnan((byte[]) expected), actualValues);
        } else if (expected instanceof short[]) {
            assertArrayEquals(Numeric.cumcountnan((short[]) expected), actualValues);
        } else if (expected instanceof int[]) {
            assertArrayEquals(Numeric.cumcountnan((int[]) expected), actualValues);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Numeric.cumcountnan((long[]) expected), actualValues);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Numeric.cumcountnan((float[]) expected), actualValues);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Numeric.cumcountnan((double[]) expected), actualValues);
        } else if (type == BigDecimal.class || type == BigInteger.class) {
            // BI/BD can't be NaN
            final long[] allZeros = new long[actualValues.length];
            assertArrayEquals(allZeros, actualValues);
        } else {
            Assert.statementNeverExecuted();
        }
    }

    final void assertWithCumCountInfinite(
            @NotNull final Object expected,
            @NotNull final Object actual,
            @NotNull final Class<?> type) {
        final long[] actualValues = (long[]) actual;

        if (expected instanceof byte[]) {
            assertArrayEquals(Numeric.cumcountinf((byte[]) expected), actualValues);
        } else if (expected instanceof short[]) {
            assertArrayEquals(Numeric.cumcountinf((short[]) expected), actualValues);
        } else if (expected instanceof int[]) {
            assertArrayEquals(Numeric.cumcountinf((int[]) expected), actualValues);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Numeric.cumcountinf((long[]) expected), actualValues);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Numeric.cumcountinf((float[]) expected), actualValues);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Numeric.cumcountinf((double[]) expected), actualValues);
        } else if (type == BigDecimal.class || type == BigInteger.class) {
            // BI/BD can't be infinite
            final long[] allZeros = new long[actualValues.length];
            assertArrayEquals(allZeros, actualValues);
        } else {
            Assert.statementNeverExecuted();
        }
    }

    final void assertWithCumCountFinite(
            @NotNull final Object expected,
            @NotNull final Object actual,
            @NotNull final Class<?> type) {
        final long[] actualValues = (long[]) actual;

        if (expected instanceof byte[]) {
            assertArrayEquals(Numeric.cumcountfinite((byte[]) expected), actualValues);
        } else if (expected instanceof short[]) {
            assertArrayEquals(Numeric.cumcountfinite((short[]) expected), actualValues);
        } else if (expected instanceof int[]) {
            assertArrayEquals(Numeric.cumcountfinite((int[]) expected), actualValues);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Numeric.cumcountfinite((long[]) expected), actualValues);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Numeric.cumcountfinite((float[]) expected), actualValues);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Numeric.cumcountfinite((double[]) expected), actualValues);
        } else if (type == BigDecimal.class || type == BigInteger.class) {
            // BI/BD are finite as long as not null
            assertArrayEquals(object_cumcount((Object[]) expected, Objects::nonNull), actualValues);
        } else {
            Assert.statementNeverExecuted();
        }
    }
    // endregion
}
