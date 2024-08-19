//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.TstUtils;
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
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.testTable;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.function.Basic.isNull;
import static io.deephaven.util.QueryConstants.*;
import static org.junit.Assert.assertArrayEquals;

@Category(OutOfBandTest.class)
public class TestCumProd extends BaseUpdateByTest {
    // region Zero Key Tests
    @Test
    public void testStaticZeroKey() {
        final QueryTable t = createTestTable(1000, false, false, false, 0xABCD1234).t;
        final Table result = t.updateBy(UpdateByOperation.CumProd());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if ("boolCol".equals(col)) {
                continue;
            }
            assertWithCumProd(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(result, col).toArray(),
                    result.getDefinition().getColumn(col).getDataType());
        }
    }

    @Test
    public void testStaticZeroKeyAllNulls() {
        final QueryTable t = createTestTableAllNull(100000, false, false, false, 0x31313131,
                ArrayTypeUtils.EMPTY_STRING_ARRAY, new TestDataGenerator[0]).t;
        final Table result = t.updateBy(UpdateByOperation.CumProd());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if ("boolCol".equals(col)) {
                continue;
            }
            assertWithCumProd(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(result, col).toArray(),
                    result.getDefinition().getColumn(col).getDataType());
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
                longCol("ByteValProd", 1, 2, NULL_LONG, 3),
                longCol("ShortValProd", 1, 2, NULL_LONG, 3),
                longCol("IntValProd", 1, 2, NULL_LONG, 3));

        final Table r = t.updateBy(List.of(
                UpdateByOperation.CumProd("ByteValProd=ByteVal"),
                UpdateByOperation.CumProd("ShortValProd=ShortVal"),
                UpdateByOperation.CumProd("IntValProd=IntVal")), "Sym");

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
        final QueryTable t = createTestTable(10000, true, grouped, false, 0x4321CBDA).t;

        final Table result =
                t.updateBy(UpdateByOperation.CumProd("byteCol", "shortCol", "intCol", "longCol", "floatCol",
                        "doubleCol", "bigIntCol", "bigDecimalCol"), "Sym");

        final PartitionedTable preOp = t.partitionBy("Sym");
        final PartitionedTable postOp = result.partitionBy("Sym");

        String[] columns = Arrays.stream(t.getDefinition().getColumnNamesArray())
                .filter(col -> !col.equals("Sym") && !col.equals("boolCol")).toArray(String[]::new);

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithCumProd(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        actual.getDefinition().getColumn(col).getDataType());
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
        final CreateResult result = createTestTable(10000, bucketed, false, true, 0x31313131);
        final QueryTable t = result.t;
        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(UpdateByOperation.CumProd(), "Sym")
                                : t.updateBy(UpdateByOperation.CumProd());
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < 100; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(100, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    @Test
    public void testZeroKeyGeneralTicking() {
        final CreateResult result = createTestTable(10000, false, false, true, 0x31313131);
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.CumProd());
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < 100; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(100, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testBucketedGeneralTicking() {
        final CreateResult result = createTestTable(10000, true, false, true, 0x31313131);
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.CumProd(), "Sym");
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < 100; ii++) {
            try {
                simulateShiftAwareStep(100, billy, t, result.infos, nuggets);
            } catch (Throwable ex) {
                System.out.println("Crapped out on step " + ii);
                throw ex;
            }
        }
    }

    // endregion

    public static long[] cumprod(byte[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        long[] result = new long[values.length];
        result[0] = isNull(values[0]) ? NULL_LONG : values[0];

        for (int i = 1; i < values.length; i++) {
            final boolean curValNull = isNull(values[i]);
            if (isNull(result[i - 1])) {
                result[i] = curValNull ? NULL_LONG : values[i];
            } else {
                if (curValNull) {
                    result[i] = result[i - 1];
                } else {
                    result[i] = result[i - 1] * values[i];
                }
            }
        }

        return result;
    }

    public static long[] cumprod(short[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        long[] result = new long[values.length];
        result[0] = isNull(values[0]) ? NULL_LONG : values[0];

        for (int i = 1; i < values.length; i++) {
            final boolean curValNull = isNull(values[i]);
            if (isNull(result[i - 1])) {
                result[i] = curValNull ? NULL_LONG : values[i];
            } else {
                if (curValNull) {
                    result[i] = result[i - 1];
                } else {
                    result[i] = result[i - 1] * values[i];
                }
            }
        }

        return result;
    }

    public static long[] cumprod(int[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        long[] result = new long[values.length];
        result[0] = isNull(values[0]) ? NULL_LONG : values[0];

        for (int i = 1; i < values.length; i++) {
            final boolean curValNull = isNull(values[i]);
            if (isNull(result[i - 1])) {
                result[i] = curValNull ? NULL_LONG : values[i];
            } else {
                if (curValNull) {
                    result[i] = result[i - 1];
                } else {
                    result[i] = result[i - 1] * values[i];
                }
            }
        }

        return result;
    }

    public static Object[] cumprod(Object[] values, final boolean isBD) {
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
            } else if (isBD) {
                result[i] = ((BigDecimal) result[i - 1]).multiply((BigDecimal) values[i],
                        UpdateByControl.mathContextDefault());
            } else {
                result[i] = ((BigInteger) result[i - 1]).multiply((BigInteger) values[i]);
            }
        }

        return result;
    }

    final void assertWithCumProd(@NotNull final Object expected, @NotNull final Object actual, Class<?> type) {
        if (expected instanceof byte[]) {
            assertArrayEquals(cumprod((byte[]) expected), (long[]) actual);
        } else if (expected instanceof short[]) {
            assertArrayEquals(cumprod((short[]) expected), (long[]) actual);
        } else if (expected instanceof int[]) {
            assertArrayEquals(cumprod((int[]) expected), (long[]) actual);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Numeric.cumprod((long[]) expected), (long[]) actual);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Numeric.cumprod((float[]) expected), (double[]) actual, .001f);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Numeric.cumprod((double[]) expected), (double[]) actual, .001d);
        } else {
            assertArrayEquals(cumprod((Object[]) expected, type == BigDecimal.class), (Object[]) actual);
        }
    }
}
