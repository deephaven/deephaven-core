//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.EvalNugget;
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
public class TestCumSum extends BaseUpdateByTest {
    // region Zero Key Tests

    @Test
    public void testStaticZeroKey() {
        final QueryTable t = createTestTable(100000, false, false, false, 0x31313131).t;

        t.setRefreshing(false);

        final Table summed = t.updateBy(UpdateByOperation.CumSum());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithCumSum(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(summed, col).toArray(),
                    summed.getDefinition().getColumn(col).getDataType());
        }
    }

    @Test
    public void testStaticZeroKeyAllNulls() {
        final QueryTable t = createTestTableAllNull(100000, false, false, false, 0x31313131,
                ArrayTypeUtils.EMPTY_STRING_ARRAY, new TestDataGenerator[0]).t;

        t.setRefreshing(false);

        final Table summed = t.updateBy(UpdateByOperation.CumSum());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithCumSum(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(summed, col).toArray(),
                    summed.getDefinition().getColumn(col).getDataType());
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
                longCol("ByteValSum", 1, 3, NULL_LONG, 3),
                longCol("ShortValSum", 1, 3, NULL_LONG, 3),
                longCol("IntValSum", 1, 3, NULL_LONG, 3));

        final Table r = t.updateBy(List.of(
                UpdateByOperation.CumSum("ByteValSum=ByteVal"),
                UpdateByOperation.CumSum("ShortValSum=ShortVal"),
                UpdateByOperation.CumSum("IntValSum=IntVal")), "Sym");

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

        final Table summed = t.updateBy(UpdateByOperation.CumSum("byteCol", "shortCol", "intCol", "longCol", "floatCol",
                "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"), "Sym");


        final PartitionedTable preOp = t.partitionBy("Sym");
        final PartitionedTable postOp = summed.partitionBy("Sym");

        String[] columns = Arrays.stream(t.getDefinition().getColumnNamesArray())
                .filter(col -> !col.equals("Sym") && !col.equals("boolCol")).toArray(String[]::new);

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithCumSum(
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
        final CreateResult result = createTestTable(10000, bucketed, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;
        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(UpdateByOperation.CumSum(), "Sym")
                                : t.updateBy(UpdateByOperation.CumSum());
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
        final CreateResult result = createTestTable(100, false, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.CumSum());
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
        final CreateResult result = createTestTable(100, true, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.CumSum(), "Sym");
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

    /*
     * Ideas for specialized tests: 1) Remove first index 2) Removed everything, add some back 3) Make sandwiches
     */
    // endregion

    private long[] boolean_cumsum(Boolean[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        long[] result = new long[values.length];
        result[0] = values[0] == null ? NULL_LONG : (values[0] ? 1 : 0);

        for (int i = 1; i < values.length; i++) {
            final boolean curValNull = values[i] == null;
            if (isNull(result[i - 1])) {
                result[i] = curValNull ? NULL_LONG : (values[i] ? 1 : 0);
            } else {
                if (curValNull) {
                    result[i] = result[i - 1];
                } else {
                    result[i] = result[i - 1] + (values[i] ? 1 : 0);
                }
            }
        }

        return result;
    }

    public static Object[] big_cumSum(Object[] values, final boolean isBD) {
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
                result[i] = ((BigDecimal) result[i - 1]).add((BigDecimal) values[i],
                        UpdateByControl.mathContextDefault());
            } else {
                result[i] = ((BigInteger) result[i - 1]).add((BigInteger) values[i]);
            }
        }

        return result;
    }

    final void assertWithCumSum(@NotNull final Object expected, @NotNull final Object actual, Class type) {
        if (expected instanceof byte[]) {
            assertArrayEquals(Numeric.cumsum((byte[]) expected), (long[]) actual);
        } else if (expected instanceof short[]) {
            assertArrayEquals(Numeric.cumsum((short[]) expected), (long[]) actual);
        } else if (expected instanceof int[]) {
            assertArrayEquals(Numeric.cumsum((int[]) expected), (long[]) actual);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Numeric.cumsum((long[]) expected), (long[]) actual);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Numeric.cumsum((float[]) expected), (double[]) actual, .001f);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Numeric.cumsum((double[]) expected), (double[]) actual, .001d);
        } else if (expected instanceof Boolean[]) {
            assertArrayEquals(boolean_cumsum((Boolean[]) expected), (long[]) actual);
        } else {
            assertArrayEquals(big_cumSum((Object[]) expected, type == BigDecimal.class), (Object[]) actual);
        }
    }
}
