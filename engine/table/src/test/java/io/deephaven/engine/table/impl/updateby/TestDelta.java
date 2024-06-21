//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.DeltaControl;
import io.deephaven.api.updateby.NullBehavior;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.SortedInstantGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.util.QueryConstants.*;
import static org.junit.Assert.assertArrayEquals;

@Category(OutOfBandTest.class)
public class TestDelta extends BaseUpdateByTest {
    /**
     * These are used in the ticking table evaluations where we verify dynamic vs static tables.
     */
    final String[] columns = new String[] {
            "timeCol",
            "charCol",
            "byteCol",
            "shortCol",
            "intCol",
            "longCol",
            "floatCol",
            "doubleCol",
            "bigIntCol",
            "bigDecimalCol",
    };

    final int STATIC_TABLE_SIZE = 10_000;
    final int DYNAMIC_TABLE_SIZE = 1_000;
    final int DYNAMIC_UPDATE_SIZE = 100;
    final int DYNAMIC_UPDATE_STEPS = 20;

    @Test
    public void testManualVerification() {
        final int[] inputData = {100, 101, 102, NULL_INT, 103, 104};

        final int[] outputNullDominates = {NULL_INT, 1, 1, NULL_INT, NULL_INT, 1};
        final int[] outputValueDominates = {100, 1, 1, NULL_INT, 103, 1};
        final int[] outputZeroDominates = {0, 1, 1, NULL_INT, 0, 1};

        final QueryTable table = TstUtils.testRefreshingTable(
                intCol("Int", inputData));

        // default is NULL_DOMINATES
        QueryTable result = (QueryTable) table.updateBy(List.of(UpdateByOperation.Delta()));
        assertArrayEquals(ColumnVectors.ofInt(result, "Int").toArray(), outputNullDominates);

        result = (QueryTable) table.updateBy(List.of(UpdateByOperation.Delta(DeltaControl.NULL_DOMINATES)));
        assertArrayEquals(ColumnVectors.ofInt(result, "Int").toArray(), outputNullDominates);

        result = (QueryTable) table.updateBy(List.of(UpdateByOperation.Delta(DeltaControl.VALUE_DOMINATES)));
        assertArrayEquals(ColumnVectors.ofInt(result, "Int").toArray(), outputValueDominates);

        result = (QueryTable) table.updateBy(List.of(UpdateByOperation.Delta(DeltaControl.ZERO_DOMINATES)));
        assertArrayEquals(ColumnVectors.ofInt(result, "Int").toArray(), outputZeroDominates);
    }

    // region Zero Key Tests

    @Test
    public void testStaticZeroKey() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131,
                new String[] {"timeCol", "charCol"}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;
        t.setRefreshing(false);

        Table result = t.updateBy(UpdateByOperation.Delta(columns));

        for (String col : columns) {
            if ("boolCol".equals(col)) {
                continue;
            }
            assertWithDelta(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(result, col).toArray(),
                    DeltaControl.DEFAULT);
        }

        result = t.updateBy(UpdateByOperation.Delta(DeltaControl.NULL_DOMINATES, columns));

        for (String col : columns) {
            if ("boolCol".equals(col)) {
                continue;
            }
            assertWithDelta(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(result, col).toArray(),
                    DeltaControl.NULL_DOMINATES);
        }

        result = t.updateBy(UpdateByOperation.Delta(DeltaControl.VALUE_DOMINATES, columns));

        for (String col : columns) {
            if ("boolCol".equals(col)) {
                continue;
            }
            assertWithDelta(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(result, col).toArray(),
                    DeltaControl.VALUE_DOMINATES);
        }

        result = t.updateBy(UpdateByOperation.Delta(DeltaControl.ZERO_DOMINATES, columns));

        for (String col : columns) {
            if ("boolCol".equals(col)) {
                continue;
            }
            assertWithDelta(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(result, col).toArray(),
                    DeltaControl.ZERO_DOMINATES);
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
        final QueryTable t = createTestTable(100000, true, grouped, false, 0x31313131,
                new String[] {"timeCol", "charCol"}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;
        t.setRefreshing(false);

        final String[] columns = Arrays.stream(t.getDefinition().getColumnNamesArray())
                .filter(col -> !col.equals("Sym") && !col.equals("boolCol")).toArray(String[]::new);

        Table result = t.updateBy(UpdateByOperation.Delta(columns), "Sym");

        PartitionedTable preOp = t.partitionBy("Sym");
        PartitionedTable postOp = result.partitionBy("Sym");

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithDelta(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        DeltaControl.DEFAULT);
            });
            return source;
        });

        result = t.updateBy(UpdateByOperation.Delta(DeltaControl.NULL_DOMINATES, columns), "Sym");

        preOp = t.partitionBy("Sym");
        postOp = result.partitionBy("Sym");

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithDelta(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        DeltaControl.NULL_DOMINATES);
            });
            return source;
        });

        result = t.updateBy(UpdateByOperation.Delta(DeltaControl.VALUE_DOMINATES, columns), "Sym");

        preOp = t.partitionBy("Sym");
        postOp = result.partitionBy("Sym");

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithDelta(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        DeltaControl.VALUE_DOMINATES);
            });
            return source;
        });

        result = t.updateBy(UpdateByOperation.Delta(DeltaControl.ZERO_DOMINATES, columns), "Sym");

        preOp = t.partitionBy("Sym");
        postOp = result.partitionBy("Sym");

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithDelta(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        DeltaControl.ZERO_DOMINATES);
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
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"timeCol", "charCol"}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;
        t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.Delta(columns), "Sym")
                        : t.updateBy(UpdateByOperation.Delta(columns))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.Delta(DeltaControl.NULL_DOMINATES, columns), "Sym")
                        : t.updateBy(UpdateByOperation.Delta(DeltaControl.NULL_DOMINATES, columns))),
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    @Test
    public void testZeroKeyGeneralTicking() {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, false, false, true, 0x31313131,
                new String[] {"timeCol", "charCol"}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.Delta(columns))),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.Delta(DeltaControl.NULL_DOMINATES, columns)))
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testBucketedGeneralTicking() {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"timeCol", "charCol"}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.Delta(columns), "Sym")),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.Delta(DeltaControl.NULL_DOMINATES, columns), "Sym"))
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            try {
                simulateShiftAwareStep(DYNAMIC_UPDATE_SIZE, billy, t, result.infos, nuggets);
            } catch (Throwable ex) {
                System.out.println("Crapped out on step " + ii);
                throw ex;
            }
        }
    }

    @Test
    public void testDHC5040() {
        // ensure that Instant columns are handled correctly (interpreted to longs internally).
        try (final SafeCloseable ignored = ExecutionContext.newBuilder()
                .newQueryLibrary("DEFAULT")
                .captureQueryCompiler()
                .captureQueryScope()
                .build().open();) {
            ExecutionContext.getContext().getQueryLibrary().importStatic(TableTools.class);

            final Random billy = new Random(0xB177B177);
            int size = 1;

            final ColumnInfo<?, ?>[] infos = initColumnInfos(
                    new String[] {"Timestamp"},
                    new SortedInstantGenerator(DateTimeUtils.parseInstant("2015-09-11T09:30:00 NY"),
                            DateTimeUtils.parseInstant("2015-09-11T10:00:00 NY")));

            final QueryTable timeTable = getTable(false, size, billy, infos);
            timeTable.setRefreshing(true);

            final Table st = timeTable
                    .update("Minutes=(long)(epochNanos(Timestamp)/60_000_000_000)",
                            "TimestampTable=newTable(longCol(\"Minutes\", Minutes), instantCol(\"Timestamp2\", Timestamp)).updateView(\"Timestamp2=Timestamp2+1\")")
                    .dropColumns("Timestamp");

            final TableDefinition td = TableDefinition.of(
                    ColumnDefinition.ofTime("Timestamp2"),
                    ColumnDefinition.ofLong("Minutes"));

            final PartitionedTable pt = PartitionedTableFactory.of(
                    st,
                    Collections.singleton("Minutes"),
                    false,
                    "TimestampTable",
                    td,
                    true);

            final Table mt = pt.merge();

            final Table ut = mt.updateBy(UpdateByOperation.Delta("NanosDelta=Timestamp2"), "Minutes");

            for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
                ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                        () -> generateAppends(1, billy, timeTable, infos));
                Assert.eqFalse(st.isFailed(), "st.isFailed()");
                Assert.eqFalse(ut.isFailed(), "ut.isFailed()");
                Assert.eq(st.size(), "st.size()", ut.size(), "ut.size()");
            }
        }
    }

    /*
     * Ideas for specialized tests: 1) Remove first index 2) Removed everything, add some back 3) Make sandwiches
     */
    // endregion


    private static byte[] delta(@NotNull final byte[] expected, DeltaControl control) {
        final byte[] result = new byte[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (expected[ii] == NULL_BYTE) {
                result[ii] = NULL_BYTE;
            } else if (ii == 0 || expected[ii - 1] == NULL_BYTE) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : (control.nullBehavior() == NullBehavior.NullDominates
                                ? NULL_BYTE
                                : 0); // ZeroDominates
            } else {
                result[ii] = (byte) (expected[ii] - expected[ii - 1]);
            }
        }

        return result;
    }

    private static char[] delta(@NotNull final char[] expected, DeltaControl control) {
        final char[] result = new char[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (expected[ii] == NULL_CHAR) {
                result[ii] = NULL_CHAR;
            } else if (ii == 0 || expected[ii - 1] == NULL_CHAR) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : (control.nullBehavior() == NullBehavior.NullDominates
                                ? NULL_CHAR
                                : 0); // ZeroDominates
            } else {
                result[ii] = (char) (expected[ii] - expected[ii - 1]);
            }
        }

        return result;
    }

    private static short[] delta(@NotNull final short[] expected, DeltaControl control) {
        final short[] result = new short[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (expected[ii] == NULL_SHORT) {
                result[ii] = NULL_SHORT;
            } else if (ii == 0 || expected[ii - 1] == NULL_SHORT) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : (control.nullBehavior() == NullBehavior.NullDominates
                                ? NULL_SHORT
                                : 0); // ZeroDominates
            } else {
                result[ii] = (short) (expected[ii] - expected[ii - 1]);
            }
        }

        return result;
    }

    private static int[] delta(@NotNull final int[] expected, DeltaControl control) {
        final int[] result = new int[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (expected[ii] == NULL_INT) {
                result[ii] = NULL_INT;
            } else if (ii == 0 || expected[ii - 1] == NULL_INT) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : (control.nullBehavior() == NullBehavior.NullDominates
                                ? NULL_INT
                                : 0); // ZeroDominates
            } else {
                result[ii] = (int) (expected[ii] - expected[ii - 1]);
            }
        }

        return result;
    }

    private static long[] delta(@NotNull final long[] expected, DeltaControl control) {
        final long[] result = new long[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (expected[ii] == NULL_LONG) {
                result[ii] = NULL_LONG;
            } else if (ii == 0 || expected[ii - 1] == NULL_LONG) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : (control.nullBehavior() == NullBehavior.NullDominates
                                ? NULL_LONG
                                : 0); // ZeroDominates
            } else {
                result[ii] = (long) (expected[ii] - expected[ii - 1]);
            }
        }

        return result;
    }

    private static float[] delta(@NotNull final float[] expected, DeltaControl control) {
        final float[] result = new float[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (expected[ii] == NULL_FLOAT) {
                result[ii] = NULL_FLOAT;
            } else if (ii == 0 || expected[ii - 1] == NULL_FLOAT) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : (control.nullBehavior() == NullBehavior.NullDominates
                                ? NULL_FLOAT
                                : 0); // ZeroDominates
            } else {
                result[ii] = (float) (expected[ii] - expected[ii - 1]);
            }
        }

        return result;
    }

    private static double[] delta(@NotNull final double[] expected, DeltaControl control) {
        final double[] result = new double[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (expected[ii] == NULL_DOUBLE) {
                result[ii] = NULL_DOUBLE;
            } else if (ii == 0 || expected[ii - 1] == NULL_DOUBLE) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : (control.nullBehavior() == NullBehavior.NullDominates
                                ? NULL_DOUBLE
                                : 0); // ZeroDominates
            } else {
                result[ii] = (double) (expected[ii] - expected[ii - 1]);
            }
        }

        return result;
    }


    private static Object[] delta(@NotNull final Object[] expected, DeltaControl control) {
        final Object[] result = new Object[expected.length];

        for (int ii = 0; ii < expected.length; ii++) {
            if (expected[ii] == null) {
                result[ii] = null;
            } else if (expected[ii] instanceof BigInteger) {
                if (ii == 0 || expected[ii - 1] == null) {
                    result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                            ? expected[ii]
                            : (control.nullBehavior() == NullBehavior.NullDominates
                                    ? null
                                    : BigInteger.ZERO); // ZeroDominates
                } else {
                    result[ii] = ((BigInteger) expected[ii]).subtract((BigInteger) expected[ii - 1]);
                }
            } else {
                if (ii == 0 || expected[ii - 1] == null) {
                    result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                            ? expected[ii]
                            : (control.nullBehavior() == NullBehavior.NullDominates
                                    ? null
                                    : BigDecimal.ZERO); // ZeroDominates
                } else {
                    result[ii] = ((BigDecimal) expected[ii]).subtract((BigDecimal) expected[ii - 1]);
                }
            }
        }
        return result;
    }

    private static long[] deltaTime(@NotNull final Object[] expected, DeltaControl control) {
        final long[] result = new long[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (expected[ii] == null) {
                result[ii] = NULL_LONG;
            } else if (ii == 0 || expected[ii - 1] == null) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? DateTimeUtils.epochNanos(((Instant) expected[ii]))
                        : (control.nullBehavior() == NullBehavior.NullDominates
                                ? NULL_LONG
                                : 0); // ZeroDominates
            } else {
                result[ii] = DateTimeUtils.epochNanos(((Instant) expected[ii]))
                        - DateTimeUtils.epochNanos(((Instant) expected[ii - 1]));
            }
        }

        return result;
    }

    final void assertWithDelta(@NotNull final Object expected, @NotNull final Object actual, DeltaControl control) {

        if (expected instanceof byte[]) {
            assertArrayEquals(delta((byte[]) expected, control), (byte[]) actual);
        } else if (expected instanceof char[]) {
            assertArrayEquals(delta((char[]) expected, control), (char[]) actual);
        } else if (expected instanceof short[]) {
            assertArrayEquals(delta((short[]) expected, control), (short[]) actual);
        } else if (expected instanceof int[]) {
            assertArrayEquals(delta((int[]) expected, control), (int[]) actual);
        } else if (expected instanceof long[]) {
            assertArrayEquals(delta((long[]) expected, control), (long[]) actual);
        } else if (expected instanceof float[]) {
            assertArrayEquals(delta((float[]) expected, control), (float[]) actual, .001f);
        } else if (expected instanceof double[]) {
            assertArrayEquals(delta((double[]) expected, control), (double[]) actual, .001d);
        } else if (((Object[]) expected).length > 0 && ((Object[]) expected)[0] instanceof Instant) {
            assertArrayEquals(deltaTime((Object[]) expected, control), (long[]) actual);
        } else if (((Object[]) expected).length > 0) {
            assertArrayEquals(delta((Object[]) expected, control), (Object[]) actual);
        }
    }
}
