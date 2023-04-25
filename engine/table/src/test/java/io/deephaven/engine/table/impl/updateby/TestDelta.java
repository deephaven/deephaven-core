package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.DeltaControl;
import io.deephaven.api.updateby.NullBehavior;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.SortedDateTimeGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
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

    // region Zero Key Tests

    @Test
    public void testStaticZeroKey() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131,
                new String[] {"timeCol", "charCol"}, new TestDataGenerator[] {
                        new SortedDateTimeGenerator(
                                DateTimeUtils.parseDateTime("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseDateTime("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;
        t.setRefreshing(false);

        final Table result = t.updateBy(UpdateByOperation.Delta(columns));

        for (String col : columns) {
            if ("boolCol".equals(col)) {
                continue;
            }
            assertWithDelta(t.getColumn(col).getDirect(),
                    result.getColumn(col).getDirect(),
                    DeltaControl.DEFAULT);
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
                        new SortedDateTimeGenerator(
                                DateTimeUtils.parseDateTime("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseDateTime("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;
        t.setRefreshing(false);

        final Table summed = t.updateBy(UpdateByOperation.Delta(columns), "Sym");


        final PartitionedTable preOp = t.partitionBy("Sym");
        final PartitionedTable postOp = summed.partitionBy("Sym");

        String[] columns = Arrays.stream(t.getDefinition().getColumnNamesArray())
                .filter(col -> !col.equals("Sym") && !col.equals("boolCol")).toArray(String[]::new);

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithDelta(source.getColumn(col).getDirect(),
                        actual.getColumn(col).getDirect(),
                        DeltaControl.DEFAULT);
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
                        new SortedDateTimeGenerator(
                                DateTimeUtils.parseDateTime("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseDateTime("2022-03-09T16:30:00.000 NY")),
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
            UpdateGraphProcessor.DEFAULT
                    .runWithinUnitTestCycle(() -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    @Test
    public void testZeroKeyGeneralTicking() {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, false, false, true, 0x31313131,
                new String[] {"timeCol", "charCol"}, new TestDataGenerator[] {
                        new SortedDateTimeGenerator(
                                DateTimeUtils.parseDateTime("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseDateTime("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.Delta(columns))),
                EvalNugget.from(() -> t.updateBy(UpdateByOperation.Delta(DeltaControl.NULL_DOMINATES, columns)))
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testBucketedGeneralTicking() {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"timeCol", "charCol"}, new TestDataGenerator[] {
                        new SortedDateTimeGenerator(
                                DateTimeUtils.parseDateTime("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseDateTime("2022-03-09T16:30:00.000 NY")),
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

    /*
     * Ideas for specialized tests: 1) Remove first index 2) Removed everything, add some back 3) Make sandwiches
     */
    // endregion


    private static byte[] delta(@NotNull final byte[] expected, DeltaControl control) {
        final byte[] result = new byte[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (ii == 0 || expected[ii - 1] == NULL_BYTE) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : NULL_BYTE;
            } else {
                result[ii] = expected[ii] == NULL_BYTE ? NULL_BYTE : (byte) (expected[ii] - expected[ii - 1]);
            }
        }

        return result;
    }

    private static char[] delta(@NotNull final char[] expected, DeltaControl control) {
        final char[] result = new char[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (ii == 0 || expected[ii - 1] == NULL_CHAR) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : NULL_CHAR;
            } else {
                result[ii] = expected[ii] == NULL_CHAR ? NULL_CHAR : (char) (expected[ii] - expected[ii - 1]);
            }
        }

        return result;
    }

    private static short[] delta(@NotNull final short[] expected, DeltaControl control) {
        final short[] result = new short[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (ii == 0 || expected[ii - 1] == NULL_SHORT) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : NULL_SHORT;
            } else {
                result[ii] = expected[ii] == NULL_SHORT ? NULL_SHORT : (short) (expected[ii] - expected[ii - 1]);
            }
        }

        return result;
    }

    private static int[] delta(@NotNull final int[] expected, DeltaControl control) {
        final int[] result = new int[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (ii == 0 || expected[ii - 1] == NULL_INT) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : NULL_INT;
            } else {
                result[ii] = expected[ii] == NULL_INT ? NULL_INT : expected[ii] - expected[ii - 1];
            }
        }

        return result;
    }

    private static long[] delta(@NotNull final long[] expected, DeltaControl control) {
        final long[] result = new long[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (ii == 0 || expected[ii - 1] == NULL_LONG) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : NULL_LONG;
            } else {
                result[ii] = expected[ii] == NULL_LONG ? NULL_LONG : expected[ii] - expected[ii - 1];
            }
        }

        return result;
    }

    private static float[] delta(@NotNull final float[] expected, DeltaControl control) {
        final float[] result = new float[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (ii == 0 || expected[ii - 1] == NULL_FLOAT) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : NULL_FLOAT;
            } else {
                result[ii] = expected[ii] == NULL_FLOAT ? NULL_FLOAT : expected[ii] - expected[ii - 1];
            }
        }

        return result;
    }

    private static double[] delta(@NotNull final double[] expected, DeltaControl control) {
        final double[] result = new double[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (ii == 0 || expected[ii - 1] == NULL_DOUBLE) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : NULL_DOUBLE;
            } else {
                result[ii] = expected[ii] == NULL_DOUBLE ? NULL_DOUBLE : expected[ii] - expected[ii - 1];
            }
        }

        return result;
    }


    private static Object[] delta(@NotNull final Object[] expected, DeltaControl control) {
        final Object[] result = new Object[expected.length];
        for (int ii = 0; ii < expected.length; ii++) {
            if (ii == 0 || expected[ii - 1] == null) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? expected[ii]
                        : null;
            } else {
                if (expected[ii] == null) {
                    result[ii] = null;
                } else if (expected[ii] instanceof BigInteger) {
                    result[ii] = ((BigInteger) expected[ii]).subtract((BigInteger) expected[ii - 1]);
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
            if (ii == 0 || expected[ii - 1] == null) {
                result[ii] = control.nullBehavior() == NullBehavior.ValueDominates
                        ? ((DateTime) expected[ii]).getNanos()
                        : NULL_LONG;
            } else {
                if (expected[ii] == null) {
                    result[ii] = NULL_LONG;
                } else {
                    result[ii] = ((DateTime) expected[ii]).getNanos() - ((DateTime) expected[ii - 1]).getNanos();
                }
            }
        }

        return result;
    }

    final void assertWithDelta(final @NotNull Object expected, final @NotNull Object actual, DeltaControl control) {

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
        } else if (((Object[]) expected).length > 0 && ((Object[]) expected)[0] instanceof DateTime) {
            assertArrayEquals(deltaTime((Object[]) expected, control), (long[]) actual);
        } else if (((Object[]) expected).length > 0) {
            assertArrayEquals(delta((Object[]) expected, control), (Object[]) actual);
        }
    }
}
