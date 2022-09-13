package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTime;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.table.impl.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.table.impl.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.table.impl.TstUtils.assertTableEquals;
import static io.deephaven.engine.table.impl.TstUtils.testTable;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.function.Basic.isNull;
import static io.deephaven.time.DateTimeUtils.MINUTE;
import static io.deephaven.time.DateTimeUtils.convertDateTime;
import static io.deephaven.util.QueryConstants.*;
import static org.junit.Assert.assertArrayEquals;

@Category(OutOfBandTest.class)
public class TestRollingSum extends BaseUpdateByTest {
    // region Zero Key Tests

    @Test
    public void testStaticZeroKey() {
        final QueryTable t = createTestTable(10000, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        int prevTicks = 100;
        int postTicks = 0;

        final Table summed = t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks));
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTicks(t.getColumn(col).getDirect(), summed.getColumn(col).getDirect(),
                    summed.getColumn(col).getType(), prevTicks, postTicks);
        }
    }

    @Test
    public void testStaticZeroKeyFwdWindow() {
        final QueryTable t = createTestTable(10000, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        int prevTicks = 0;
        int postTicks = 100;

        final Table summed = t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks));
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTicks(t.getColumn(col).getDirect(), summed.getColumn(col).getDirect(),
                    summed.getColumn(col).getType(), prevTicks, postTicks);
        }
    }

    @Test
    public void testStaticZeroKeyFwdRevWindow() {
        final QueryTable t = createTestTable(10000, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        int prevTicks = 100;
        int postTicks = 100;

        final Table summed = t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks));
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTicks(t.getColumn(col).getDirect(), summed.getColumn(col).getDirect(),
                    summed.getColumn(col).getType(), prevTicks, postTicks);
        }
    }

    @Test
    public void testStaticZeroKeyTimed() {
        final QueryTable t = createTestTable(10000, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TstUtils.Generator[] {new TstUtils.SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))}).t;

        final OperationControl skipControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.SKIP)
                .onNanValue(BadDataBehavior.SKIP).build();

        final OperationControl resetControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.RESET)
                .onNanValue(BadDataBehavior.RESET).build();

        Duration prevTime = Duration.ofMinutes(10);
        Duration postTime = Duration.ZERO;

        final Table summed =
                t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime, "byteCol", "shortCol", "intCol", "longCol", "floatCol",
                        "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"
                ));


        DateTime[] ts = (DateTime[])t.getColumn("ts").getDirect();
        long[] timestamps = new long[t.intSize()];
        for (int i = 0; i < t.intSize(); i++) {
            timestamps[i] = ts[i].getNanos();
        }

        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTime(t.getColumn(col).getDirect(), summed.getColumn(col).getDirect(), timestamps,
                    summed.getColumn(col).getType(), prevTime.toNanos(), postTime.toNanos());
        }
    }

    @Test
    public void testStaticZeroKeyFwdWindowTimed() {
        final QueryTable t = createTestTable(10000, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TstUtils.Generator[] {new TstUtils.SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))}).t;

        final OperationControl skipControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.SKIP)
                .onNanValue(BadDataBehavior.SKIP).build();

        final OperationControl resetControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.RESET)
                .onNanValue(BadDataBehavior.RESET).build();

        Duration prevTime = Duration.ZERO;
        Duration postTime = Duration.ofMinutes(10);

        final Table summed =
                t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime, "byteCol", "shortCol", "intCol", "longCol", "floatCol",
                        "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"
                ));


        DateTime[] ts = (DateTime[])t.getColumn("ts").getDirect();
        long[] timestamps = new long[t.intSize()];
        for (int i = 0; i < t.intSize(); i++) {
            timestamps[i] = ts[i].getNanos();
        }

        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTime(t.getColumn(col).getDirect(), summed.getColumn(col).getDirect(), timestamps,
                    summed.getColumn(col).getType(), prevTime.toNanos(), postTime.toNanos());
        }
    }

    @Test
    public void testStaticZeroKeyFwdRevWindowTimed() {
        final QueryTable t = createTestTable(10000, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TstUtils.Generator[] {new TstUtils.SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))}).t;

        final OperationControl skipControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.SKIP)
                .onNanValue(BadDataBehavior.SKIP).build();

        final OperationControl resetControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.RESET)
                .onNanValue(BadDataBehavior.RESET).build();

        Duration prevTime = Duration.ofMinutes(10);
        Duration postTime = Duration.ofMinutes(10);

        final Table summed =
                t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime, "byteCol", "shortCol", "intCol", "longCol", "floatCol",
                        "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"
                ));


        DateTime[] ts = (DateTime[])t.getColumn("ts").getDirect();
        long[] timestamps = new long[t.intSize()];
        for (int i = 0; i < t.intSize(); i++) {
            timestamps[i] = ts[i].getNanos();
        }

        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTime(t.getColumn(col).getDirect(), summed.getColumn(col).getDirect(), timestamps,
                    summed.getColumn(col).getType(), prevTime.toNanos(), postTime.toNanos());
        }
    }

    // endregion

    // region Bucketed Tests

    @Test
    public void testNullOnBucketChange() throws IOException {
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
                UpdateByOperation.RollingSum(100, "ByteValSum=ByteVal"),
                UpdateByOperation.RollingSum(100, "ShortValSum=ShortVal"),
                UpdateByOperation.RollingSum(100, "IntValSum=IntVal")), "Sym");

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

        int prevTicks = 100;
        int postTicks = 10;

        final Table summed =
                t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks, "byteCol", "shortCol", "intCol", "longCol", "floatCol",
                        "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"
                ), "Sym");


        final PartitionedTable preOp = t.partitionBy("Sym");
        final PartitionedTable postOp = summed.partitionBy("Sym");

        String[] columns = t.getDefinition().getColumnStream().map(ColumnDefinition::getName).toArray(String[]::new);

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithRollingSumTicks(source.getColumn(col).getDirect(), actual.getColumn(col).getDirect(),
                        actual.getColumn(col).getType(), prevTicks, postTicks);
            });
            return source;
        });
    }

    @Test
    public void testStaticBucketedTimed() {
        doTestStaticBucketedTimed(false, Duration.ofMinutes(10), Duration.ZERO);
    }

    @Test
    public void testStaticBucketedFwdWindowTimed() {
        doTestStaticBucketedTimed(false, Duration.ZERO, Duration.ofMinutes(10));
    }

    @Test
    public void testStaticBucketedFwdRevWindowTimed() {
        doTestStaticBucketedTimed(false, Duration.ofMinutes(10), Duration.ofMinutes(10));
    }

    private void doTestStaticBucketedTimed(boolean grouped, Duration prevTime, Duration postTime) {
        final QueryTable t = createTestTable(10000, true, grouped, false, 0xFFFABBBC,
                new String[] {"ts"}, new TstUtils.Generator[] {new TstUtils.SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))}).t;

        final Table summed =
                t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime, "byteCol", "shortCol", "intCol", "longCol", "floatCol",
                        "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"
                ), "Sym");


        final PartitionedTable preOp = t.partitionBy("Sym");
        final PartitionedTable postOp = summed.partitionBy("Sym");

        String[] columns = t.getDefinition().getColumnStream().map(ColumnDefinition::getName).toArray(String[]::new);

        preOp.partitionedTransform(postOp, (source, actual) -> {
            DateTime[] ts = (DateTime[])source.getColumn("ts").getDirect();
            long[] timestamps = new long[source.intSize()];
            for (int i = 0; i < source.intSize(); i++) {
                timestamps[i] = ts[i].getNanos();
            }
            Arrays.stream(columns).forEach(col -> {
                assertWithRollingSumTime(source.getColumn(col).getDirect(), actual.getColumn(col).getDirect(), timestamps,
                        actual.getColumn(col).getType(), prevTime.toNanos(), postTime.toNanos());
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
                        return bucketed ? t.updateBy(UpdateByOperation.RollingSum(100), "Sym")
                                : t.updateBy(UpdateByOperation.RollingSum(100));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < 100; ii++) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> generateAppends(100, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    @Test
    public void testZeroKeyAppendOnlyTimed() {
        doTestAppendOnlyTimed(false);
    }

    @Test
    public void testBucketedAppendOnlyTimed() {
        doTestAppendOnlyTimed(true);
    }


    private void doTestAppendOnlyTimed(boolean bucketed) {
        final CreateResult result = createTestTable(10000, bucketed, false, true, 0x31313131,
                new String[] {"ts"}, new TstUtils.Generator[] {new TstUtils.SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))});
        final QueryTable t = result.t;
        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        Duration prevTime = Duration.ofMinutes(10);
        Duration postTime = Duration.ZERO;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime), "Sym")
                                : t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < 100; ii++) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> generateAppends(100, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    @Test
    public void testZeroKeyGeneralTicking() {
        final CreateResult result = createTestTable(10000, false, false, true, 0x31313131);
        final QueryTable t = result.t;

        final long prevTicks = 100;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum(prevTicks));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < 100; ii++) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(100, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testZeroKeyGeneralTickingFwdWindow() {
        final CreateResult result = createTestTable(10000, false, false, true, 0x31313131);
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum(100));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < 100; ii++) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
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
                        return t.updateBy(UpdateByOperation.RollingSum(100), "Sym");
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

    // implement these calculations as pure rolling sums with local storage

    private long[] rollingSum(byte[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        long[] result = new long[values.length];


        for (int i = 0; i < values.length; i++) {
            result[i] = NULL_LONG;

            // set the head and the tail
            final int head = Math.max(0, i - prevTicks + 1);
            final int tail = Math.min(values.length - 1, i + postTicks);

            // compute everything in this window
            for (int computeIdx = head; computeIdx <= tail; computeIdx++) {
                if (!isNull(values[computeIdx])) {
                    if (result[i] == NULL_LONG) {
                        result[i] = values[computeIdx];
                    } else {
                        result[i] += values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private long[] rollingSum(short[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        long[] result = new long[values.length];


        for (int i = 0; i < values.length; i++) {
            result[i] = NULL_LONG;

            // set the head and the tail
            final int head = Math.max(0, i - prevTicks + 1);
            final int tail = Math.min(values.length - 1, i + postTicks);


            // compute everything in this window
            for (int computeIdx = head; computeIdx <= tail; computeIdx++) {
                if (!isNull(values[computeIdx])) {
                    if (result[i] == NULL_LONG) {
                        result[i] = values[computeIdx];
                    } else {
                        result[i] += values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private long[] rollingSum(int[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        long[] result = new long[values.length];


        for (int i = 0; i < values.length; i++) {
            result[i] = NULL_LONG;

            // set the head and the tail
            final int head = Math.max(0, i - prevTicks + 1);
            final int tail = Math.min(values.length - 1, i + postTicks);

            // compute everything in this window
            for (int computeIdx = head; computeIdx <= tail; computeIdx++) {
                if (!isNull(values[computeIdx])) {
                    if (result[i] == NULL_LONG) {
                        result[i] = values[computeIdx];
                    } else {
                        result[i] += values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private long[] rollingSum(long[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        long[] result = new long[values.length];


        for (int i = 0; i < values.length; i++) {
            result[i] = NULL_LONG;

            // set the head and the tail
            final int head = Math.max(0, i - prevTicks + 1);
            final int tail = Math.min(values.length - 1, i + postTicks);

            // compute everything in this window
            for (int computeIdx = head; computeIdx <= tail; computeIdx++) {
                if (!isNull(values[computeIdx])) {
                    if (result[i] == NULL_LONG) {
                        result[i] = values[computeIdx];
                    } else {
                        result[i] += values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private float[] rollingSum(float[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new float[0];
        }

        float[] result = new float[values.length];


        for (int i = 0; i < values.length; i++) {
            result[i] = NULL_FLOAT;

            // set the head and the tail
            final int head = Math.max(0, i - prevTicks + 1);
            final int tail = Math.min(values.length - 1, i + postTicks);

            // compute everything in this window
            for (int computeIdx = head; computeIdx <= tail; computeIdx++) {
                if (!isNull(values[computeIdx])) {
                    if (result[i] == NULL_FLOAT) {
                        result[i] = values[computeIdx];
                    } else {
                        result[i] += values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private double[] rollingSum(double[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new double[0];
        }

        double[] result = new double[values.length];


        for (int i = 0; i < values.length; i++) {
            result[i] = NULL_DOUBLE;

            // set the head and the tail
            final int head = Math.max(0, i - prevTicks + 1);
            final int tail = Math.min(values.length - 1, i + postTicks);

            // compute everything in this window
            for (int computeIdx = head; computeIdx <= tail; computeIdx++) {
                if (!isNull(values[computeIdx])) {
                    if (result[i] == NULL_DOUBLE) {
                        result[i] = values[computeIdx];
                    } else {
                        result[i] += values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private long[] rollingSum(Boolean[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        long[] result = new long[values.length];


        for (int i = 0; i < values.length; i++) {
            result[i] = NULL_LONG;

            // set the head and the tail
            final int head = Math.max(0, i - prevTicks + 1);
            final int tail = Math.min(values.length - 1, i + postTicks);

            // compute everything in this window
            for (int computeIdx = head; computeIdx <= tail; computeIdx++) {
                if (!isNull(values[computeIdx])) {
                    if (result[i] == NULL_LONG) {
                        result[i] = values[computeIdx] ? 1 : 0;
                    } else {
                        result[i] += (values[computeIdx] ? 1 : 0);
                    }
                }
            }
        }

        return result;
    }

    public static Object[] rollingSum(Object[] values, final boolean isBD, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new Object[0];
        }

        Object[] result = new Object[values.length];


        for (int i = 0; i < values.length; i++) {
            result[i] = null;

            // set the head and the tail
            final int head = Math.max(0, i - prevTicks + 1);
            final int tail = Math.min(values.length - 1, i + postTicks);

            // compute everything in this window
            for (int computeIdx = head; computeIdx <= tail; computeIdx++) {
                if (values[computeIdx] != null) {
                    if (result[i] == null) {
                        result[i] = values[computeIdx];
                    } else {
                        if (isBD) {
                            result[i] = ((BigDecimal) result[i]).add((BigDecimal) values[computeIdx],
                                    UpdateByControl.mathContextDefault());
                        } else {
                            result[i] = ((BigInteger) result[i]).add((BigInteger) values[computeIdx]);
                        }
                    }
                }
            }
        }

        return result;
    }

    private long[] rollingSumTime(byte[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        long[] result = new long[values.length];

        int head = 0;
        int tail = 0;

        for (int i = 0; i < values.length; i++) {
            result[i] = NULL_LONG;

            // check the current timestamp. skip if NULL
            if (timestamps[i] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[i] - prevNanos;
            final long tailTime = timestamps[i] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                tail++;
            }

            // compute everything in this window
            for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                if (!isNull(values[computeIdx])) {
                    if (result[i] == NULL_LONG) {
                        result[i] = values[computeIdx];
                    } else {
                        result[i] += values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private long[] rollingSumTime(short[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        long[] result = new long[values.length];

        int head = 0;
        int tail = 0;

        for (int i = 0; i < values.length; i++) {
            result[i] = NULL_LONG;

            // check the current timestamp. skip if NULL
            if (timestamps[i] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[i] - prevNanos;
            final long tailTime = timestamps[i] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                tail++;
            }

            // compute everything in this window
            for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                if (!isNull(values[computeIdx])) {
                    if (result[i] == NULL_LONG) {
                        result[i] = values[computeIdx];
                    } else {
                        result[i] += values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private long[] rollingSumTime(int[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        long[] result = new long[values.length];

        int head = 0;
        int tail = 0;

        for (int i = 0; i < values.length; i++) {
            result[i] = NULL_LONG;

            // check the current timestamp. skip if NULL
            if (timestamps[i] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[i] - prevNanos;
            final long tailTime = timestamps[i] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                tail++;
            }

            // compute everything in this window
            for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                if (!isNull(values[computeIdx])) {
                    if (result[i] == NULL_LONG) {
                        result[i] = values[computeIdx];
                    } else {
                        result[i] += values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private long[] rollingSumTime(long[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        long[] result = new long[values.length];

        int head = 0;
        int tail = 0;

        for (int i = 0; i < values.length; i++) {
            result[i] = NULL_LONG;

            // check the current timestamp. skip if NULL
            if (timestamps[i] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[i] - prevNanos;
            final long tailTime = timestamps[i] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                tail++;
            }

            // compute everything in this window
            for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                if (!isNull(values[computeIdx])) {
                    if (result[i] == NULL_LONG) {
                        result[i] = values[computeIdx];
                    } else {
                        result[i] += values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private float[] rollingSumTime(float[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new float[0];
        }

        float[] result = new float[values.length];

        int head = 0;
        int tail = 0;

        for (int i = 0; i < values.length; i++) {
            result[i] = NULL_FLOAT;

            // check the current timestamp. skip if NULL
            if (timestamps[i] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[i] - prevNanos;
            final long tailTime = timestamps[i] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                tail++;
            }

            // compute everything in this window
            for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                if (!isNull(values[computeIdx])) {
                    if (result[i] == NULL_FLOAT) {
                        result[i] = values[computeIdx];
                    } else {
                        result[i] += values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private double[] rollingSumTime(double[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new double[0];
        }

        double[] result = new double[values.length];

        int head = 0;
        int tail = 0;

        for (int i = 0; i < values.length; i++) {
            result[i] = NULL_DOUBLE;

            // check the current timestamp. skip if NULL
            if (timestamps[i] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[i] - prevNanos;
            final long tailTime = timestamps[i] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                tail++;
            }

            // compute everything in this window
            for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                if (!isNull(values[computeIdx])) {
                    if (result[i] == NULL_DOUBLE) {
                        result[i] = values[computeIdx];
                    } else {
                        result[i] += values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private long[] rollingSumTime(Boolean[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0];
        }

        long[] result = new long[values.length];

        int head = 0;
        int tail = 0;

        for (int i = 0; i < values.length; i++) {
            result[i] = NULL_LONG;

            // check the current timestamp. skip if NULL
            if (timestamps[i] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[i] - prevNanos;
            final long tailTime = timestamps[i] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                tail++;
            }

            // compute everything in this window
            for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                if (!isNull(values[computeIdx])) {
                    if (result[i] == NULL_LONG) {
                        result[i] = values[computeIdx] ? 1 : 0;
                    } else {
                        result[i] += (values[computeIdx] ? 1 : 0);
                    }
                }
            }
        }

        return result;
    }

    private Object[] rollingSumTime(Object[] values, long[] timestamps, final boolean isBD, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new Object[0];
        }

        Object[] result = new Object[values.length];

        int head = 0;
        int tail = 0;

        for (int i = 0; i < values.length; i++) {
            result[i] = null;

            // check the current timestamp. skip if NULL
            if (timestamps[i] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[i] - prevNanos;
            final long tailTime = timestamps[i] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                tail++;
            }

            // compute everything in this window
            for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                if (values[computeIdx] != null) {
                    if (result[i] == null) {
                        result[i] = values[computeIdx];
                    } else {
                        if (isBD) {
                            result[i] = ((BigDecimal) result[i]).add((BigDecimal) values[computeIdx],
                                    UpdateByControl.mathContextDefault());
                        } else {
                            result[i] = ((BigInteger) result[i]).add((BigInteger) values[computeIdx]);
                        }
                    }
                }
            }
        }

        return result;
    }


    final void assertWithRollingSumTicks(final @NotNull Object expected, final @NotNull Object actual, Class type, int prevTicks, int postTicks) {
        // looking for gross errors like missing entries (NOTE: pairwise results are more accurate than true rolling)
        final float deltaF = .02f;
        final double deltaD = .02d;

        if (expected instanceof byte[]) {
            assertArrayEquals(rollingSum((byte[]) expected, prevTicks, postTicks), (long[]) actual);
        } else if (expected instanceof short[]) {
            assertArrayEquals(rollingSum((short[]) expected, prevTicks, postTicks), (long[]) actual);
        } else if (expected instanceof int[]) {
            assertArrayEquals(rollingSum((int[]) expected, prevTicks, postTicks), (long[]) actual);
        } else if (expected instanceof long[]) {
            assertArrayEquals(rollingSum((long[]) expected, prevTicks, postTicks), (long[]) actual);
        } else if (expected instanceof float[]) {
            assertArrayEquals(rollingSum((float[]) expected, prevTicks, postTicks), (float[]) actual, deltaF);
        } else if (expected instanceof double[]) {
            assertArrayEquals(rollingSum((double[]) expected, prevTicks, postTicks), (double[]) actual, deltaD);
        } else if (expected instanceof Boolean[]) {
            assertArrayEquals(rollingSum((Boolean[]) expected, prevTicks, postTicks), (long[]) actual);
        } else {
            if (type == BigDecimal.class) {
                assertArrayEquals(rollingSum((Object[]) expected, true, prevTicks, postTicks), (Object[]) actual);
            } else if (type == BigInteger.class) {
                assertArrayEquals(rollingSum((Object[]) expected, false, prevTicks, postTicks), (Object[]) actual);
            }
        }
    }

    final void assertWithRollingSumTime(final @NotNull Object expected, final @NotNull Object actual,
                        final @NotNull long[] timestamps, Class type, long prevTime, long postTime) {
        // looking for gross errors like missing entries (NOTE: pairwise results are more accurate than true rolling)
        final float deltaF = .02f;
        final double deltaD = .02d;

        if (expected instanceof byte[]) {
            assertArrayEquals(rollingSumTime((byte[]) expected, timestamps, prevTime, postTime), (long[]) actual);
        } else if (expected instanceof short[]) {
            assertArrayEquals(rollingSumTime((short[]) expected, timestamps, prevTime, postTime), (long[]) actual);
        } else if (expected instanceof int[]) {
            assertArrayEquals(rollingSumTime((int[]) expected, timestamps, prevTime, postTime), (long[]) actual);
        } else if (expected instanceof long[]) {
            assertArrayEquals(rollingSumTime((long[]) expected, timestamps, prevTime, postTime), (long[]) actual);
        } else if (expected instanceof float[]) {
            assertArrayEquals(rollingSumTime((float[]) expected, timestamps, prevTime, postTime), (float[]) actual, deltaF);
        } else if (expected instanceof double[]) {
            assertArrayEquals(rollingSumTime((double[]) expected, timestamps, prevTime, postTime), (double[]) actual, deltaD);
        } else if (expected instanceof Boolean[]) {
            assertArrayEquals(rollingSumTime((Boolean[]) expected, timestamps, prevTime, postTime), (long[]) actual);
        } else {
            if (type == BigDecimal.class) {
                assertArrayEquals(rollingSumTime((Object[]) expected, timestamps, true, prevTime, postTime), (Object[]) actual);
            } else if (type == BigInteger.class) {
                assertArrayEquals(rollingSumTime((Object[]) expected, timestamps, false, prevTime, postTime), (Object[]) actual);
            }
        }
    }
}
