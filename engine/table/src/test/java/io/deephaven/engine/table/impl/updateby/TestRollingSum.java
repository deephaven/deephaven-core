//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
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
import io.deephaven.engine.testutil.generator.SortedInstantGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.testTable;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.function.Basic.isNull;
import static io.deephaven.util.QueryConstants.*;
import static org.junit.Assert.assertArrayEquals;

@Category(OutOfBandTest.class)
public class TestRollingSum extends BaseUpdateByTest {
    /**
     * Because of the pairwise functions performing re-ordering of the values during summation, we can get very small
     * comparison errors in floating point values. This class tolerates larger differences in the float/double operator
     * results.
     */
    private abstract static class FuzzyEvalNuget extends EvalNugget {
        @Override
        @NotNull
        public EnumSet<TableDiff.DiffItems> diffItems() {
            return EnumSet.of(TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);
        }
    }

    // region Static Zero Key Tests

    @Test
    public void testStaticZeroKeyAllNullVector() {
        final QueryTable t = createTestTable(10000, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        // With a window size of 1 and 10% null generation, guaranteed to cover the all NULL case.
        final int prevTicks = 1;
        final int postTicks = 0;

        final Table summed = t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks));

        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTicks(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(summed, col).toArray(),
                    summed.getDefinition().getColumn(col).getDataType(), prevTicks, postTicks);
        }
    }

    @Test
    public void testStaticZeroKeyRev() {
        final QueryTable t = createTestTable(10000, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        final int prevTicks = 100;
        final int postTicks = 0;

        final Table summed = t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks));
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTicks(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(summed, col).toArray(),
                    summed.getDefinition().getColumn(col).getDataType(), prevTicks, postTicks);
        }
    }

    @Test
    public void testStaticZeroKeyRevExclusive() {
        final QueryTable t = createTestTable(10000, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        final int prevTicks = 100;
        final int postTicks = -50;

        final Table summed = t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks));
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTicks(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(summed, col).toArray(),
                    summed.getDefinition().getColumn(col).getDataType(), prevTicks, postTicks);
        }
    }

    @Test
    public void testStaticZeroKeyFwd() {
        final QueryTable t = createTestTable(10000, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        final int prevTicks = 0;
        final int postTicks = 100;

        final Table summed = t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks));
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTicks(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(summed, col).toArray(),
                    summed.getDefinition().getColumn(col).getDataType(), prevTicks, postTicks);
        }
    }

    @Test
    public void testStaticZeroKeyFwdExclusive() {
        final QueryTable t = createTestTable(10000, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        final int prevTicks = -50;
        final int postTicks = 100;

        final Table summed = t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks));
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTicks(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(summed, col).toArray(),
                    summed.getDefinition().getColumn(col).getDataType(), prevTicks, postTicks);
        }
    }

    @Test
    public void testStaticZeroKeyFwdRevWindow() {
        final QueryTable t = createTestTable(10000, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        final int prevTicks = 100;
        final int postTicks = 100;

        final Table summed = t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks));
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTicks(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(summed, col).toArray(),
                    summed.getDefinition().getColumn(col).getDataType(), prevTicks, postTicks);
        }
    }

    @Test
    public void testStaticZeroKeyTimedRev() {
        final QueryTable t = createTestTable(10000, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))}).t;

        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ZERO;

        final Table summed =
                t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime, "byteCol", "shortCol", "intCol",
                        "longCol", "floatCol",
                        "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"));


        final Instant[] ts = ColumnVectors.ofObject(t, "ts", Instant.class).toArray();
        final long[] timestamps = new long[t.intSize()];
        for (int i = 0; i < t.intSize(); i++) {
            timestamps[i] = DateTimeUtils.epochNanos(ts[i]);
        }

        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTime(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(summed, col).toArray(),
                    timestamps, summed.getDefinition().getColumn(col).getDataType(),
                    prevTime.toNanos(), postTime.toNanos());
        }
    }

    @Test
    public void testStaticZeroKeyTimedRevExclusive() {
        final QueryTable t = createTestTable(10000, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))}).t;

        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        final Table summed =
                t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime, "byteCol", "shortCol", "intCol",
                        "longCol", "floatCol",
                        "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"));


        final Instant[] ts = ColumnVectors.ofObject(t, "ts", Instant.class).toArray();
        final long[] timestamps = new long[t.intSize()];
        for (int i = 0; i < t.intSize(); i++) {
            timestamps[i] = DateTimeUtils.epochNanos(ts[i]);
        }

        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTime(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(summed, col).toArray(),
                    timestamps, summed.getDefinition().getColumn(col).getDataType(),
                    prevTime.toNanos(), postTime.toNanos());
        }
    }

    @Test
    public void testStaticZeroKeyTimedFwd() {
        final QueryTable t = createTestTable(10000, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))}).t;

        final Duration prevTime = Duration.ZERO;
        final Duration postTime = Duration.ofMinutes(10);

        final Table summed =
                t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime, "byteCol", "shortCol", "intCol",
                        "longCol", "floatCol",
                        "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"));


        final Instant[] ts = ColumnVectors.ofObject(t, "ts", Instant.class).toArray();
        final long[] timestamps = new long[t.intSize()];
        for (int i = 0; i < t.intSize(); i++) {
            timestamps[i] = DateTimeUtils.epochNanos(ts[i]);
        }

        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTime(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(summed, col).toArray(),
                    timestamps, summed.getDefinition().getColumn(col).getDataType(),
                    prevTime.toNanos(), postTime.toNanos());
        }
    }

    @Test
    public void testStaticZeroKeyTimedFwdExclusive() {
        final QueryTable t = createTestTable(10000, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))}).t;

        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        final Table summed =
                t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime, "byteCol", "shortCol", "intCol",
                        "longCol", "floatCol",
                        "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"));


        final Instant[] ts = ColumnVectors.ofObject(t, "ts", Instant.class).toArray();
        final long[] timestamps = new long[t.intSize()];
        for (int i = 0; i < t.intSize(); i++) {
            timestamps[i] = DateTimeUtils.epochNanos(ts[i]);
        }

        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTime(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(summed, col).toArray(),
                    timestamps, summed.getDefinition().getColumn(col).getDataType(),
                    prevTime.toNanos(), postTime.toNanos());
        }
    }

    @Test
    public void testStaticZeroKeyTimedFwdRev() {
        final QueryTable t = createTestTable(10000, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))}).t;

        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(10);

        final Table summed =
                t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime, "byteCol", "shortCol", "intCol",
                        "longCol", "floatCol",
                        "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"));


        final Instant[] ts = ColumnVectors.ofObject(t, "ts", Instant.class).toArray();
        final long[] timestamps = new long[t.intSize()];
        for (int i = 0; i < t.intSize(); i++) {
            timestamps[i] = DateTimeUtils.epochNanos(ts[i]);
        }

        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingSumTime(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(summed, col).toArray(),
                    timestamps, summed.getDefinition().getColumn(col).getDataType(),
                    prevTime.toNanos(), postTime.toNanos());
        }
    }

    // endregion

    // region Static Bucketed Tests

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
                UpdateByOperation.RollingSum(100, "ByteValSum=ByteVal"),
                UpdateByOperation.RollingSum(100, "ShortValSum=ShortVal"),
                UpdateByOperation.RollingSum(100, "IntValSum=IntVal")), "Sym");

        assertTableEquals(expected, r);
    }

    @Test
    public void testStaticBucketedAllNull() {
        // With a window size of 1 and 10% null generation, guaranteed to cover the all NULL case.
        final int prevTicks = 1;
        final int postTicks = 0;
        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedRev() {
        final int prevTicks = 100;
        final int postTicks = 0;
        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;
        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;
        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;
        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticGroupedBucketed() {
        final int prevTicks = 100;
        final int postTicks = 0;
        doTestStaticBucketed(true, prevTicks, postTicks);
    }

    private void doTestStaticBucketed(boolean grouped, int prevTicks, int postTicks) {
        final QueryTable t = createTestTable(100000, true, grouped, false, 0x31313131).t;

        final Table summed =
                t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks, "byteCol", "shortCol", "intCol",
                        "longCol", "floatCol",
                        "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"), "Sym");


        final PartitionedTable preOp = t.partitionBy("Sym");
        final PartitionedTable postOp = summed.partitionBy("Sym");

        final String[] columns =
                t.getDefinition().getColumnStream().map(ColumnDefinition::getName).toArray(String[]::new);

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithRollingSumTicks(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        actual.getDefinition().getColumn(col).getDataType(), prevTicks, postTicks);
            });
            return source;
        });
    }

    @Test
    public void testStaticBucketedTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);
        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);
        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);
        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);
        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedFwdRevWindowTimed() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);
        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    private void doTestStaticBucketedTimed(boolean grouped, Duration prevTime, Duration postTime) {
        final QueryTable t = createTestTable(10000, true, grouped, false, 0xFFFABBBC,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))}).t;

        final Table summed =
                t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime, "byteCol", "shortCol", "intCol",
                        "longCol", "floatCol",
                        "doubleCol", "boolCol", "bigIntCol", "bigDecimalCol"), "Sym");


        final PartitionedTable preOp = t.partitionBy("Sym");
        final PartitionedTable postOp = summed.partitionBy("Sym");

        String[] columns = t.getDefinition().getColumnStream().map(ColumnDefinition::getName).toArray(String[]::new);

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Instant[] ts = ColumnVectors.ofObject(source, "ts", Instant.class).toArray();
            long[] timestamps = new long[source.intSize()];
            for (int i = 0; i < source.intSize(); i++) {
                timestamps[i] = DateTimeUtils.epochNanos(ts[i]);
            }
            Arrays.stream(columns).forEach(col -> {
                assertWithRollingSumTime(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        timestamps, actual.getDefinition().getColumn(col).getDataType(),
                        prevTime.toNanos(), postTime.toNanos());
            });
            return source;
        });
    }

    // endregion

    // region Live Tests

    @Test
    public void testZeroKeyAppendOnlyAllNull() {
        // With a window size of 1 and 10% null generation, guaranteed to cover the all NULL case.
        final int prevTicks = 1;
        final int postTicks = 0;
        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyRev() {
        final int prevTicks = 100;
        final int postTicks = 0;
        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;
        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;
        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;
        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwdRev() {
        final int prevTicks = 50;
        final int postTicks = 50;
        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyAllNull() {
        // With a window size of 1 and 10% null generation, guaranteed to cover the all NULL case.
        final int prevTicks = 1;
        final int postTicks = 0;
        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyRev() {
        final int prevTicks = 100;
        final int postTicks = 0;
        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;
        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;
        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;
        doTestAppendOnly(true, prevTicks, postTicks);
    }

    private void doTestAppendOnly(boolean bucketed, int prevTicks, int postTicks) {
        final CreateResult result = createTestTable(10000, bucketed, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;
        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks), "Sym")
                                : t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks));
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
    public void testZeroKeyAppendOnlyTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);
        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);
        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);
        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);
        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);
        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    private void doTestAppendOnlyTimed(boolean bucketed, Duration prevTime, Duration postTime) {
        final CreateResult result = createTestTable(10000, bucketed, false, true, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;
        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime), "Sym")
                                : t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime));
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
    public void testBucketedAppendOnlyTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);
        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);
        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);
        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);
        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);
        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testZeroKeyGeneralTickingRev() {
        final CreateResult result = createTestTable(10000, false, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final long prevTicks = 100;
        final long fwdTicks = 0;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum(prevTicks, fwdTicks));
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
    public void testZeroKeyGeneralTickingRevExclusive() {
        final CreateResult result = createTestTable(10000, false, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final long prevTicks = 100;
        final long fwdTicks = -50;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum(prevTicks, fwdTicks));
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
    public void testZeroKeyGeneralTickingFwd() {
        final CreateResult result = createTestTable(10000, false, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum(100));
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
    public void testZeroKeyGeneralTickingFwdExclusive() {
        final CreateResult result = createTestTable(10000, false, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum(-50, 100));
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
    public void testBucketedGeneralTickingRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        final CreateResult result = createTestTable(10000, true, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks), "Sym");
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

    @Test
    public void testBucketedGeneralTickingRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        final CreateResult result = createTestTable(10000, true, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks), "Sym");
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

    @Test
    public void testBucketedGeneralTickingFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        final CreateResult result = createTestTable(10000, true, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks), "Sym");
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

    @Test
    public void testBucketedGeneralTickingFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        final CreateResult result = createTestTable(10000, true, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks), "Sym");
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

    @Test
    public void testBucketedGeneralTickingFwdRev() {
        final int prevTicks = 50;
        final int postTicks = 50;

        final CreateResult result = createTestTable(10000, true, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum(prevTicks, postTicks), "Sym");
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

    @Test
    public void testBucketedGeneralTickingTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        final CreateResult result = createTestTable(10000, true, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});


        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime), "Sym");
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

    @Test
    public void testBucketedGeneralTickingTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        final CreateResult result = createTestTable(100, true, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});


        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime), "Sym");
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < 100; ii++) {
            try {
                simulateShiftAwareStep(10, billy, t, result.infos, nuggets);
            } catch (Throwable ex) {
                System.out.println("Crapped out on step " + ii);
                throw ex;
            }
        }
    }

    @Test
    public void testBucketedGeneralTickingTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        final CreateResult result = createTestTable(10000, true, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});


        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime), "Sym");
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

    @Test
    public void testBucketedGeneralTickingTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        final CreateResult result = createTestTable(10000, true, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});


        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime), "Sym");
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

    @Test
    public void testBucketedGeneralTickingTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(0);

        final CreateResult result = createTestTable(10000, true, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});


        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNuget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingSum("ts", prevTime, postTime, "floatCol"), "Sym");
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

    private double[] rollingSum(float[] values, int prevTicks, int postTicks) {
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

    private double[] rollingSumTime(float[] values, long[] timestamps, long prevNanos, long postNanos) {
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

    private Object[] rollingSumTime(Object[] values, long[] timestamps, final boolean isBD, long prevNanos,
            long postNanos) {
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


    final void assertWithRollingSumTicks(@NotNull final Object expected, @NotNull final Object actual, Class type,
            int prevTicks, int postTicks) {
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
            assertArrayEquals(rollingSum((float[]) expected, prevTicks, postTicks), (double[]) actual, deltaF);
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

    final void assertWithRollingSumTime(@NotNull final Object expected, @NotNull final Object actual,
            @NotNull final long[] timestamps, Class type, long prevTime, long postTime) {
        // looking for gross errors like missing entries (NOTE: pairwise results are more accurate than true rolling)
        final float deltaF = .03f;
        final double deltaD = .03d;

        if (expected instanceof byte[]) {
            assertArrayEquals(rollingSumTime((byte[]) expected, timestamps, prevTime, postTime), (long[]) actual);
        } else if (expected instanceof short[]) {
            assertArrayEquals(rollingSumTime((short[]) expected, timestamps, prevTime, postTime), (long[]) actual);
        } else if (expected instanceof int[]) {
            assertArrayEquals(rollingSumTime((int[]) expected, timestamps, prevTime, postTime), (long[]) actual);
        } else if (expected instanceof long[]) {
            assertArrayEquals(rollingSumTime((long[]) expected, timestamps, prevTime, postTime), (long[]) actual);
        } else if (expected instanceof float[]) {
            assertArrayEquals(rollingSumTime((float[]) expected, timestamps, prevTime, postTime), (double[]) actual,
                    deltaF);
        } else if (expected instanceof double[]) {
            assertArrayEquals(rollingSumTime((double[]) expected, timestamps, prevTime, postTime), (double[]) actual,
                    deltaD);
        } else if (expected instanceof Boolean[]) {
            assertArrayEquals(rollingSumTime((Boolean[]) expected, timestamps, prevTime, postTime), (long[]) actual);
        } else {
            if (type == BigDecimal.class) {
                assertArrayEquals(rollingSumTime((Object[]) expected, timestamps, true, prevTime, postTime),
                        (Object[]) actual);
            } else if (type == BigInteger.class) {
                assertArrayEquals(rollingSumTime((Object[]) expected, timestamps, false, prevTime, postTime),
                        (Object[]) actual);
            }
        }
    }
}
