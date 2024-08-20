//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.SortedInstantGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.junit.Assert.assertArrayEquals;

@Category(OutOfBandTest.class)
public class TestRollingGroup extends BaseUpdateByTest {
    final String[] columns = new String[] {
            "byteCol",
            "shortCol",
            "intCol",
            "longCol",
            "floatCol",
            "doubleCol",
            "boolCol",
            "bigIntCol",
            "bigDecimalCol"
    };

    final int STATIC_TABLE_SIZE = 10_000;
    final int DYNAMIC_TABLE_SIZE = 1_000;
    final int DYNAMIC_UPDATE_SIZE = 100;
    final int DYNAMIC_UPDATE_STEPS = 20;

    // region Static Zero Key Tests

    @Test
    public void testStaticZeroKeyAllNullVector() {
        final int prevTicks = 1;
        final int postTicks = 0;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyFwdRevWindow() {
        final int prevTicks = 100;
        final int postTicks = 100;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ZERO;

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwd() {
        final Duration prevTime = Duration.ZERO;
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    private void doTestStaticZeroKey(final int prevTicks, final int postTicks) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        final Table summed = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, columns));
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingGroupTicks(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(summed, col).toArray(),
                    summed.getDefinition().getColumn(col).getDataType(), prevTicks, postTicks);
        }
    }

    private void doTestStaticZeroKeyTimed(final Duration prevTime, final Duration postTime) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))}).t;

        final Table summed =
                t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, columns));


        final Instant[] ts = ColumnVectors.ofObject(t, "ts", Instant.class).toArray();
        final long[] timestamps = new long[t.intSize()];
        for (int i = 0; i < t.intSize(); i++) {
            timestamps[i] = DateTimeUtils.epochNanos(ts[i]);
        }

        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingGroupTime(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(summed, col).toArray(),
                    timestamps, summed.getDefinition().getColumn(col).getDataType(),
                    prevTime.toNanos(), postTime.toNanos());
        }
    }

    // endregion

    // region Static Bucketed Tests

    @Test
    public void testStaticGroupedBucketed() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestStaticBucketed(true, prevTicks, postTicks);
    }

    @Test
    public void testStaticGroupedBucketedTimed() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestStaticBucketedTimed(true, prevTime, postTime);
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

    private void doTestStaticBucketed(boolean grouped, int prevTicks, int postTicks) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, grouped, false, 0x31313131).t;

        final Table summed =
                t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, columns), "Sym");


        final PartitionedTable preOp = t.partitionBy("Sym");
        final PartitionedTable postOp = summed.partitionBy("Sym");

        final String[] columns =
                t.getDefinition().getColumnStream().map(ColumnDefinition::getName).toArray(String[]::new);

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithRollingGroupTicks(
                        ColumnVectors.of(source, col).toArray(), ColumnVectors.of(actual, col).toArray(),
                        actual.getDefinition().getColumn(col).getDataType(), prevTicks, postTicks);
            });
            return source;
        });
    }

    private void doTestStaticBucketedTimed(boolean grouped, Duration prevTime, Duration postTime) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, grouped, false, 0xFFFABBBC,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))}).t;

        final Table summed =
                t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, columns), "Sym");


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
                assertWithRollingGroupTime(
                        ColumnVectors.of(source, col).toArray(), ColumnVectors.of(actual, col).toArray(),
                        timestamps, actual.getDefinition().getColumn(col).getDataType(),
                        prevTime.toNanos(), postTime.toNanos());
            });
            return source;
        });
    }

    // endregion

    // region Append Only Tests

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

    private void doTestAppendOnly(boolean bucketed, int prevTicks, int postTicks) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131);
        final QueryTable t = result.t;
        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, columns),
                                        "Sym")
                                : t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, columns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    private void doTestAppendOnlyTimed(boolean bucketed, Duration prevTime, Duration postTime) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});
        final QueryTable t = result.t;
        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(
                                UpdateByOperation.RollingGroup("ts", prevTime, postTime, columns), "Sym")
                                : t.updateBy(
                                        UpdateByOperation.RollingGroup("ts", prevTime, postTime, columns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    // endregion

    // region General Ticking Tests

    @Test
    public void testZeroKeyGeneralTickingRev() {
        final long prevTicks = 100;
        final long fwdTicks = 0;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyGeneralTickingRevExclusive() {
        final long prevTicks = 100;
        final long fwdTicks = -50;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyGeneralTickingFwd() {
        final long prevTicks = 0;
        final long fwdTicks = 100;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyGeneralTickingFwdExclusive() {
        final long prevTicks = -50;
        final long fwdTicks = 100;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedGeneralTickingRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestTicking(false, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwdRev() {
        final int prevTicks = 50;
        final int postTicks = 50;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        doTestTickingTimed(true, prevTime, postTime);
    }

    private void doTestTicking(final boolean bucketed, final long prevTicks, final long fwdTicks) {

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131);
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingGroup(prevTicks, fwdTicks, columns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    private void doTestTickingTimed(final boolean bucketed, final Duration prevTime, final Duration postTime) {

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});

        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, columns),
                                "Sym");
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testBucketedGeneralTickingRevRedirected() {
        final int prevTicks = 100;
        final int postTicks = 0;

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131);
        final QueryTable t = result.t;

        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(control,
                                List.of(UpdateByOperation.RollingGroup(prevTicks, postTicks, columns)),
                                ColumnName.from("Sym"));
                    }
                }
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
    public void testBucketedGeneralTickingTimedRevRedirected() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});

        final QueryTable t = result.t;

        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(control,
                                List.of(UpdateByOperation.RollingGroup("ts", prevTime, postTime, columns)),
                                ColumnName.from("Sym"));
                    }
                }
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

    // endregion

    // region Ungroup test

    @Test
    public void testUngroupOneRow() {
        final int prevTicks = 1; // one row
        final int postTicks = 0;

        final CreateResult result = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131);
        final QueryTable t = result.t;

        final Table grouped = t.updateBy(
                List.of(UpdateByOperation.RollingGroup(prevTicks, postTicks, columns)));

        final Table ungrouped = grouped.ungroup(columns);

        TstUtils.assertTableEquals(t, ungrouped);
    }

    @Test
    public void testUngroupOneRowBucketed() {
        final int prevTicks = 1; // one row
        final int postTicks = 0;

        final CreateResult result = createTestTable(STATIC_TABLE_SIZE, true, false, false, 0x31313131);
        final QueryTable t = result.t;

        final Table grouped = t.updateBy(
                List.of(UpdateByOperation.RollingGroup(prevTicks, postTicks, columns)),
                ColumnName.from("Sym"));

        final Table ungrouped = grouped.ungroup(columns);

        TstUtils.assertTableEquals(t, ungrouped);
    }

    @Test
    public void testUngroupOneRowTimed() {
        // This selects only one row in this case where no timestamps are identical. Otherwise the groups include all
        // rows with identical timestamps.
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(0);

        final CreateResult result = createTestTable(STATIC_TABLE_SIZE, false, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});

        final QueryTable t = result.t;

        final Table grouped = t.updateBy(
                List.of(UpdateByOperation.RollingGroup("ts", prevTime, postTime, columns)));

        final Table ungrouped = grouped.ungroup(columns);

        TstUtils.assertTableEquals(t, ungrouped);
    }

    @Test
    public void testUngroupOneRowBucketedTimed() {
        // This selects only one row in this case where no timestamps are identical. Otherwise the groups include all
        // rows with identical timestamps.
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(0);

        final CreateResult result = createTestTable(STATIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});

        final QueryTable t = result.t;

        final Table grouped = t.updateBy(
                List.of(UpdateByOperation.RollingGroup("ts", prevTime, postTime, columns)),
                ColumnName.from("Sym"));

        final Table ungrouped = grouped.ungroup(columns);

        TstUtils.assertTableEquals(t, ungrouped);
    }

    @Test
    public void testUngroupFiveRowsRev() {
        final int prevTicks = 5;
        final int postTicks = 0;

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, false, false, false, 0x31313131);
        final QueryTable t = result.t;

        final Table grouped = t.updateBy(
                List.of(UpdateByOperation.RollingGroup(prevTicks, postTicks, columns)));

        final Table ungrouped = grouped.ungroup(columns);

        // Hacky way to test that the final value in each grouped/ungrouped matches the original value. This works
        // because we are stopping at the current row in our selection.

        try (final RowSet.RangeIterator rangeIt = ungrouped.getRowSet().rangeIterator();
                final RowSet.Iterator rowIt = t.getRowSet().iterator()) {
            while (rowIt.hasNext() && rangeIt.hasNext()) {

                final long rowKeyA = rowIt.nextLong();
                rangeIt.next();
                final long rowKeyB = rangeIt.currentRangeEnd();

                for (String col : columns) {
                    Object a = t.getColumnSource(col).get(rowKeyA);
                    Object b = ungrouped.getColumnSource(col).get(rowKeyB);
                    assert Objects.equals(a, b);
                }
            }
        }
    }

    @Test
    public void testUngroupFiveRowsRevTimed() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(0);

        final CreateResult result = createTestTable(STATIC_TABLE_SIZE, false, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});

        final QueryTable t = result.t;

        final Table grouped = t.updateBy(
                List.of(UpdateByOperation.RollingGroup("ts", prevTime, postTime, columns)));

        final Table ungrouped = grouped.ungroup(columns);

        // Hacky way to test that the final value in each grouped/ungrouped matches the original value. This works
        // because we are stopping at the current row in our selection.

        try (final RowSet.RangeIterator rangeIt = ungrouped.getRowSet().rangeIterator();
                final RowSet.Iterator rowIt = t.getRowSet().iterator()) {
            while (rowIt.hasNext() && rangeIt.hasNext()) {

                final long rowKeyA = rowIt.nextLong();
                rangeIt.next();
                final long rowKeyB = rangeIt.currentRangeEnd();

                for (String col : columns) {
                    Object a = t.getColumnSource(col).get(rowKeyA);
                    Object b = ungrouped.getColumnSource(col).get(rowKeyB);
                    assert Objects.equals(a, b);
                }
            }
        }
    }

    @Test
    public void testUngroupFiveRowsFwd() {
        final int prevTicks = 1;
        final int postTicks = 4;

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, false, false, false, 0x31313131);
        final QueryTable t = result.t;

        final Table grouped = t.updateBy(
                List.of(UpdateByOperation.RollingGroup(prevTicks, postTicks, columns)));

        final Table ungrouped = grouped.ungroup(columns);

        // Hacky way to test that the first value in each grouped/ungrouped matches the original value. This works
        // because we are starting at the current row in our selection.

        try (final RowSet.RangeIterator rangeIt = ungrouped.getRowSet().rangeIterator();
                final RowSet.Iterator rowIt = t.getRowSet().iterator()) {
            while (rowIt.hasNext() && rangeIt.hasNext()) {

                final long rowKeyA = rowIt.nextLong();
                rangeIt.next();
                final long rowKeyB = rangeIt.currentRangeStart();

                for (String col : columns) {
                    Object a = t.getColumnSource(col).get(rowKeyA);
                    Object b = ungrouped.getColumnSource(col).get(rowKeyB);
                    assert Objects.equals(a, b);
                }
            }
        }
    }

    @Test
    public void testUngroupFiveRowsFwdTimed() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(5);

        final CreateResult result = createTestTable(STATIC_TABLE_SIZE, false, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});

        final QueryTable t = result.t;

        final Table grouped = t.updateBy(
                List.of(UpdateByOperation.RollingGroup("ts", prevTime, postTime, columns)));

        final Table ungrouped = grouped.ungroup(columns).select();

        // Hacky way to test that the first value in each grouped/ungrouped matches the original value. This works
        // because we are starting at the current row in our selection.

        try (final RowSet.RangeIterator rangeIt = ungrouped.getRowSet().rangeIterator();
                final RowSet.Iterator rowIt = t.getRowSet().iterator()) {
            while (rowIt.hasNext() && rangeIt.hasNext()) {

                final long rowKeyA = rowIt.nextLong();
                rangeIt.next();
                final long rowKeyB = rangeIt.currentRangeStart();

                for (String col : columns) {
                    Object a = t.getColumnSource(col).get(rowKeyA);
                    Object b = ungrouped.getColumnSource(col).get(rowKeyB);
                    assert Objects.equals(a, b);
                }
            }
        }
    }

    @Test
    public void testEmptyUngroupTimed() {
        final Duration prevTime = Duration.ofMinutes(-4_999_999);
        final Duration postTime = Duration.ofMinutes(5_000_000);

        final CreateResult result = createTestTable(STATIC_TABLE_SIZE, false, false, true, 0x31313131,
                new String[] {"ts"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY"))});

        final QueryTable t = result.t;

        final Table grouped = t.updateBy(
                List.of(UpdateByOperation.RollingGroup("ts", prevTime, postTime, columns)));

        final Table ungrouped = grouped.ungroup(columns).select();

        Assert.eqZero(ungrouped.getRowSet().size(), "ungrouped.getRowSet().size()");
    }

    @Test
    public void testFilteredUngroup() {
        final int prevTicks = 10;
        final int postTicks = 0;

        final int TABLE_SIZE = 1000;

        // Create very specific values.
        final int[] values = new int[TABLE_SIZE];
        final int[] mod2 = new int[TABLE_SIZE];
        final int[] mod5 = new int[TABLE_SIZE];

        for (int ii = 0; ii < TABLE_SIZE; ii++) {
            values[ii] = ii;
            mod2[ii] = ii % 2;
            mod5[ii] = ii % 5;
        }

        final QueryTable t = TstUtils.testTable(
                intCol("idx", values),
                intCol("mod2", mod2),
                intCol("mod5", mod5));

        final Table grouped = t.updateBy(
                List.of(UpdateByOperation.RollingGroup(prevTicks, postTicks, "idx", "mod2", "mod5")));

        final Table ungrouped = grouped.ungroup("idx", "mod2", "mod5");

        // Test mod 2.
        Table filteredTable = ungrouped.where("mod2==0");
        int[] filteredArray = ColumnVectors.ofInt(filteredTable, "idx").toArray();
        for (int ii = 0; ii < filteredArray.length; ii++) {
            Assert.eq(0, "filteredArray[ii] % 2", filteredArray[ii] % 2);
        }
        filteredTable = ungrouped.where("mod2==1");
        filteredArray = ColumnVectors.ofInt(filteredTable, "idx").toArray();
        for (int ii = 0; ii < filteredArray.length; ii++) {
            Assert.eq(1, "filteredArray[ii] % 2", filteredArray[ii] % 2);
        }

        // Test mod 5
        filteredTable = ungrouped.where("mod5==0");
        filteredArray = ColumnVectors.ofInt(filteredTable, "idx").toArray();
        for (int ii = 0; ii < filteredArray.length; ii++) {
            Assert.eq(0, "filteredArray[ii] % 5", filteredArray[ii] % 5);
        }
        filteredTable = ungrouped.where("mod5==1");
        filteredArray = ColumnVectors.ofInt(filteredTable, "idx").toArray();
        for (int ii = 0; ii < filteredArray.length; ii++) {
            Assert.eq(1, "filteredArray[ii] % 5", filteredArray[ii] % 5);
        }
        filteredTable = ungrouped.where("mod5==2");
        filteredArray = ColumnVectors.ofInt(filteredTable, "idx").toArray();
        for (int ii = 0; ii < filteredArray.length; ii++) {
            Assert.eq(2, "filteredArray[ii] % 5", filteredArray[ii] % 5);
        }
        filteredTable = ungrouped.where("mod5==3");
        filteredArray = ColumnVectors.ofInt(filteredTable, "idx").toArray();
        for (int ii = 0; ii < filteredArray.length; ii++) {
            Assert.eq(3, "filteredArray[ii] % 5", filteredArray[ii] % 5);
        }
        filteredTable = ungrouped.where("mod5==4");
        filteredArray = ColumnVectors.ofInt(filteredTable, "idx").toArray();
        for (int ii = 0; ii < filteredArray.length; ii++) {
            Assert.eq(4, "filteredArray[ii] % 5", filteredArray[ii] % 5);
        }
    }

    // endregion

    // region Edge cases

    @Test
    public void testCombinedGroupStatic() {
        final int prevTicks = 10;
        final int postTicks = 0;

        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131).t;
        t.setRefreshing(false);

        // Make a unique RollingGroup operator for each column.
        List<UpdateByOperation> ops = Arrays.stream(columns)
                .map(col -> UpdateByOperation.RollingGroup(prevTicks, postTicks, col))
                .collect(Collectors.toList());

        final Table summed = t.updateBy(ops);

        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithRollingGroupTicks(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(summed, col).toArray(),
                    summed.getDefinition().getColumn(col).getDataType(), prevTicks, postTicks);
        }
    }

    // endregion

    // region Rolling Group static verification

    private byte[][] rollingGroup(byte[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new byte[0][0];
        }

        byte[][] result = new byte[values.length][0];


        for (int ii = 0; ii < values.length; ii++) {
            // set the head and the tail
            final int head = Math.max(0, ii - prevTicks + 1);
            final int tail = Math.min(values.length - 1, ii + postTicks);

            final int size = Math.max(0, tail - head + 1); // tail is inclusive
            if (size > 0) {
                result[ii] = new byte[size];
                System.arraycopy(values, head, result[ii], 0, size);
            }
        }

        return result;
    }

    private short[][] rollingGroup(short[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new short[0][0];
        }

        short[][] result = new short[values.length][0];


        for (int ii = 0; ii < values.length; ii++) {
            // set the head and the tail
            final int head = Math.max(0, ii - prevTicks + 1);
            final int tail = Math.min(values.length - 1, ii + postTicks);

            final int size = Math.max(0, tail - head + 1); // tail is inclusive
            if (size > 0) {
                result[ii] = new short[size];

                System.arraycopy(values, head, result[ii], 0, size);
            }
        }

        return result;
    }

    private int[][] rollingGroup(int[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new int[0][0];
        }

        int[][] result = new int[values.length][0];


        for (int ii = 0; ii < values.length; ii++) {
            // set the head and the tail
            final int head = Math.max(0, ii - prevTicks + 1);
            final int tail = Math.min(values.length - 1, ii + postTicks);

            final int size = Math.max(0, tail - head + 1); // tail is inclusive
            if (size > 0) {
                result[ii] = new int[size];

                System.arraycopy(values, head, result[ii], 0, size);
            }
        }

        return result;
    }

    private long[][] rollingGroup(long[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0][0];
        }

        long[][] result = new long[values.length][0];


        for (int ii = 0; ii < values.length; ii++) {
            // set the head and the tail
            final int head = Math.max(0, ii - prevTicks + 1);
            final int tail = Math.min(values.length - 1, ii + postTicks);

            final int size = Math.max(0, tail - head + 1); // tail is inclusive
            if (size > 0) {
                result[ii] = new long[size];

                System.arraycopy(values, head, result[ii], 0, size);
            }
        }

        return result;
    }

    private float[][] rollingGroup(float[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new float[0][0];
        }

        float[][] result = new float[values.length][0];


        for (int ii = 0; ii < values.length; ii++) {
            // set the head and the tail
            final int head = Math.max(0, ii - prevTicks + 1);
            final int tail = Math.min(values.length - 1, ii + postTicks);

            final int size = Math.max(0, tail - head + 1); // tail is inclusive
            if (size > 0) {
                result[ii] = new float[size];

                System.arraycopy(values, head, result[ii], 0, size);
            }
        }

        return result;
    }

    private double[][] rollingGroup(double[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new double[0][0];
        }

        double[][] result = new double[values.length][0];


        for (int ii = 0; ii < values.length; ii++) {
            // set the head and the tail
            final int head = Math.max(0, ii - prevTicks + 1);
            final int tail = Math.min(values.length - 1, ii + postTicks);

            final int size = Math.max(0, tail - head + 1); // tail is inclusive
            if (size > 0) {
                result[ii] = new double[size];

                System.arraycopy(values, head, result[ii], 0, size);
            }
        }

        return result;
    }

    private Object[][] rollingGroup(Object[] values, int prevTicks, int postTicks) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new Object[0][0];
        }

        Object[][] result = new Object[values.length][0];


        for (int ii = 0; ii < values.length; ii++) {
            // set the head and the tail
            final int head = Math.max(0, ii - prevTicks + 1);
            final int tail = Math.min(values.length - 1, ii + postTicks);

            final int size = Math.max(0, tail - head + 1); // tail is inclusive
            if (size > 0) {
                result[ii] = new Object[size];

                System.arraycopy(values, head, result[ii], 0, size);
            }
        }

        return result;
    }

    private byte[][] rollingGroupTime(byte[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new byte[0][0];
        }

        byte[][] result = new byte[values.length][0];

        // track how many nulls are in the window
        int nullCount = 0;

        int head = 0;
        int tail = 0;

        for (int ii = 0; ii < values.length; ii++) {
            // check the current timestamp. skip if NULL
            if (timestamps[ii] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[ii] - prevNanos;
            final long tailTime = timestamps[ii] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                if (timestamps[head] == NULL_LONG) {
                    nullCount--;
                }
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                if (timestamps[tail] == NULL_LONG) {
                    nullCount++;
                }
                tail++;
            }

            final int size = Math.max(0, tail - head - nullCount);
            if (size > 0) {
                result[ii] = new byte[size];

                // compute everything in this window
                int storeIdx = 0;
                for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                    if (timestamps[computeIdx] != NULL_LONG) {
                        result[ii][storeIdx++] = values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private short[][] rollingGroupTime(short[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new short[0][0];
        }

        short[][] result = new short[values.length][0];

        // track how many nulls are in the window
        int nullCount = 0;

        int head = 0;
        int tail = 0;

        for (int ii = 0; ii < values.length; ii++) {
            // check the current timestamp. skip if NULL
            if (timestamps[ii] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[ii] - prevNanos;
            final long tailTime = timestamps[ii] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                if (timestamps[head] == NULL_LONG) {
                    nullCount--;
                }
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                if (timestamps[tail] == NULL_LONG) {
                    nullCount++;
                }
                tail++;
            }

            final int size = Math.max(0, tail - head - nullCount);
            if (size > 0) {
                result[ii] = new short[size];

                // compute everything in this window
                int storeIdx = 0;
                for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                    if (timestamps[computeIdx] != NULL_LONG) {
                        result[ii][storeIdx++] = values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private int[][] rollingGroupTime(int[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new int[0][0];
        }

        int[][] result = new int[values.length][0];

        // track how many nulls are in the window
        int nullCount = 0;

        int head = 0;
        int tail = 0;

        for (int ii = 0; ii < values.length; ii++) {
            // check the current timestamp. skip if NULL
            if (timestamps[ii] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[ii] - prevNanos;
            final long tailTime = timestamps[ii] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                if (timestamps[head] == NULL_LONG) {
                    nullCount--;
                }
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                if (timestamps[tail] == NULL_LONG) {
                    nullCount++;
                }
                tail++;
            }

            final int size = Math.max(0, tail - head - nullCount);
            if (size > 0) {
                result[ii] = new int[size];

                // compute everything in this window
                int storeIdx = 0;
                for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                    if (timestamps[computeIdx] != NULL_LONG) {
                        result[ii][storeIdx++] = values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private long[][] rollingGroupTime(long[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new long[0][0];
        }

        long[][] result = new long[values.length][0];

        // track how many nulls are in the window
        int nullCount = 0;

        int head = 0;
        int tail = 0;

        for (int ii = 0; ii < values.length; ii++) {
            // check the current timestamp. skip if NULL
            if (timestamps[ii] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[ii] - prevNanos;
            final long tailTime = timestamps[ii] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                if (timestamps[head] == NULL_LONG) {
                    nullCount--;
                }
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                if (timestamps[tail] == NULL_LONG) {
                    nullCount++;
                }
                tail++;
            }

            final int size = Math.max(0, tail - head - nullCount);
            if (size > 0) {
                result[ii] = new long[size];

                // compute everything in this window
                int storeIdx = 0;
                for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                    if (timestamps[computeIdx] != NULL_LONG) {
                        result[ii][storeIdx++] = values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private float[][] rollingGroupTime(float[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new float[0][0];
        }

        float[][] result = new float[values.length][0];

        // track how many nulls are in the window
        int nullCount = 0;

        int head = 0;
        int tail = 0;

        for (int ii = 0; ii < values.length; ii++) {
            // check the current timestamp. skip if NULL
            if (timestamps[ii] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[ii] - prevNanos;
            final long tailTime = timestamps[ii] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                if (timestamps[head] == NULL_LONG) {
                    nullCount--;
                }
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                if (timestamps[tail] == NULL_LONG) {
                    nullCount++;
                }
                tail++;
            }

            final int size = Math.max(0, tail - head - nullCount);
            if (size > 0) {
                result[ii] = new float[size];

                // compute everything in this window
                int storeIdx = 0;
                for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                    if (timestamps[computeIdx] != NULL_LONG) {
                        result[ii][storeIdx++] = values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private double[][] rollingGroupTime(double[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new double[0][0];
        }

        double[][] result = new double[values.length][0];

        // track how many nulls are in the window
        int nullCount = 0;

        int head = 0;
        int tail = 0;

        for (int ii = 0; ii < values.length; ii++) {
            // check the current timestamp. skip if NULL
            if (timestamps[ii] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[ii] - prevNanos;
            final long tailTime = timestamps[ii] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                if (timestamps[head] == NULL_LONG) {
                    nullCount--;
                }
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                if (timestamps[tail] == NULL_LONG) {
                    nullCount++;
                }
                tail++;
            }

            final int size = Math.max(0, tail - head - nullCount);
            if (size > 0) {
                result[ii] = new double[size];

                // compute everything in this window
                int storeIdx = 0;
                for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                    if (timestamps[computeIdx] != NULL_LONG) {
                        result[ii][storeIdx++] = values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    private Object[][] rollingGroupTime(Object[] values, long[] timestamps, long prevNanos, long postNanos) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new Object[0][0];
        }

        Object[][] result = new Object[values.length][0];

        // track how many nulls are in the window
        int nullCount = 0;

        int head = 0;
        int tail = 0;

        for (int ii = 0; ii < values.length; ii++) {
            // check the current timestamp. skip if NULL
            if (timestamps[ii] == NULL_LONG) {
                continue;
            }

            // set the head and the tail
            final long headTime = timestamps[ii] - prevNanos;
            final long tailTime = timestamps[ii] + postNanos;

            // advance head and tail until they are in the correct spots
            while (head < values.length && timestamps[head] < headTime) {
                if (timestamps[head] == NULL_LONG) {
                    nullCount--;
                }
                head++;
            }

            while (tail < values.length && timestamps[tail] <= tailTime) {
                if (timestamps[tail] == NULL_LONG) {
                    nullCount++;
                }
                tail++;
            }

            final int size = Math.max(0, tail - head - nullCount);
            if (size > 0) {
                result[ii] = new Object[size];

                // compute everything in this window
                int storeIdx = 0;
                for (int computeIdx = head; computeIdx < tail; computeIdx++) {
                    if (timestamps[computeIdx] != NULL_LONG) {
                        result[ii][storeIdx++] = values[computeIdx];
                    }
                }
            }
        }

        return result;
    }

    final byte[][] convertToArray(ByteVector[] vectors) {
        final byte[][] result = new byte[vectors.length][];
        for (int ii = 0; ii < vectors.length; ii++) {
            result[ii] = vectors[ii] == null ? null : vectors[ii].toArray();
        }
        return result;
    }

    final short[][] convertToArray(ShortVector[] vectors) {
        final short[][] result = new short[vectors.length][];
        for (int ii = 0; ii < vectors.length; ii++) {
            result[ii] = vectors[ii] == null ? null : vectors[ii].toArray();
        }
        return result;
    }

    final int[][] convertToArray(IntVector[] vectors) {
        final int[][] result = new int[vectors.length][];
        for (int ii = 0; ii < vectors.length; ii++) {
            result[ii] = vectors[ii] == null ? null : vectors[ii].toArray();
        }
        return result;
    }

    final long[][] convertToArray(LongVector[] vectors) {
        final long[][] result = new long[vectors.length][];
        for (int ii = 0; ii < vectors.length; ii++) {
            result[ii] = vectors[ii] == null ? null : vectors[ii].toArray();
        }
        return result;
    }

    final float[][] convertToArray(FloatVector[] vectors) {
        final float[][] result = new float[vectors.length][];
        for (int ii = 0; ii < vectors.length; ii++) {
            result[ii] = vectors[ii] == null ? null : vectors[ii].toArray();
        }
        return result;
    }

    final double[][] convertToArray(DoubleVector[] vectors) {
        final double[][] result = new double[vectors.length][];
        for (int ii = 0; ii < vectors.length; ii++) {
            result[ii] = vectors[ii] == null ? null : vectors[ii].toArray();
        }
        return result;
    }

    final Object[][] convertToArray(ObjectVector[] vectors) {
        final Object[][] result = new Object[vectors.length][];
        for (int ii = 0; ii < vectors.length; ii++) {
            result[ii] = vectors[ii] == null ? null : vectors[ii].toArray();
        }
        return result;
    }

    final void assertWithRollingGroupTicks(@NotNull final Object expected, @NotNull final Object actual, Class type,
            int prevTicks, int postTicks) {

        if (expected instanceof byte[]) {
            assertArrayEquals(rollingGroup((byte[]) expected, prevTicks, postTicks),
                    convertToArray((ByteVector[]) actual));
        } else if (expected instanceof short[]) {
            assertArrayEquals(rollingGroup((short[]) expected, prevTicks, postTicks),
                    convertToArray((ShortVector[]) actual));
        } else if (expected instanceof int[]) {
            assertArrayEquals(rollingGroup((int[]) expected, prevTicks, postTicks),
                    convertToArray((IntVector[]) actual));
        } else if (expected instanceof long[]) {
            assertArrayEquals(rollingGroup((long[]) expected, prevTicks, postTicks),
                    convertToArray((LongVector[]) actual));
        } else if (expected instanceof float[]) {
            assertArrayEquals(rollingGroup((float[]) expected, prevTicks, postTicks),
                    convertToArray((FloatVector[]) actual));
        } else if (expected instanceof double[]) {
            assertArrayEquals(rollingGroup((double[]) expected, prevTicks, postTicks),
                    convertToArray((DoubleVector[]) actual));
        } else {
            if (type == BigDecimal.class || type == BigInteger.class) {
                assertArrayEquals(rollingGroup((Object[]) expected, prevTicks, postTicks),
                        convertToArray((ObjectVector[]) actual));
            }
        }
    }

    final void assertWithRollingGroupTime(@NotNull final Object expected, @NotNull final Object actual,
            @NotNull final long[] timestamps, Class type, long prevTime, long postTime) {

        if (expected instanceof byte[]) {
            assertArrayEquals(rollingGroupTime((byte[]) expected, timestamps, prevTime, postTime),
                    convertToArray((ByteVector[]) actual));
        } else if (expected instanceof short[]) {
            assertArrayEquals(rollingGroupTime((short[]) expected, timestamps, prevTime, postTime),
                    convertToArray((ShortVector[]) actual));
        } else if (expected instanceof int[]) {
            assertArrayEquals(rollingGroupTime((int[]) expected, timestamps, prevTime, postTime),
                    convertToArray((IntVector[]) actual));
        } else if (expected instanceof long[]) {
            assertArrayEquals(rollingGroupTime((long[]) expected, timestamps, prevTime, postTime),
                    convertToArray((LongVector[]) actual));
        } else if (expected instanceof float[]) {
            assertArrayEquals(rollingGroupTime((float[]) expected, timestamps, prevTime, postTime),
                    convertToArray((FloatVector[]) actual));
        } else if (expected instanceof double[]) {
            assertArrayEquals(rollingGroupTime((double[]) expected, timestamps, prevTime, postTime),
                    convertToArray((DoubleVector[]) actual));
        } else {
            if (type == BigDecimal.class || type == BigInteger.class) {
                assertArrayEquals(rollingGroupTime((Object[]) expected, timestamps, prevTime, postTime),
                        convertToArray((ObjectVector[]) actual));
            }
        }
    }

    // endregion
}
