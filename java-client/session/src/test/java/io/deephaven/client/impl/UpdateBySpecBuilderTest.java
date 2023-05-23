package io.deephaven.client.impl;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.DeltaControl;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.spec.*;
import io.deephaven.api.updateby.spec.UpdateBySpec.Visitor;
import io.deephaven.proto.backplane.grpc.UpdateByDeltaOptions;
import io.deephaven.proto.backplane.grpc.UpdateByEmaOptions;
import io.deephaven.proto.backplane.grpc.UpdateByEmaTimescale;
import io.deephaven.proto.backplane.grpc.UpdateByNullBehavior;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class UpdateBySpecBuilderTest {

    private enum ExpectedSpecVisitor implements Visitor<UpdateByColumn.UpdateBySpec> {
        INSTANCE;

        // Note: this is written in a way to encourage new tests get added any time a new UpdateByColumn.UpdateBySpec
        // type gets created.
        // The visitor methods should not typically need to look at the actual message - it's meant to return the
        // expected value. An exception is for CumMinMaxSpec, where we need to read the field to determine the proper
        // gRPC message type.

        @Override
        public UpdateByColumn.UpdateBySpec visit(EmaSpec spec) {
            return UpdateByColumn.UpdateBySpec
                    .newBuilder().setEma(
                            UpdateByEma.newBuilder()
                                    .setTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(EmsSpec spec) {
            return UpdateByColumn.UpdateBySpec
                    .newBuilder().setEms(
                            UpdateByEms.newBuilder()
                                    .setTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(EmMinMaxSpec spec) {
            if (spec.isMax()) {
                return UpdateByColumn.UpdateBySpec
                        .newBuilder().setEmMax(
                                UpdateByEmMax.newBuilder()
                                        .setTimescale(UpdateByEmaTimescale.newBuilder()
                                                .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                        .setColumn("Timestamp").setPeriodNanos(1).build())
                                                .build())
                                        .build())
                        .build();
            } else {
                return UpdateByColumn.UpdateBySpec
                        .newBuilder().setEmMin(
                                UpdateByEmMin.newBuilder()
                                        .setTimescale(UpdateByEmaTimescale.newBuilder()
                                                .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                        .setColumn("Timestamp").setPeriodNanos(1).build())
                                                .build())
                                        .build())
                        .build();
            }
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(EmStdSpec spec) {
            return UpdateByColumn.UpdateBySpec
                    .newBuilder().setEmStd(
                            UpdateByEmStd.newBuilder()
                                    .setTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .build())
                    .build();
        }

        @Override

        public UpdateByColumn.UpdateBySpec visit(FillBySpec spec) {
            return UpdateByColumn.UpdateBySpec.newBuilder().setFill(UpdateByFill.getDefaultInstance()).build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(CumSumSpec spec) {
            return UpdateByColumn.UpdateBySpec.newBuilder().setSum(UpdateByCumulativeSum.getDefaultInstance()).build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(CumMinMaxSpec spec) {
            if (spec.isMax()) {
                return UpdateByColumn.UpdateBySpec.newBuilder().setMax(UpdateByCumulativeMax.getDefaultInstance())
                        .build();
            } else {
                return UpdateByColumn.UpdateBySpec.newBuilder().setMin(UpdateByCumulativeMin.getDefaultInstance())
                        .build();
            }
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(CumProdSpec spec) {
            return UpdateByColumn.UpdateBySpec.newBuilder().setProduct(UpdateByCumulativeProduct.getDefaultInstance())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(DeltaSpec spec) {
            return UpdateByColumn.UpdateBySpec.newBuilder().setDelta(
                    UpdateByDelta.newBuilder().setOptions(
                            UpdateByDeltaOptions.newBuilder().setNullBehavior(UpdateByNullBehavior.NULL_DOMINATES))
                            .build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingSumSpec spec) {
            return UpdateByColumn.UpdateBySpec
                    .newBuilder().setRollingSum(
                            UpdateByColumn.UpdateBySpec.UpdateByRollingSum.newBuilder()
                                    .setReverseTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .setForwardTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingGroupSpec spec) {
            return UpdateByColumn.UpdateBySpec
                    .newBuilder().setRollingGroup(
                            UpdateByColumn.UpdateBySpec.UpdateByRollingGroup.newBuilder()
                                    .setReverseTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .setForwardTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingAvgSpec spec) {
            return UpdateByColumn.UpdateBySpec
                    .newBuilder().setRollingAvg(
                            UpdateByColumn.UpdateBySpec.UpdateByRollingAvg.newBuilder()
                                    .setReverseTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .setForwardTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingMinMaxSpec spec) {
            if (spec.isMax()) {
                return UpdateByColumn.UpdateBySpec
                        .newBuilder().setRollingMax(
                                UpdateByColumn.UpdateBySpec.UpdateByRollingMax.newBuilder()
                                        .setReverseTimescale(UpdateByEmaTimescale.newBuilder()
                                                .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                        .setColumn("Timestamp").setPeriodNanos(1).build())
                                                .build())
                                        .setForwardTimescale(UpdateByEmaTimescale.newBuilder()
                                                .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                        .setColumn("Timestamp").setPeriodNanos(1).build())
                                                .build())
                                        .build())
                        .build();
            } else {
                return UpdateByColumn.UpdateBySpec
                        .newBuilder().setRollingMin(
                                UpdateByColumn.UpdateBySpec.UpdateByRollingMin.newBuilder()
                                        .setReverseTimescale(UpdateByEmaTimescale.newBuilder()
                                                .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                        .setColumn("Timestamp").setPeriodNanos(1).build())
                                                .build())
                                        .setForwardTimescale(UpdateByEmaTimescale.newBuilder()
                                                .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                        .setColumn("Timestamp").setPeriodNanos(1).build())
                                                .build())
                                        .build())
                        .build();
            }
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingProductSpec spec) {
            return UpdateByColumn.UpdateBySpec
                    .newBuilder().setRollingProduct(
                            UpdateByColumn.UpdateBySpec.UpdateByRollingProduct.newBuilder()
                                    .setReverseTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .setForwardTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingCountSpec spec) {
            return UpdateByColumn.UpdateBySpec
                    .newBuilder().setRollingCount(
                            UpdateByColumn.UpdateBySpec.UpdateByRollingCount.newBuilder()
                                    .setReverseTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .setForwardTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingStdSpec spec) {
            return UpdateByColumn.UpdateBySpec
                    .newBuilder().setRollingStd(
                            UpdateByColumn.UpdateBySpec.UpdateByRollingStd.newBuilder()
                                    .setReverseTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .setForwardTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingWAvgSpec spec) {
            return UpdateByColumn.UpdateBySpec
                    .newBuilder().setRollingWavg(
                            UpdateByColumn.UpdateBySpec.UpdateByRollingWAvg.newBuilder()
                                    .setReverseTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .setForwardTimescale(UpdateByEmaTimescale.newBuilder()
                                            .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                                                    .setColumn("Timestamp").setPeriodNanos(1).build())
                                            .build())
                                    .setWeightColumn("Weight")
                                    .build())
                    .build();
        }
    }

    @Test
    void ema() {
        check(EmaSpec.ofTime("Timestamp", Duration.ofNanos(1)));
        check(EmaSpec.ofTicks(42L), UpdateByColumn.UpdateBySpec.newBuilder()
                .setEma(UpdateByEma.newBuilder().setTimescale(UpdateByEmaTimescale.newBuilder()
                        .setTicks(UpdateByEmaTimescale.UpdateByEmaTicks.newBuilder().setTicks(42L).build()).build())
                        .build())
                .build());
        check(EmaSpec.ofTicks(OperationControl.builder().onNullValue(BadDataBehavior.THROW).build(), 100L),
                UpdateByColumn.UpdateBySpec.newBuilder().setEma(UpdateByEma.newBuilder()
                        .setOptions(UpdateByEmaOptions.newBuilder()
                                .setOnNullValue(io.deephaven.proto.backplane.grpc.BadDataBehavior.THROW).build())
                        .setTimescale(UpdateByEmaTimescale.newBuilder()
                                .setTicks(UpdateByEmaTimescale.UpdateByEmaTicks.newBuilder().setTicks(100L).build())
                                .build())
                        .build()).build());
    }

    @Test
    void ems() {
        check(EmsSpec.ofTime("Timestamp", Duration.ofNanos(1)));
        check(EmsSpec.ofTicks(42L), UpdateByColumn.UpdateBySpec.newBuilder()
                .setEms(UpdateByEms.newBuilder().setTimescale(UpdateByEmaTimescale.newBuilder()
                        .setTicks(UpdateByEmaTimescale.UpdateByEmaTicks.newBuilder().setTicks(42L).build()).build())
                        .build())
                .build());
        check(EmsSpec.ofTicks(OperationControl.builder().onNullValue(BadDataBehavior.THROW).build(), 100L),
                UpdateByColumn.UpdateBySpec.newBuilder().setEms(UpdateByEms.newBuilder()
                        .setOptions(UpdateByEmaOptions.newBuilder()
                                .setOnNullValue(io.deephaven.proto.backplane.grpc.BadDataBehavior.THROW).build())
                        .setTimescale(UpdateByEmaTimescale.newBuilder()
                                .setTicks(UpdateByEmaTimescale.UpdateByEmaTicks.newBuilder().setTicks(100L).build())
                                .build())
                        .build()).build());
    }

    @Test
    void emMin() {
        check(EmMinMaxSpec.ofTime(false, "Timestamp", Duration.ofNanos(1)));
        check(EmMinMaxSpec.ofTicks(false, 42L), UpdateByColumn.UpdateBySpec.newBuilder()
                .setEmMin(UpdateByEmMin.newBuilder().setTimescale(UpdateByEmaTimescale.newBuilder()
                        .setTicks(UpdateByEmaTimescale.UpdateByEmaTicks.newBuilder().setTicks(42L).build()).build())
                        .build())
                .build());
        check(EmMinMaxSpec.ofTicks(OperationControl.builder().onNullValue(BadDataBehavior.THROW).build(), false, 100L),
                UpdateByColumn.UpdateBySpec.newBuilder().setEmMin(UpdateByEmMin.newBuilder()
                        .setOptions(UpdateByEmaOptions.newBuilder()
                                .setOnNullValue(io.deephaven.proto.backplane.grpc.BadDataBehavior.THROW).build())
                        .setTimescale(UpdateByEmaTimescale.newBuilder()
                                .setTicks(UpdateByEmaTimescale.UpdateByEmaTicks.newBuilder().setTicks(100L).build())
                                .build())
                        .build()).build());
    }

    @Test
    void emMax() {
        check(EmMinMaxSpec.ofTime(true, "Timestamp", Duration.ofNanos(1)));
        check(EmMinMaxSpec.ofTicks(true, 42L), UpdateByColumn.UpdateBySpec.newBuilder()
                .setEmMax(UpdateByEmMax.newBuilder().setTimescale(UpdateByEmaTimescale.newBuilder()
                        .setTicks(UpdateByEmaTimescale.UpdateByEmaTicks.newBuilder().setTicks(42L).build()).build())
                        .build())
                .build());
        check(EmMinMaxSpec.ofTicks(OperationControl.builder().onNullValue(BadDataBehavior.THROW).build(), true, 100L),
                UpdateByColumn.UpdateBySpec.newBuilder().setEmMax(UpdateByEmMax.newBuilder()
                        .setOptions(UpdateByEmaOptions.newBuilder()
                                .setOnNullValue(io.deephaven.proto.backplane.grpc.BadDataBehavior.THROW).build())
                        .setTimescale(UpdateByEmaTimescale.newBuilder()
                                .setTicks(UpdateByEmaTimescale.UpdateByEmaTicks.newBuilder().setTicks(100L).build())
                                .build())
                        .build()).build());
    }

    @Test
    void emStd() {
        check(EmStdSpec.ofTime("Timestamp", Duration.ofNanos(1)));
        check(EmStdSpec.ofTicks(42L), UpdateByColumn.UpdateBySpec.newBuilder()
                .setEmStd(UpdateByEmStd.newBuilder().setTimescale(UpdateByEmaTimescale.newBuilder()
                        .setTicks(UpdateByEmaTimescale.UpdateByEmaTicks.newBuilder().setTicks(42L).build()).build())
                        .build())
                .build());
        check(EmStdSpec.ofTicks(OperationControl.builder().onNullValue(BadDataBehavior.THROW).build(), 100L),
                UpdateByColumn.UpdateBySpec.newBuilder().setEmStd(UpdateByEmStd.newBuilder()
                        .setOptions(UpdateByEmaOptions.newBuilder()
                                .setOnNullValue(io.deephaven.proto.backplane.grpc.BadDataBehavior.THROW).build())
                        .setTimescale(UpdateByEmaTimescale.newBuilder()
                                .setTicks(UpdateByEmaTimescale.UpdateByEmaTicks.newBuilder().setTicks(100L).build())
                                .build())
                        .build()).build());
    }

    @Test
    void cumulativeMin() {
        check(CumMinMaxSpec.of(false));
    }

    @Test
    void cumulativeMax() {
        check(CumMinMaxSpec.of(true));
    }

    @Test
    void cumulativeProd() {
        check(CumProdSpec.of());
    }

    @Test
    void fillBy() {
        check(FillBySpec.of());
    }

    @Test
    void delta() {
        check(DeltaSpec.of());
        check(DeltaSpec.of(DeltaControl.NULL_DOMINATES),
                UpdateByColumn.UpdateBySpec.newBuilder().setDelta(
                        UpdateByColumn.UpdateBySpec.UpdateByDelta.newBuilder()
                                .setOptions(
                                        UpdateByDeltaOptions.newBuilder()
                                                .setNullBehavior(UpdateByNullBehavior.NULL_DOMINATES))
                                .build())
                        .build());
        check(DeltaSpec.of(DeltaControl.VALUE_DOMINATES),
                UpdateByColumn.UpdateBySpec.newBuilder().setDelta(
                        UpdateByColumn.UpdateBySpec.UpdateByDelta.newBuilder()
                                .setOptions(
                                        UpdateByDeltaOptions.newBuilder()
                                                .setNullBehavior(UpdateByNullBehavior.VALUE_DOMINATES))
                                .build())
                        .build());
        check(DeltaSpec.of(DeltaControl.ZERO_DOMINATES),
                UpdateByColumn.UpdateBySpec.newBuilder().setDelta(
                        UpdateByColumn.UpdateBySpec.UpdateByDelta.newBuilder()
                                .setOptions(
                                        UpdateByDeltaOptions.newBuilder()
                                                .setNullBehavior(UpdateByNullBehavior.ZERO_DOMINATES))
                                .build())
                        .build());
    }

    @Test
    void rollingSum() {
        check(RollingSumSpec.ofTime("Timestamp", Duration.ofNanos(1), Duration.ofNanos(2)),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingSum(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingSum.newBuilder()
                                .setReverseTimescale(time("Timestamp", 1))
                                .setForwardTimescale(time("Timestamp", 2))
                                .build())
                        .build());

        check(RollingSumSpec.ofTicks(42L, 43L),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingSum(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingSum.newBuilder()
                                .setReverseTimescale(ticks(42L))
                                .setForwardTimescale(ticks(43L))
                                .build())
                        .build());
    }

    @Test
    void rollingGroup() {
        check(RollingGroupSpec.ofTime("Timestamp", Duration.ofNanos(1), Duration.ofNanos(2)),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingGroup(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingGroup.newBuilder()
                                .setReverseTimescale(time("Timestamp", 1))
                                .setForwardTimescale(time("Timestamp", 2))
                                .build())
                        .build());

        check(RollingGroupSpec.ofTicks(42L, 43L),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingGroup(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingGroup.newBuilder()
                                .setReverseTimescale(ticks(42L))
                                .setForwardTimescale(ticks(43L))
                                .build())
                        .build());
    }

    @Test
    void rollingAvg() {
        check(RollingAvgSpec.ofTime("Timestamp", Duration.ofNanos(1), Duration.ofNanos(2)),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingAvg(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingAvg.newBuilder()
                                .setReverseTimescale(time("Timestamp", 1))
                                .setForwardTimescale(time("Timestamp", 2))
                                .build())
                        .build());

        check(RollingAvgSpec.ofTicks(42L, 43L),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingAvg(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingAvg.newBuilder()
                                .setReverseTimescale(ticks(42L))
                                .setForwardTimescale(ticks(43L))
                                .build())
                        .build());
    }

    @Test
    void rollingMin() {
        check(RollingMinMaxSpec.ofTime(false, "Timestamp", Duration.ofNanos(1), Duration.ofNanos(2)),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingMin(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingMin.newBuilder()
                                .setReverseTimescale(time("Timestamp", 1))
                                .setForwardTimescale(time("Timestamp", 2))
                                .build())
                        .build());

        check(RollingMinMaxSpec.ofTicks(false, 42L, 43L),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingMin(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingMin.newBuilder()
                                .setReverseTimescale(ticks(42L))
                                .setForwardTimescale(ticks(43L))
                                .build())
                        .build());
    }

    @Test
    void rollingMax() {
        check(RollingMinMaxSpec.ofTime(true, "Timestamp", Duration.ofNanos(1), Duration.ofNanos(2)),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingMax(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingMax.newBuilder()
                                .setReverseTimescale(time("Timestamp", 1))
                                .setForwardTimescale(time("Timestamp", 2))
                                .build())
                        .build());

        check(RollingMinMaxSpec.ofTicks(true, 42L, 43L),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingMax(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingMax.newBuilder()
                                .setReverseTimescale(ticks(42L))
                                .setForwardTimescale(ticks(43L))
                                .build())
                        .build());
    }

    @Test
    void rollingProduct() {
        check(RollingProductSpec.ofTime("Timestamp", Duration.ofNanos(1), Duration.ofNanos(2)),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingProduct(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingProduct.newBuilder()
                                .setReverseTimescale(time("Timestamp", 1))
                                .setForwardTimescale(time("Timestamp", 2))
                                .build())
                        .build());

        check(RollingProductSpec.ofTicks(42L, 43L),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingProduct(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingProduct.newBuilder()
                                .setReverseTimescale(ticks(42L))
                                .setForwardTimescale(ticks(43L))
                                .build())
                        .build());
    }

    @Test
    void rollingCount() {
        check(RollingCountSpec.ofTime("Timestamp", Duration.ofNanos(1), Duration.ofNanos(2)),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingCount(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingCount.newBuilder()
                                .setReverseTimescale(time("Timestamp", 1))
                                .setForwardTimescale(time("Timestamp", 2))
                                .build())
                        .build());

        check(RollingCountSpec.ofTicks(42L, 43L),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingCount(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingCount.newBuilder()
                                .setReverseTimescale(ticks(42L))
                                .setForwardTimescale(ticks(43L))
                                .build())
                        .build());
    }

    @Test
    void rollingStd() {
        check(RollingStdSpec.ofTime("Timestamp", Duration.ofNanos(1), Duration.ofNanos(2)),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingStd(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingStd.newBuilder()
                                .setReverseTimescale(time("Timestamp", 1))
                                .setForwardTimescale(time("Timestamp", 2))
                                .build())
                        .build());

        check(RollingStdSpec.ofTicks(42L, 43L),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingStd(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingStd.newBuilder()
                                .setReverseTimescale(ticks(42L))
                                .setForwardTimescale(ticks(43L))
                                .build())
                        .build());
    }

    @Test
    void rollingWAvg() {
        check(RollingWAvgSpec.ofTime("Timestamp", Duration.ofNanos(1), Duration.ofNanos(2), "Weight"),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingWavg(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingWAvg.newBuilder()
                                .setReverseTimescale(time("Timestamp", 1))
                                .setForwardTimescale(time("Timestamp", 2))
                                .setWeightColumn("Weight")
                                .build())
                        .build());

        check(RollingWAvgSpec.ofTicks(42L, 43L, "Weight"),
                UpdateByColumn.UpdateBySpec.newBuilder().setRollingWavg(
                        UpdateByColumn.UpdateBySpec.UpdateByRollingWAvg.newBuilder()
                                .setReverseTimescale(ticks(42L))
                                .setForwardTimescale(ticks(43L))
                                .setWeightColumn("Weight")
                                .build())
                        .build());
    }

    private static void check(UpdateBySpec spec) {
        check(spec, spec.walk(ExpectedSpecVisitor.INSTANCE));
    }

    private static void check(UpdateBySpec spec, UpdateByColumn.UpdateBySpec expected) {
        assertThat(UpdateByBuilder.adapt(spec)).isEqualTo(expected);
    }

    private static UpdateByEmaTimescale time(final String column, long nanos) {
        return UpdateByEmaTimescale.newBuilder()
                .setTime(UpdateByEmaTimescale.UpdateByEmaTime.newBuilder()
                        .setColumn(column).setPeriodNanos(nanos).build())
                .build();
    }

    private static UpdateByEmaTimescale ticks(long ticks) {
        return UpdateByEmaTimescale.newBuilder()
                .setTicks(UpdateByEmaTimescale.UpdateByEmaTicks
                        .newBuilder().setTicks(ticks).build())
                .build();
    }
}
