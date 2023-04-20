package io.deephaven.client.impl;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.spec.*;
import io.deephaven.api.updateby.spec.UpdateBySpec.Visitor;
import io.deephaven.proto.backplane.grpc.UpdateByEmaTimescale;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeMax;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeMin;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeProduct;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeSum;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByFill;
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
        public UpdateByColumn.UpdateBySpec visit(EmaSpec ema) {
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

        // TODO: complete properly (DHC ticket #3666)
        @Override
        public UpdateByColumn.UpdateBySpec visit(EmsSpec spec) {
            return null;
        }

        // TODO: complete properly (DHC ticket #3666)
        @Override
        public UpdateByColumn.UpdateBySpec visit(EmMinMaxSpec spec) {
            return null;
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(FillBySpec f) {
            return UpdateByColumn.UpdateBySpec.newBuilder().setFill(UpdateByFill.getDefaultInstance()).build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(CumSumSpec c) {
            return UpdateByColumn.UpdateBySpec.newBuilder().setSum(UpdateByCumulativeSum.getDefaultInstance()).build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(CumMinMaxSpec m) {
            if (m.isMax()) {
                return UpdateByColumn.UpdateBySpec.newBuilder().setMax(UpdateByCumulativeMax.getDefaultInstance())
                        .build();
            } else {
                return UpdateByColumn.UpdateBySpec.newBuilder().setMin(UpdateByCumulativeMin.getDefaultInstance())
                        .build();
            }
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(CumProdSpec p) {
            return UpdateByColumn.UpdateBySpec.newBuilder().setProduct(UpdateByCumulativeProduct.getDefaultInstance())
                    .build();
        }

        // TODO: add this correctly (DHC #3666)
        @Override
        public UpdateByColumn.UpdateBySpec visit(DeltaSpec spec) {
            return null;
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

        // TODO: add this correctly (DHC #3666)
        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingCountSpec spec) {
            return null;
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
                        .setOptions(UpdateByEma.UpdateByEmaOptions.newBuilder()
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
