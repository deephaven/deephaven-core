package io.deephaven.client.impl;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.spec.CumMinMaxSpec;
import io.deephaven.api.updateby.spec.CumProdSpec;
import io.deephaven.api.updateby.spec.CumSumSpec;
import io.deephaven.api.updateby.spec.EmaSpec;
import io.deephaven.api.updateby.spec.FillBySpec;
import io.deephaven.api.updateby.spec.UpdateBySpec;
import io.deephaven.api.updateby.spec.UpdateBySpec.Visitor;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeMax;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeMin;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeProduct;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeSum;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma.UpdateByEmaTimescale;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma.UpdateByEmaTimescale.UpdateByEmaTime;
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
                                    .setTimescale(UpdateByEmaTimescale.newBuilder().setTime(UpdateByEmaTime.newBuilder()
                                            .setColumn("Timestamp").setPeriodNanos(1).build()).build())
                                    .build())
                    .build();
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

    private static void check(UpdateBySpec spec) {
        check(spec, spec.walk(ExpectedSpecVisitor.INSTANCE));
    }

    private static void check(UpdateBySpec spec, UpdateByColumn.UpdateBySpec expected) {
        assertThat(UpdateByBuilder.adapt(spec)).isEqualTo(expected);
    }
}
