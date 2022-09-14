package io.deephaven.client.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Strings;
import io.deephaven.api.agg.Pair;
import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.ColumnUpdateOperation;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.spec.CumMinMaxSpec;
import io.deephaven.api.updateby.spec.CumProdSpec;
import io.deephaven.api.updateby.spec.CumSumSpec;
import io.deephaven.api.updateby.spec.EmaSpec;
import io.deephaven.api.updateby.spec.FillBySpec;
import io.deephaven.api.updateby.spec.TimeScale;
import io.deephaven.api.updateby.spec.UpdateBySpec;
import io.deephaven.proto.backplane.grpc.UpdateByRequest;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeMax;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeMin;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeProduct;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeSum;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma.UpdateByEmaOptions;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma.UpdateByEmaTimescale;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma.UpdateByEmaTimescale.UpdateByEmaTicks;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma.UpdateByEmaTimescale.UpdateByEmaTime;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByFill;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOptions;
import io.deephaven.qst.table.UpdateByTable;

import java.math.MathContext;
import java.math.RoundingMode;

class UpdateByBuilder {

    public static UpdateByRequest.Builder adapt(UpdateByTable updateByTable) {
        UpdateByRequest.Builder builder = UpdateByRequest.newBuilder();
        updateByTable.control().map(UpdateByBuilder::adapt).ifPresent(builder::setOptions);
        for (UpdateByOperation operation : updateByTable.operations()) {
            builder.addOperations(adapt(operation));
        }
        for (ColumnName groupByColumn : updateByTable.groupByColumns()) {
            builder.addGroupByColumns(groupByColumn.name());
        }
        return builder;
    }

    private enum OperationVisitor implements UpdateByOperation.Visitor<UpdateByRequest.UpdateByOperation> {
        INSTANCE;

        @Override
        public UpdateByRequest.UpdateByOperation visit(ColumnUpdateOperation clause) {
            return UpdateByRequest.UpdateByOperation.newBuilder().setColumn(adapt(clause)).build();
        }
    }

    static UpdateByRequest.UpdateByOperation adapt(UpdateByOperation clause) {
        return clause.walk(OperationVisitor.INSTANCE);
    }

    private static UpdateByColumn adapt(ColumnUpdateOperation columnUpdate) {
        UpdateByColumn.Builder builder = UpdateByColumn.newBuilder()
                .setSpec(adapt(columnUpdate.spec()));
        for (Pair pair : columnUpdate.columns()) {
            builder.addMatchPairs(Strings.of(pair));
        }
        return builder.build();
    }

    private enum SpecVisitor implements UpdateBySpec.Visitor<UpdateByColumn.UpdateBySpec> {
        INSTANCE;

        private static io.deephaven.proto.backplane.grpc.BadDataBehavior adapt(BadDataBehavior b) {
            switch (b) {
                case RESET:
                    return io.deephaven.proto.backplane.grpc.BadDataBehavior.RESET;
                case SKIP:
                    return io.deephaven.proto.backplane.grpc.BadDataBehavior.SKIP;
                case THROW:
                    return io.deephaven.proto.backplane.grpc.BadDataBehavior.THROW;
                case POISON:
                    return io.deephaven.proto.backplane.grpc.BadDataBehavior.POISON;
                default:
                    throw new IllegalArgumentException("Unexpected BadDataBehavior: " + b);
            }
        }

        private static UpdateByEmaOptions adapt(OperationControl control) {
            UpdateByEmaOptions.Builder builder = UpdateByEmaOptions.newBuilder();
            control.onNullValue().map(SpecVisitor::adapt).ifPresent(builder::setOnNullValue);
            control.onNanValue().map(SpecVisitor::adapt).ifPresent(builder::setOnNanValue);
            control.onNullTime().map(SpecVisitor::adapt).ifPresent(builder::setOnNullTime);
            control.onNegativeDeltaTime().map(SpecVisitor::adapt).ifPresent(builder::setOnNegativeDeltaTime);
            control.onZeroDeltaTime().map(SpecVisitor::adapt).ifPresent(builder::setOnZeroDeltaTime);
            control.bigValueContext().map(UpdateByBuilder::adapt).ifPresent(builder::setBigValueContext);
            return builder.build();
        }

        private static UpdateByEmaTimescale adapt(TimeScale timeScale) {
            if (timeScale.isTimeBased()) {
                return UpdateByEmaTimescale.newBuilder()
                        .setTime(UpdateByEmaTime.newBuilder()
                                .setColumn(timeScale.timestampCol())
                                .setPeriodNanos(timeScale.timescaleUnits())
                                .build())
                        .build();
            } else {
                return UpdateByEmaTimescale.newBuilder()
                        .setTicks(UpdateByEmaTicks.newBuilder()
                                .setTicks(timeScale.timescaleUnits())
                                .build())
                        .build();
            }
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(EmaSpec ema) {
            UpdateByEma.Builder builder = UpdateByEma.newBuilder().setTimescale(adapt(ema.timeScale()));
            ema.control().map(SpecVisitor::adapt).ifPresent(builder::setOptions);
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setEma(builder.build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(FillBySpec f) {
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setFill(UpdateByFill.getDefaultInstance())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(CumSumSpec c) {
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setSum(UpdateByCumulativeSum.getDefaultInstance())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(CumMinMaxSpec m) {
            if (m.isMax()) {
                return UpdateByColumn.UpdateBySpec.newBuilder()
                        .setMax(UpdateByCumulativeMax.getDefaultInstance())
                        .build();
            } else {
                return UpdateByColumn.UpdateBySpec.newBuilder()
                        .setMin(UpdateByCumulativeMin.getDefaultInstance())
                        .build();
            }
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(CumProdSpec p) {
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setProduct(UpdateByCumulativeProduct.getDefaultInstance())
                    .build();
        }
    }

    static UpdateByColumn.UpdateBySpec adapt(UpdateBySpec spec) {
        return spec.walk(SpecVisitor.INSTANCE);
    }

    static UpdateByOptions adapt(UpdateByControl control) {
        UpdateByOptions.Builder builder = UpdateByOptions.newBuilder();
        final Boolean useRedirection = control.useRedirection();
        if (useRedirection != null) {
            builder.setUseRedirection(useRedirection);
        }
        control.chunkCapacity().ifPresent(builder::setChunkCapacity);
        control.maxStaticSparseMemoryOverhead().ifPresent(builder::setMaxStaticSparseMemoryOverhead);
        control.initialHashTableSize().ifPresent(builder::setInitialHashTableSize);
        control.maximumLoadFactor().ifPresent(builder::setMaximumLoadFactor);
        control.targetLoadFactor().ifPresent(builder::setTargetLoadFactor);
        control.mathContext().map(UpdateByBuilder::adapt).ifPresent(builder::setMathContext);
        return builder.build();
    }

    static io.deephaven.proto.backplane.grpc.MathContext adapt(MathContext mathContext) {
        return io.deephaven.proto.backplane.grpc.MathContext.newBuilder()
                .setPrecision(mathContext.getPrecision())
                .setRoundingMode(adapt(mathContext.getRoundingMode()))
                .build();
    }

    private static io.deephaven.proto.backplane.grpc.MathContext.RoundingMode adapt(RoundingMode roundingMode) {
        switch (roundingMode) {
            case UP:
                return io.deephaven.proto.backplane.grpc.MathContext.RoundingMode.UP;
            case DOWN:
                return io.deephaven.proto.backplane.grpc.MathContext.RoundingMode.DOWN;
            case CEILING:
                return io.deephaven.proto.backplane.grpc.MathContext.RoundingMode.CEILING;
            case FLOOR:
                return io.deephaven.proto.backplane.grpc.MathContext.RoundingMode.FLOOR;
            case HALF_UP:
                return io.deephaven.proto.backplane.grpc.MathContext.RoundingMode.HALF_UP;
            case HALF_DOWN:
                return io.deephaven.proto.backplane.grpc.MathContext.RoundingMode.HALF_DOWN;
            case HALF_EVEN:
                return io.deephaven.proto.backplane.grpc.MathContext.RoundingMode.HALF_EVEN;
            case UNNECESSARY:
                return io.deephaven.proto.backplane.grpc.MathContext.RoundingMode.UNNECESSARY;
            default:
                throw new IllegalArgumentException("Unexpected rounding mode: " + roundingMode);
        }
    }
}
