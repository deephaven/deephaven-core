//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Strings;
import io.deephaven.api.Pair;
import io.deephaven.api.updateby.*;
import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.spec.*;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.*;
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

        private static UpdateByEmOptions adapt(OperationControl control) {
            UpdateByEmOptions.Builder builder = UpdateByEmOptions.newBuilder();
            control.onNullValue().map(SpecVisitor::adapt).ifPresent(builder::setOnNullValue);
            control.onNanValue().map(SpecVisitor::adapt).ifPresent(builder::setOnNanValue);
            control.onNullTime().map(SpecVisitor::adapt).ifPresent(builder::setOnNullTime);
            control.onNegativeDeltaTime().map(SpecVisitor::adapt).ifPresent(builder::setOnNegativeDeltaTime);
            control.onZeroDeltaTime().map(SpecVisitor::adapt).ifPresent(builder::setOnZeroDeltaTime);
            control.bigValueContext().map(UpdateByBuilder::adapt).ifPresent(builder::setBigValueContext);
            return builder.build();
        }

        private static UpdateByWindowScale adapt(WindowScale windowScale) {
            if (windowScale.isTimeBased()) {
                return UpdateByWindowScale.newBuilder()
                        .setTime(UpdateByWindowScale.UpdateByWindowTime.newBuilder()
                                .setColumn(windowScale.timestampCol())
                                .setNanos(windowScale.timeUnits())
                                .build())
                        .build();
            } else {
                return UpdateByWindowScale.newBuilder()
                        .setTicks(UpdateByWindowScale.UpdateByWindowTicks.newBuilder()
                                .setTicks(windowScale.tickUnits())
                                .build())
                        .build();
            }
        }

        private static UpdateByDeltaOptions adapt(DeltaControl control) {
            final UpdateByNullBehavior nullBehavior;
            switch (control.nullBehavior()) {
                case ValueDominates:
                    nullBehavior = UpdateByNullBehavior.VALUE_DOMINATES;
                    break;
                case ZeroDominates:
                    nullBehavior = UpdateByNullBehavior.ZERO_DOMINATES;
                    break;
                default:
                    // NULL_DOMINATES is the default state
                    nullBehavior = UpdateByNullBehavior.NULL_DOMINATES;
                    break;
            }

            return UpdateByDeltaOptions.newBuilder()
                    .setNullBehavior(nullBehavior).build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(EmaSpec spec) {
            UpdateByEma.Builder builder = UpdateByEma.newBuilder().setWindowScale(adapt(spec.windowScale()));
            spec.control().map(SpecVisitor::adapt).ifPresent(builder::setOptions);
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setEma(builder.build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(EmsSpec spec) {
            UpdateByEms.Builder builder = UpdateByEms.newBuilder().setWindowScale(adapt(spec.windowScale()));
            spec.control().map(SpecVisitor::adapt).ifPresent(builder::setOptions);
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setEms(builder.build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(EmMinMaxSpec spec) {
            if (spec.isMax()) {
                UpdateByEmMax.Builder builder = UpdateByEmMax.newBuilder().setWindowScale(adapt(spec.windowScale()));
                spec.control().map(SpecVisitor::adapt).ifPresent(builder::setOptions);
                return UpdateByColumn.UpdateBySpec.newBuilder()
                        .setEmMax(builder.build())
                        .build();
            } else {
                UpdateByEmMin.Builder builder = UpdateByEmMin.newBuilder().setWindowScale(adapt(spec.windowScale()));
                spec.control().map(SpecVisitor::adapt).ifPresent(builder::setOptions);
                return UpdateByColumn.UpdateBySpec.newBuilder()
                        .setEmMin(builder.build())
                        .build();
            }
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(FillBySpec spec) {
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setFill(UpdateByFill.getDefaultInstance())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(CumSumSpec spec) {
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setSum(UpdateByCumulativeSum.getDefaultInstance())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(CumMinMaxSpec spec) {
            if (spec.isMax()) {
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
        public UpdateByColumn.UpdateBySpec visit(CumProdSpec spec) {
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setProduct(UpdateByCumulativeProduct.getDefaultInstance())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(EmStdSpec spec) {
            UpdateByEmStd.Builder builder = UpdateByEmStd.newBuilder().setWindowScale(adapt(spec.windowScale()));
            spec.control().map(SpecVisitor::adapt).ifPresent(builder::setOptions);
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setEmStd(builder.build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(DeltaSpec spec) {
            UpdateByDelta.Builder builder = UpdateByDelta.newBuilder();
            spec.deltaControl().map(SpecVisitor::adapt).ifPresent(builder::setOptions);
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setDelta(builder.build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingSumSpec rs) {
            final UpdateByRollingSum.Builder builder =
                    UpdateByRollingSum.newBuilder()
                            .setReverseWindowScale(adapt(rs.revWindowScale()))
                            .setForwardWindowScale(adapt(rs.fwdWindowScale()));
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setRollingSum(builder.build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingGroupSpec rs) {
            final UpdateByRollingGroup.Builder builder =
                    UpdateByRollingGroup.newBuilder()
                            .setReverseWindowScale(adapt(rs.revWindowScale()))
                            .setForwardWindowScale(adapt(rs.fwdWindowScale()));
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setRollingGroup(builder.build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingAvgSpec rs) {
            final UpdateByRollingAvg.Builder builder =
                    UpdateByRollingAvg.newBuilder()
                            .setReverseWindowScale(adapt(rs.revWindowScale()))
                            .setForwardWindowScale(adapt(rs.fwdWindowScale()));
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setRollingAvg(builder.build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingMinMaxSpec rs) {
            if (rs.isMax()) {
                final UpdateByRollingMax.Builder builder =
                        UpdateByRollingMax.newBuilder()
                                .setReverseWindowScale(adapt(rs.revWindowScale()))
                                .setForwardWindowScale(adapt(rs.fwdWindowScale()));
                return UpdateByColumn.UpdateBySpec.newBuilder()
                        .setRollingMax(builder.build())
                        .build();
            } else {
                final UpdateByRollingMin.Builder builder =
                        UpdateByRollingMin.newBuilder()
                                .setReverseWindowScale(adapt(rs.revWindowScale()))
                                .setForwardWindowScale(adapt(rs.fwdWindowScale()));
                return UpdateByColumn.UpdateBySpec.newBuilder()
                        .setRollingMin(builder.build())
                        .build();
            }
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingProductSpec rs) {
            final UpdateByRollingProduct.Builder builder =
                    UpdateByRollingProduct.newBuilder()
                            .setReverseWindowScale(adapt(rs.revWindowScale()))
                            .setForwardWindowScale(adapt(rs.fwdWindowScale()));
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setRollingProduct(builder.build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingCountSpec rs) {
            final UpdateByRollingCount.Builder builder =
                    UpdateByRollingCount.newBuilder()
                            .setReverseWindowScale(adapt(rs.revWindowScale()))
                            .setForwardWindowScale(adapt(rs.fwdWindowScale()));
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setRollingCount(builder.build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingStdSpec rs) {
            final UpdateByRollingStd.Builder builder =
                    UpdateByRollingStd.newBuilder()
                            .setReverseWindowScale(adapt(rs.revWindowScale()))
                            .setForwardWindowScale(adapt(rs.fwdWindowScale()));
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setRollingStd(builder.build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingWAvgSpec rs) {
            final UpdateByRollingWAvg.Builder builder =
                    UpdateByRollingWAvg.newBuilder()
                            .setReverseWindowScale(adapt(rs.revWindowScale()))
                            .setForwardWindowScale(adapt(rs.fwdWindowScale()))
                            .setWeightColumn(rs.weightCol());
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setRollingWavg(builder.build())
                    .build();
        }

        @Override
        public UpdateByColumn.UpdateBySpec visit(RollingFormulaSpec rs) {
            final UpdateByRollingFormula.Builder builder =
                    UpdateByRollingFormula.newBuilder()
                            .setReverseWindowScale(adapt(rs.revWindowScale()))
                            .setForwardWindowScale(adapt(rs.fwdWindowScale()))
                            .setFormula(rs.formula())
                            .setParamToken(rs.paramToken().orElse(null));
            return UpdateByColumn.UpdateBySpec.newBuilder()
                    .setRollingFormula(builder.build())
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
