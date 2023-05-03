package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Pair;
import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.ColumnUpdateOperation;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.spec.*;
import io.deephaven.api.updateby.spec.WindowScale;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.UpdateByEmaTimescale;
import io.deephaven.proto.backplane.grpc.UpdateByRequest;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.*;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOptions;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionState;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public final class UpdateByGrpcImpl extends GrpcTableOperation<UpdateByRequest> {

    @Inject
    public UpdateByGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionUpdateBy, BatchTableRequest.Operation::getUpdateBy,
                UpdateByRequest::getResultId, UpdateByRequest::getSourceId);
    }

    public void validateRequest(final UpdateByRequest request) throws StatusRuntimeException {
        try {
            if (request.getOperationsCount() == 0) {
                // Copied condition from io.deephaven.qst.table.UpdateByTable.checkNumOperations
                throw new IllegalArgumentException("Operations must not be empty");
            }
            if (request.hasOptions()) {
                adaptOptions(request.getOptions());
            }
            for (UpdateByRequest.UpdateByOperation updateByOperation : request.getOperationsList()) {
                adaptOperation(updateByOperation);
            }
            for (String columnName : request.getGroupByColumnsList()) {
                ColumnName.of(columnName);
            }
        } catch (IllegalArgumentException e) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, e.getMessage());
        }

    }

    @Override
    public Table create(final UpdateByRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        final Table parent = sourceTables.get(0).get();
        final UpdateByControl control = request.hasOptions() ? adaptOptions(request.getOptions()) : null;
        final List<UpdateByOperation> operations =
                request.getOperationsList().stream().map(UpdateByGrpcImpl::adaptOperation).collect(Collectors.toList());
        final List<ColumnName> groupByColumns =
                request.getGroupByColumnsList().stream().map(ColumnName::of).collect(Collectors.toList());

        if (parent.isRefreshing()) {
            return UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked(() -> control == null
                    ? parent.updateBy(operations, groupByColumns)
                    : parent.updateBy(control, operations, groupByColumns));
        }

        return control == null ? parent.updateBy(operations, groupByColumns)
                : parent.updateBy(control, operations, groupByColumns);
    }

    private static UpdateByControl adaptOptions(UpdateByOptions options) {
        UpdateByControl.Builder builder = UpdateByControl.builder();
        if (options.hasUseRedirection()) {
            builder.useRedirection(options.getUseRedirection());
        }
        if (options.hasChunkCapacity()) {
            builder.chunkCapacity(options.getChunkCapacity());
        }
        if (options.hasMaxStaticSparseMemoryOverhead()) {
            builder.maxStaticSparseMemoryOverhead(options.getMaxStaticSparseMemoryOverhead());
        }
        if (options.hasInitialHashTableSize()) {
            builder.initialHashTableSize(options.getInitialHashTableSize());
        }
        if (options.hasMaximumLoadFactor()) {
            builder.maximumLoadFactor(options.getMaximumLoadFactor());
        }
        if (options.hasTargetLoadFactor()) {
            builder.targetLoadFactor(options.getTargetLoadFactor());
        }
        if (options.hasMathContext()) {
            builder.mathContext(adaptMathContext(options.getMathContext()));
        }
        return builder.build();
    }

    private static UpdateByOperation adaptOperation(UpdateByRequest.UpdateByOperation operation) {
        switch (operation.getTypeCase()) {
            case COLUMN:
                return adaptColumn(operation.getColumn());
            case TYPE_NOT_SET:
            default:
                throw new IllegalArgumentException("Unexpected operation type case: " + operation.getTypeCase());
        }
    }

    private static ColumnUpdateOperation adaptColumn(UpdateByColumn column) {
        ColumnUpdateOperation.Builder builder = ColumnUpdateOperation.builder()
                .spec(adaptSpec(column.getSpec()));
        for (String matchPair : column.getMatchPairsList()) {
            builder.addColumns(Pair.parse(matchPair));
        }
        return builder.build();
    }

    private static UpdateBySpec adaptSpec(UpdateByColumn.UpdateBySpec spec) {
        switch (spec.getTypeCase()) {
            case SUM:
                return adaptSum(spec.getSum());
            case MIN:
                return adaptMin(spec.getMin());
            case MAX:
                return adaptMax(spec.getMax());
            case PRODUCT:
                return adaptProduct(spec.getProduct());
            case FILL:
                return adaptFill(spec.getFill());
            case EMA:
                return adaptEma(spec.getEma());

            case ROLLING_SUM:
                return adaptRollingSum(spec.getRollingSum());
            case ROLLING_GROUP:
                return adaptRollingGroup(spec.getRollingGroup());
            case ROLLING_AVG:
                return adaptRollingAvg(spec.getRollingAvg());
            case ROLLING_MIN:
                return adaptRollingMin(spec.getRollingMin());
            case ROLLING_MAX:
                return adaptRollingMax(spec.getRollingMax());
            case ROLLING_PRODUCT:
                return adaptRollingProduct(spec.getRollingProduct());

            case TYPE_NOT_SET:
            default:
                throw new IllegalArgumentException("Unexpected spec type: " + spec.getTypeCase());
        }
    }

    private static CumSumSpec adaptSum(@SuppressWarnings("unused") UpdateByCumulativeSum sum) {
        return CumSumSpec.of();
    }

    private static CumMinMaxSpec adaptMin(@SuppressWarnings("unused") UpdateByCumulativeMin min) {
        return CumMinMaxSpec.of(false);
    }

    private static CumMinMaxSpec adaptMax(@SuppressWarnings("unused") UpdateByCumulativeMax max) {
        return CumMinMaxSpec.of(true);
    }

    private static CumProdSpec adaptProduct(@SuppressWarnings("unused") UpdateByCumulativeProduct product) {
        return CumProdSpec.of();
    }

    private static FillBySpec adaptFill(@SuppressWarnings("unused") UpdateByFill fill) {
        return FillBySpec.of();
    }

    private static EmaSpec adaptEma(UpdateByEma ema) {
        return ema.hasOptions() ? EmaSpec.of(adaptEmaOptions(ema.getOptions()), adaptTimescale(ema.getTimescale()))
                : EmaSpec.of(adaptTimescale(ema.getTimescale()));
    }

    private static OperationControl adaptEmaOptions(UpdateByEma.UpdateByEmaOptions options) {
        final OperationControl.Builder builder = OperationControl.builder();
        if (options.hasOnNullValue()) {
            builder.onNullValue(adaptBadDataBehavior(options.getOnNullValue()));
        }
        if (options.hasOnNanValue()) {
            builder.onNanValue(adaptBadDataBehavior(options.getOnNanValue()));
        }
        if (options.hasBigValueContext()) {
            builder.bigValueContext(adaptMathContext(options.getBigValueContext()));
        }
        return builder.build();
    }

    private static RollingSumSpec adaptRollingSum(UpdateByColumn.UpdateBySpec.UpdateByRollingSum sum) {
        return RollingSumSpec.of(
                adaptTimescale(sum.getReverseTimescale()),
                adaptTimescale(sum.getForwardTimescale()));
    }

    private static RollingGroupSpec adaptRollingGroup(UpdateByColumn.UpdateBySpec.UpdateByRollingGroup group) {
        return RollingGroupSpec.of(
                adaptTimescale(group.getReverseTimescale()),
                adaptTimescale(group.getForwardTimescale()));
    }

    private static RollingAvgSpec adaptRollingAvg(UpdateByColumn.UpdateBySpec.UpdateByRollingAvg avg) {
        return RollingAvgSpec.of(
                adaptTimescale(avg.getReverseTimescale()),
                adaptTimescale(avg.getForwardTimescale()));
    }

    private static RollingMinMaxSpec adaptRollingMin(UpdateByColumn.UpdateBySpec.UpdateByRollingMin min) {
        return RollingMinMaxSpec.of(false,
                adaptTimescale(min.getReverseTimescale()),
                adaptTimescale(min.getForwardTimescale()));
    }

    private static RollingMinMaxSpec adaptRollingMax(UpdateByColumn.UpdateBySpec.UpdateByRollingMax max) {
        return RollingMinMaxSpec.of(true,
                adaptTimescale(max.getReverseTimescale()),
                adaptTimescale(max.getForwardTimescale()));
    }

    private static RollingProductSpec adaptRollingProduct(UpdateByColumn.UpdateBySpec.UpdateByRollingProduct product) {
        return RollingProductSpec.of(
                adaptTimescale(product.getReverseTimescale()),
                adaptTimescale(product.getForwardTimescale()));
    }

    private static MathContext adaptMathContext(io.deephaven.proto.backplane.grpc.MathContext bigValueContext) {
        return new MathContext(bigValueContext.getPrecision(), adaptRoundingMode(bigValueContext.getRoundingMode()));
    }

    private static RoundingMode adaptRoundingMode(
            io.deephaven.proto.backplane.grpc.MathContext.RoundingMode roundingMode) {
        switch (roundingMode) {
            case UP:
                return RoundingMode.UP;
            case DOWN:
                return RoundingMode.DOWN;
            case CEILING:
                return RoundingMode.CEILING;
            case FLOOR:
                return RoundingMode.FLOOR;
            case HALF_UP:
                return RoundingMode.HALF_UP;
            case HALF_DOWN:
                return RoundingMode.HALF_DOWN;
            case HALF_EVEN:
                return RoundingMode.HALF_EVEN;
            case UNNECESSARY:
                return RoundingMode.UNNECESSARY;
            case UNRECOGNIZED:
            default:
                throw new IllegalArgumentException("Unexpected rounding mode: " + roundingMode);
        }
    }

    private static WindowScale adaptTimescale(UpdateByEmaTimescale timescale) {
        switch (timescale.getTypeCase()) {
            case TICKS:
                return WindowScale.ofTicks(timescale.getTicks().getTicks());
            case TIME:
                return WindowScale.ofTime(timescale.getTime().getColumn(), timescale.getTime().getPeriodNanos());
            case TYPE_NOT_SET:
            default:
                throw new IllegalArgumentException("Unexpected timescale type: " + timescale.getTypeCase());
        }
    }

    private static BadDataBehavior adaptBadDataBehavior(io.deephaven.proto.backplane.grpc.BadDataBehavior b) {
        switch (b) {
            case RESET:
                return BadDataBehavior.RESET;
            case SKIP:
                return BadDataBehavior.SKIP;
            case THROW:
                return BadDataBehavior.THROW;
            case POISON:
                return BadDataBehavior.POISON;
            case UNRECOGNIZED:
            default:
                throw new IllegalArgumentException("Unexpected BadDataBehavior: " + b);
        }
    }
}
