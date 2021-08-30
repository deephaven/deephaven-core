package io.deephaven.grpc_api.util;

import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation.OpCase;
import io.deephaven.proto.backplane.grpc.TableReference;

import java.util.stream.Stream;

public class OperationHelper {
    public static Stream<TableReference> getSourceIds(Operation op) {
        final OpCase opCase = op.getOpCase();
        if (opCase == null) {
            throw new IllegalArgumentException(
                    "Protocol has been updated, seeing unknown Operation");
        }
        switch (opCase) {
            case EMPTY_TABLE:
            case TIME_TABLE:
                return Stream.empty();
            case DROP_COLUMNS:
                return Stream.of(op.getDropColumns().getSourceId());
            case UPDATE:
                return Stream.of(op.getUpdate().getSourceId());
            case LAZY_UPDATE:
                return Stream.of(op.getLazyUpdate().getSourceId());
            case VIEW:
                return Stream.of(op.getView().getSourceId());
            case UPDATE_VIEW:
                return Stream.of(op.getUpdateView().getSourceId());
            case SELECT:
                return Stream.of(op.getSelect().getSourceId());
            case SELECT_DISTINCT:
                return Stream.of(op.getSelectDistinct().getSourceId());
            case FILTER:
                return Stream.of(op.getFilter().getSourceId());
            case UNSTRUCTURED_FILTER:
                return Stream.of(op.getUnstructuredFilter().getSourceId());
            case SORT:
                return Stream.of(op.getSort().getSourceId());
            case HEAD:
                return Stream.of(op.getHead().getSourceId());
            case TAIL:
                return Stream.of(op.getTail().getSourceId());
            case HEAD_BY:
                return Stream.of(op.getHeadBy().getSourceId());
            case TAIL_BY:
                return Stream.of(op.getTailBy().getSourceId());
            case UNGROUP:
                return Stream.of(op.getUngroup().getSourceId());
            case MERGE:
                return op.getMerge().getSourceIdsList().stream();
            case CROSS_JOIN:
                return Stream.of(op.getCrossJoin().getLeftId(), op.getCrossJoin().getRightId());
            case NATURAL_JOIN:
                return Stream.of(op.getNaturalJoin().getLeftId(), op.getNaturalJoin().getRightId());
            case EXACT_JOIN:
                return Stream.of(op.getExactJoin().getLeftId(), op.getExactJoin().getRightId());
            case LEFT_JOIN:
                return Stream.of(op.getLeftJoin().getLeftId(), op.getLeftJoin().getRightId());
            case AS_OF_JOIN:
                return Stream.of(op.getAsOfJoin().getLeftId(), op.getAsOfJoin().getRightId());
            case COMBO_AGGREGATE:
                return Stream.of(op.getComboAggregate().getSourceId());
            case SNAPSHOT:
                return Stream.of(op.getSnapshot().getLeftId(), op.getSnapshot().getRightId());
            case FLATTEN:
                return Stream.of(op.getFlatten().getSourceId());
            case RUN_CHART_DOWNSAMPLE:
                return Stream.of(op.getRunChartDownsample().getSourceId());
            case FETCH_TABLE:
                return Stream.of(op.getFetchTable().getSourceId());
            case FETCH_PANDAS_TABLE:
                return Stream.of(op.getFetchPandasTable().getSourceId());
            case OP_NOT_SET:
                throw new IllegalStateException("Operation id not set");
            default:
                throw new UnsupportedOperationException("Operation not implemented yet, " + opCase);
        }
    }
}
