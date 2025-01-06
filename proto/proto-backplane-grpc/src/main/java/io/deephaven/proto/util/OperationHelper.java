//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.proto.util;

import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation.OpCase;
import io.deephaven.proto.backplane.grpc.MultiJoinInput;
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
            case AJ:
                return Stream.of(op.getAj().getLeftId(), op.getAj().getRightId());
            case RAJ:
                return Stream.of(op.getRaj().getLeftId(), op.getRaj().getRightId());
            case COMBO_AGGREGATE:
                return Stream.of(op.getComboAggregate().getSourceId());
            case AGGREGATE_ALL:
                return Stream.of(op.getAggregateAll().getSourceId());
            case AGGREGATE:
                return op.getAggregate().hasInitialGroupsId()
                        ? Stream.of(op.getAggregate().getSourceId(), op.getAggregate().getInitialGroupsId())
                        : Stream.of(op.getAggregate().getSourceId());
            case SNAPSHOT:
                return Stream.of(op.getSnapshot().getSourceId());
            case SNAPSHOT_WHEN:
                return Stream.of(op.getSnapshotWhen().getBaseId(), op.getSnapshotWhen().getTriggerId());
            case FLATTEN:
                return Stream.of(op.getFlatten().getSourceId());
            case META_TABLE:
                return Stream.of(op.getMetaTable().getSourceId());
            case RUN_CHART_DOWNSAMPLE:
                return Stream.of(op.getRunChartDownsample().getSourceId());
            case FETCH_TABLE:
                return Stream.of(op.getFetchTable().getSourceId());
            case APPLY_PREVIEW_COLUMNS:
                return Stream.of(op.getApplyPreviewColumns().getSourceId());
            case CREATE_INPUT_TABLE:
                return op.getCreateInputTable().hasSourceTableId()
                        ? Stream.of(op.getCreateInputTable().getSourceTableId())
                        : Stream.empty();
            case UPDATE_BY:
                return Stream.of(op.getUpdateBy().getSourceId());
            case WHERE_IN:
                return Stream.of(op.getWhereIn().getLeftId(), op.getWhereIn().getRightId());
            case RANGE_JOIN:
                return Stream.of(op.getRangeJoin().getLeftId(), op.getRangeJoin().getRightId());
            case COLUMN_STATISTICS:
                return Stream.of(op.getColumnStatistics().getSourceId());
            case MULTI_JOIN:
                return op.getMultiJoin().getMultiJoinInputsList().stream().map(MultiJoinInput::getSourceId);
            case SLICE:
                return Stream.of(op.getSlice().getSourceId());
            case OP_NOT_SET:
                throw new IllegalStateException("Operation id not set");
            default:
                throw new UnsupportedOperationException("Operation not implemented yet, " + opCase);
        }
    }
}
