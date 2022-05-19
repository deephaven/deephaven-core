package io.deephaven.server.table;

import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.server.session.SessionState;

public abstract class TableAccessBatchDelegator implements TableAccess {
    @Override
    public SessionState batch(BatchTableRequest request) {
        SessionState sessionState = null;
        for (Operation operation : request.getOpsList()) {
            sessionState = batchOperation(operation);
        }
        return sessionState;
    }

    protected SessionState batchOperation(Operation op) {
        switch (op.getOpCase()) {
            case EMPTY_TABLE:
                return emptyTable(op.getEmptyTable());
            case TIME_TABLE:
                return timeTable(op.getTimeTable());
            case DROP_COLUMNS:
                return dropColumns(op.getDropColumns());
            case UPDATE:
                return update(op.getUpdate());
            case LAZY_UPDATE:
                return lazyUpdate(op.getLazyUpdate());
            case VIEW:
                return view(op.getView());
            case UPDATE_VIEW:
                return updateView(op.getUpdateView());
            case SELECT:
                return select(op.getSelect());
            case SELECT_DISTINCT:
                return selectDistinct(op.getSelectDistinct());
            case FILTER:
                return filter(op.getFilter());
            case UNSTRUCTURED_FILTER:
                return unstructuredFilter(op.getUnstructuredFilter());
            case SORT:
                return sort(op.getSort());
            case HEAD:
                return head(op.getHead());
            case TAIL:
                return tail(op.getTail());
            case HEAD_BY:
                return headBy(op.getHeadBy());
            case TAIL_BY:
                return tailBy(op.getTailBy());
            case UNGROUP:
                return ungroup(op.getUngroup());
            case MERGE:
                return mergeTables(op.getMerge());
            case COMBO_AGGREGATE:
                return comboAggregate(op.getComboAggregate());
            case SNAPSHOT:
                return snapshot(op.getSnapshot());
            case FLATTEN:
                return flatten(op.getFlatten());
            case RUN_CHART_DOWNSAMPLE:
                return runChartDownsample(op.getRunChartDownsample());
            case CROSS_JOIN:
                return crossJoinTables(op.getCrossJoin());
            case NATURAL_JOIN:
                return naturalJoinTables(op.getNaturalJoin());
            case EXACT_JOIN:
                return exactJoinTables(op.getExactJoin());
            case LEFT_JOIN:
                return leftJoinTables(op.getLeftJoin());
            case AS_OF_JOIN:
                return asOfJoinTables(op.getAsOfJoin());
            case FETCH_TABLE:
                return fetchTable(op.getFetchTable());
            case APPLY_PREVIEW_COLUMNS:
                return applyPreviewColumns(op.getApplyPreviewColumns());
            case CREATE_INPUT_TABLE:
                return createInputTable(op.getCreateInputTable());
            case OP_NOT_SET:
            case FETCH_PANDAS_TABLE:
            default:
                throw new IllegalArgumentException("Unexpected op: " + op.getOpCase());
        }
    }
}
