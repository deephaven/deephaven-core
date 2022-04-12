package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailByRequest;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.table.validation.ColumnExpressionValidator;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.function.Function;

public abstract class HeadOrTailByGrpcImpl extends GrpcTableOperation<HeadOrTailByRequest> {
    @FunctionalInterface
    protected interface RealTableOperation {
        Table apply(Table source, long nRows, String[] columnSpecs);
    }

    private final RealTableOperation realTableOperation;
    private final UpdateGraphProcessor updateGraphProcessor;

    protected HeadOrTailByGrpcImpl(
            final Function<BatchTableRequest.Operation, HeadOrTailByRequest> getRequest,
            final RealTableOperation realTableOperation,
            final UpdateGraphProcessor updateGraphProcessor) {
        super(getRequest, HeadOrTailByRequest::getResultId, HeadOrTailByRequest::getSourceId);
        this.realTableOperation = realTableOperation;
        this.updateGraphProcessor = updateGraphProcessor;
    }

    @Override
    public void validateRequest(final HeadOrTailByRequest request) throws StatusRuntimeException {
        final long nRows = request.getNumRows();
        if (nRows < 0) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "numRows must be >= 0 (found: " + nRows + ")");
        }
    }

    @Override
    public Table create(final HeadOrTailByRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        final Table parent = sourceTables.get(0).get();
        final String[] columnSpecs =
                request.getGroupByColumnSpecsList().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        final SelectColumn[] expressions = SelectColumnFactory.getExpressions(columnSpecs);

        // note: we don't use the output from validateColumnExpressions because the headBy/tailBy
        // overloads that take SelectColumn arrays throw UnsupportedOperationException, but we validate anyway
        ColumnExpressionValidator.validateColumnExpressions(expressions, columnSpecs, parent);


        // note that headBy/tailBy use ungroup which currently requires the UGP lock
        return updateGraphProcessor.sharedLock()
                .computeLocked(() -> realTableOperation.apply(parent, request.getNumRows(), columnSpecs));
    }

    @Singleton
    public static class HeadByGrpcImpl extends HeadOrTailByGrpcImpl {
        @Inject
        public HeadByGrpcImpl(final UpdateGraphProcessor updateGraphProcessor) {
            super(BatchTableRequest.Operation::getHeadBy, Table::headBy, updateGraphProcessor);
        }
    }

    @Singleton
    public static class TailByGrpcImpl extends HeadOrTailByGrpcImpl {
        @Inject
        public TailByGrpcImpl(final UpdateGraphProcessor updateGraphProcessor) {
            super(BatchTableRequest.Operation::getTailBy, Table::tailBy, updateGraphProcessor);
        }
    }
}
