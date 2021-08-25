package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import com.google.rpc.Code;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.SelectColumnFactory;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.table.validation.ColumnExpressionValidator;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailByRequest;
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
    private final LiveTableMonitor liveTableMonitor;

    protected HeadOrTailByGrpcImpl(
            final Function<BatchTableRequest.Operation, HeadOrTailByRequest> getRequest,
            final RealTableOperation realTableOperation,
            final LiveTableMonitor liveTableMonitor) {
        super(getRequest, HeadOrTailByRequest::getResultId, HeadOrTailByRequest::getSourceId);
        this.realTableOperation = realTableOperation;
        this.liveTableMonitor = liveTableMonitor;
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


        // note that headBy/tailBy use ungroup which currently requires the LTM lock
        return liveTableMonitor.sharedLock()
                .computeLocked(() -> realTableOperation.apply(parent, request.getNumRows(), columnSpecs));
    }

    @Singleton
    public static class HeadByGrpcImpl extends HeadOrTailByGrpcImpl {
        @Inject
        public HeadByGrpcImpl(final LiveTableMonitor liveTableMonitor) {
            super(BatchTableRequest.Operation::getHeadBy, Table::headBy, liveTableMonitor);
        }
    }

    @Singleton
    public static class TailByGrpcImpl extends HeadOrTailByGrpcImpl {
        @Inject
        public TailByGrpcImpl(final LiveTableMonitor liveTableMonitor) {
            super(BatchTableRequest.Operation::getTailBy, Table::tailBy, liveTableMonitor);
        }
    }
}
