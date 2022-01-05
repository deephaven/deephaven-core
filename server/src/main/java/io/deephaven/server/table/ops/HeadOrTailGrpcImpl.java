package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailRequest;
import io.deephaven.server.session.SessionState;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.function.Function;

public abstract class HeadOrTailGrpcImpl extends GrpcTableOperation<HeadOrTailRequest> {
    @FunctionalInterface
    protected interface RealTableOperation {
        Table apply(Table source, long nRows);
    }

    private final RealTableOperation realTableOperation;

    protected HeadOrTailGrpcImpl(
            final Function<BatchTableRequest.Operation, HeadOrTailRequest> getRequest,
            final RealTableOperation realTableOperation) {
        super(getRequest, HeadOrTailRequest::getResultId, HeadOrTailRequest::getSourceId);
        this.realTableOperation = realTableOperation;
    }

    @Override
    public void validateRequest(final HeadOrTailRequest request) throws StatusRuntimeException {
        final long nRows = request.getNumRows();
        if (nRows < 0) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "numRows must be >= 0 (found: " + nRows + ")");
        }
    }

    @Override
    public Table create(final HeadOrTailRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        return realTableOperation.apply(sourceTables.get(0).get(), request.getNumRows());
    }

    @Singleton
    public static class HeadGrpcImpl extends HeadOrTailGrpcImpl {
        @Inject
        public HeadGrpcImpl() {
            super(BatchTableRequest.Operation::getHead, Table::head);
        }
    }

    @Singleton
    public static class TailGrpcImpl extends HeadOrTailGrpcImpl {
        @Inject
        public TailGrpcImpl() {
            super(BatchTableRequest.Operation::getTail, Table::tail);
        }
    }
}
