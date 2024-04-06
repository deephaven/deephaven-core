//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailRequest;
import io.deephaven.proto.util.Exceptions;
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
            final PermissionFunction<HeadOrTailRequest> permission,
            final Function<BatchTableRequest.Operation, HeadOrTailRequest> getRequest,
            final RealTableOperation realTableOperation) {
        super(permission, getRequest, HeadOrTailRequest::getResultId, HeadOrTailRequest::getSourceId);
        this.realTableOperation = realTableOperation;
    }

    @Override
    public void validateRequest(final HeadOrTailRequest request) throws StatusRuntimeException {
        final long nRows = request.getNumRows();
        if (nRows < 0) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "numRows must be >= 0 (found: " + nRows + ")");
        }
    }

    @Override
    public Table create(final HeadOrTailRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        final Table source = sourceTables.get(0).get();
        return realTableOperation.apply(source, request.getNumRows());
    }

    @Singleton
    public static class HeadGrpcImpl extends HeadOrTailGrpcImpl {
        @Inject
        public HeadGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
            super(authWiring::checkPermissionHead, BatchTableRequest.Operation::getHead, Table::head);
        }
    }

    @Singleton
    public static class TailGrpcImpl extends HeadOrTailGrpcImpl {
        @Inject
        public TailGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
            super(authWiring::checkPermissionTail, BatchTableRequest.Operation::getTail, Table::tail);
        }
    }
}
