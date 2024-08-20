//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailByRequest;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.table.validation.ColumnExpressionValidator;
import io.deephaven.util.SafeCloseable;
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

    protected HeadOrTailByGrpcImpl(
            final PermissionFunction<HeadOrTailByRequest> permission,
            final Function<BatchTableRequest.Operation, HeadOrTailByRequest> getRequest,
            final RealTableOperation realTableOperation) {
        super(permission, getRequest, HeadOrTailByRequest::getResultId, HeadOrTailByRequest::getSourceId);
        this.realTableOperation = realTableOperation;
    }

    @Override
    public void validateRequest(final HeadOrTailByRequest request) throws StatusRuntimeException {
        final long nRows = request.getNumRows();
        if (nRows < 0) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "numRows must be >= 0 (found: " + nRows + ")");
        }
    }

    @Override
    public Table create(final HeadOrTailByRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        final Table parent = sourceTables.get(0).get();
        final String[] columnSpecs =
                request.getGroupByColumnSpecsList().toArray(String[]::new);
        final SelectColumn[] expressions = SelectColumnFactory.getExpressions(columnSpecs);

        // note: we don't use the output from validateColumnExpressions because the headBy/tailBy
        // overloads that take SelectColumn arrays throw UnsupportedOperationException, but we validate anyway
        ColumnExpressionValidator.validateColumnExpressions(expressions, columnSpecs, parent);

        // note that headBy/tailBy use ungroup which currently requires the UGP lock
        try (final SafeCloseable ignored = lock(parent)) {
            return realTableOperation.apply(parent, request.getNumRows(), columnSpecs);
        }
    }

    private SafeCloseable lock(Table parent) {
        if (parent.isRefreshing()) {
            UpdateGraph updateGraph = parent.getUpdateGraph();
            return updateGraph.sharedLock().lockCloseable();
        } else {
            return null;
        }
    }

    @Singleton
    public static class HeadByGrpcImpl extends HeadOrTailByGrpcImpl {
        @Inject
        public HeadByGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
            super(authWiring::checkPermissionHeadBy, BatchTableRequest.Operation::getHeadBy, Table::headBy);
        }
    }

    @Singleton
    public static class TailByGrpcImpl extends HeadOrTailByGrpcImpl {
        @Inject
        public TailByGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
            super(authWiring::checkPermissionTailBy, BatchTableRequest.Operation::getTailBy, Table::tailBy);
        }
    }
}
