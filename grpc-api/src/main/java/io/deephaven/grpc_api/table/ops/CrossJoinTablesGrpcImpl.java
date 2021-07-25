package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import com.google.common.collect.Lists;
import com.google.rpc.Code;
import io.deephaven.db.exceptions.ExpressionException;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.select.MatchPairFactory;
import io.deephaven.proto.backplane.grpc.CrossJoinTablesRequest;
import io.deephaven.util.FunctionalInterfaces;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class CrossJoinTablesGrpcImpl extends GrpcTableOperation<CrossJoinTablesRequest> {

    private final LiveTableMonitor liveTableMonitor;

    private static final MultiDependencyFunction<CrossJoinTablesRequest> EXTRACT_DEPS =
            (request) -> Lists.newArrayList(request.getLeftId(), request.getRightId());

    @Inject
    public CrossJoinTablesGrpcImpl(final LiveTableMonitor liveTableMonitor) {
        super(BatchTableRequest.Operation::getCrossJoin, CrossJoinTablesRequest::getResultId, EXTRACT_DEPS);
        this.liveTableMonitor = liveTableMonitor;
    }

    @Override
    public void validateRequest(final CrossJoinTablesRequest request) throws StatusRuntimeException {
        try {
            MatchPairFactory.getExpressions(request.getColumnsToMatchList());
            MatchPairFactory.getExpressions(request.getColumnsToAddList());
        } catch (final ExpressionException err) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, err.getMessage() + ": " + err.getProblemExpression());
        }
    }

    @Override
    public Table create(final CrossJoinTablesRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 2);

        final MatchPair[] columnsToMatch;
        final MatchPair[] columnsToAdd;

        try {
            columnsToMatch = MatchPairFactory.getExpressions(request.getColumnsToMatchList());
            columnsToAdd = MatchPairFactory.getExpressions(request.getColumnsToAddList());
        } catch (final ExpressionException err) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, err.getMessage() + ": " + err.getProblemExpression());
        }

        final Table lhs = sourceTables.get(0).get();
        final Table rhs = sourceTables.get(1).get();

        final FunctionalInterfaces.ThrowingSupplier<Table, RuntimeException> doJoin =
                () -> lhs.join(rhs, columnsToMatch, columnsToAdd, request.getReserveBits());

        final Table result;
        if (!lhs.isLive() && !rhs.isLive()) {
            result = doJoin.get();
        } else {
            result = liveTableMonitor.sharedLock().computeLocked(doJoin);
        }
        return result;
    }
}
