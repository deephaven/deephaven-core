package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import com.google.common.collect.Lists;
import com.google.rpc.Code;
import io.deephaven.db.exceptions.ExpressionException;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.select.MatchPairFactory;
import io.deephaven.util.FunctionalInterfaces;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.JoinTablesRequest;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class JoinTablesGrpcImpl extends GrpcTableOperation<JoinTablesRequest> {

    private final LiveTableMonitor liveTableMonitor;

    private static final MultiDependencyFunction<JoinTablesRequest> EXTRACT_DEPS =
            (request) -> Lists.newArrayList(request.getLeftId(), request.getRightId());

    @Inject
    public JoinTablesGrpcImpl(final LiveTableMonitor liveTableMonitor) {
        super(BatchTableRequest.Operation::getJoin, JoinTablesRequest::getResultId, EXTRACT_DEPS);
        this.liveTableMonitor = liveTableMonitor;
    }

    @Override
    public void validateRequest(final JoinTablesRequest request) throws StatusRuntimeException {
        try {
            MatchPairFactory.getExpressions(request.getColumnsToMatchList());
            MatchPairFactory.getExpressions(request.getColumnsToAddList());
        } catch (final ExpressionException err) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, err.getMessage() + ": " + err.getProblemExpression());
        }
        if (request.getJoinType() == JoinTablesRequest.Type.UNRECOGNIZED) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Unrecognized join type");
        }
    }

    @Override
    public Table create(final JoinTablesRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
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

        final FunctionalInterfaces.ThrowingSupplier<Table, RuntimeException> doJoin = () -> {
            switch(request.getJoinType()) {
                case CROSS_JOIN:
                    return lhs.join(rhs, columnsToMatch, columnsToAdd);
                case NATURAL_JOIN:
                    return lhs.naturalJoin(rhs, columnsToMatch, columnsToAdd);
                case EXACT_JOIN:
                    return lhs.exactJoin(rhs, columnsToMatch, columnsToAdd);
                case LEFT_JOIN:
                    return lhs.leftJoin(rhs, columnsToMatch, columnsToAdd);
                default:
                    throw new RuntimeException("Unsupported join type: " + request.getJoinType());
            }
        };

        final Table result;
        if (!lhs.isLive() && !rhs.isLive()) {
            result = doJoin.get();
        } else {
            result = liveTableMonitor.sharedLock().computeLocked(doJoin);
        }
        return result;
    }
}
