package io.deephaven.grpc_api.table.ops;

import com.google.common.collect.Lists;
import com.google.rpc.Code;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.exceptions.ExpressionException;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.select.MatchPairFactory;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.AsOfJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.JoinTablesRequest;
import io.deephaven.util.FunctionalInterfaces;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class AsOfJoinTablesGrpcImpl extends GrpcTableOperation<AsOfJoinTablesRequest> {

    private final LiveTableMonitor liveTableMonitor;

    private static final MultiDependencyFunction<AsOfJoinTablesRequest> EXTRACT_DEPS =
            (request) -> Lists.newArrayList(request.getLeftId(), request.getRightId());

    @Inject
    protected AsOfJoinTablesGrpcImpl(LiveTableMonitor liveTableMonitor) {
        super(BatchTableRequest.Operation::getAsOfJoin, AsOfJoinTablesRequest::getResultId, EXTRACT_DEPS);
        this.liveTableMonitor = liveTableMonitor;
    }

    @Override
    public void validateRequest(final AsOfJoinTablesRequest request) throws StatusRuntimeException {
        try {
            MatchPairFactory.getExpressions(request.getColumnsToMatchList());
            MatchPairFactory.getExpressions(request.getColumnsToAddList());
        } catch (final ExpressionException err) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, err.getMessage() + ": " + err.getProblemExpression());
        }
        if (request.getAsOfMatchRule() == AsOfJoinTablesRequest.MatchRule.UNRECOGNIZED) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Unrecognized as-of match rule");
        }
    }

    @Override
    public Table create(final AsOfJoinTablesRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
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
            Table.AsOfMatchRule matchRule = Table.AsOfMatchRule.valueOf(request.getAsOfMatchRule().name());
            switch (matchRule) {
                case LESS_THAN:
                case LESS_THAN_EQUAL:
                    return lhs.aj(rhs, columnsToMatch, columnsToAdd, matchRule);
                case GREATER_THAN:
                case GREATER_THAN_EQUAL:
                    return lhs.raj(rhs, columnsToMatch, columnsToAdd, matchRule);
                default:
                    throw new RuntimeException("Unsupported join type: " + matchRule);
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
