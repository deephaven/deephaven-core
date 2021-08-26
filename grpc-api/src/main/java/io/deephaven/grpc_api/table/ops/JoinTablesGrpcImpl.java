package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import com.google.common.collect.Lists;
import com.google.rpc.Code;
import io.deephaven.db.exceptions.ExpressionException;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.select.MatchPairFactory;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.AsOfJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.CrossJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.ExactJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.LeftJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.NaturalJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.function.Function;

public abstract class JoinTablesGrpcImpl<T> extends GrpcTableOperation<T> {
    @FunctionalInterface
    protected interface RealTableOperation<T> {
        Table apply(Table lhs, Table rhs, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, T request);
    }

    private final Function<T, List<String>> getColMatchList;
    private final Function<T, List<String>> getColAddList;
    private final LiveTableMonitor liveTableMonitor;
    private final RealTableOperation<T> realTableOperation;

    protected JoinTablesGrpcImpl(final LiveTableMonitor liveTableMonitor,
            final Function<BatchTableRequest.Operation, T> getRequest,
            final Function<T, Ticket> getTicket,
            final MultiDependencyFunction<T> getDependencies,
            final Function<T, List<String>> getColMatchList,
            final Function<T, List<String>> getColAddList,
            final RealTableOperation<T> realTableOperation) {
        super(getRequest, getTicket, getDependencies);
        this.liveTableMonitor = liveTableMonitor;
        this.getColMatchList = getColMatchList;
        this.getColAddList = getColAddList;
        this.realTableOperation = realTableOperation;
    }

    @Override
    public void validateRequest(final T request) throws StatusRuntimeException {
        try {
            MatchPairFactory.getExpressions(getColMatchList.apply(request));
            MatchPairFactory.getExpressions(getColAddList.apply(request));
        } catch (final ExpressionException err) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                    err.getMessage() + ": " + err.getProblemExpression());
        }
    }

    @Override
    public Table create(final T request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 2);

        final MatchPair[] columnsToMatch;
        final MatchPair[] columnsToAdd;

        try {
            columnsToMatch = MatchPairFactory.getExpressions(getColMatchList.apply(request));
            columnsToAdd = MatchPairFactory.getExpressions(getColAddList.apply(request));
        } catch (final ExpressionException err) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                    err.getMessage() + ": " + err.getProblemExpression());
        }

        final Table lhs = sourceTables.get(0).get();
        final Table rhs = sourceTables.get(1).get();

        final Table result;
        if (!lhs.isLive() && !rhs.isLive()) {
            result = realTableOperation.apply(lhs, rhs, columnsToMatch, columnsToAdd, request);
        } else {
            result = liveTableMonitor.sharedLock().computeLocked(
                    () -> realTableOperation.apply(lhs, rhs, columnsToMatch, columnsToAdd, request));
        }
        return result;
    }

    @Singleton
    public static class AsOfJoinTablesGrpcImpl extends JoinTablesGrpcImpl<AsOfJoinTablesRequest> {

        private static final MultiDependencyFunction<AsOfJoinTablesRequest> EXTRACT_DEPS =
                (request) -> Lists.newArrayList(request.getLeftId(), request.getRightId());

        @Inject
        protected AsOfJoinTablesGrpcImpl(LiveTableMonitor liveTableMonitor) {
            super(liveTableMonitor, BatchTableRequest.Operation::getAsOfJoin, AsOfJoinTablesRequest::getResultId,
                    EXTRACT_DEPS,
                    AsOfJoinTablesRequest::getColumnsToMatchList, AsOfJoinTablesRequest::getColumnsToAddList,
                    AsOfJoinTablesGrpcImpl::doJoin);
        }

        @Override
        public void validateRequest(final AsOfJoinTablesRequest request) throws StatusRuntimeException {
            super.validateRequest(request);

            if (request.getAsOfMatchRule() == AsOfJoinTablesRequest.MatchRule.UNRECOGNIZED) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Unrecognized as-of match rule");
            }
        }

        public static Table doJoin(final Table lhs, final Table rhs,
                final MatchPair[] columnsToMatch, final MatchPair[] columnsToAdd,
                final AsOfJoinTablesRequest request) {
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
        }
    }

    @Singleton
    public static class CrossJoinTablesGrpcImpl extends JoinTablesGrpcImpl<CrossJoinTablesRequest> {

        private static final MultiDependencyFunction<CrossJoinTablesRequest> EXTRACT_DEPS =
                (request) -> Lists.newArrayList(request.getLeftId(), request.getRightId());

        @Inject
        public CrossJoinTablesGrpcImpl(final LiveTableMonitor liveTableMonitor) {
            super(liveTableMonitor, BatchTableRequest.Operation::getCrossJoin, CrossJoinTablesRequest::getResultId,
                    EXTRACT_DEPS,
                    CrossJoinTablesRequest::getColumnsToMatchList, CrossJoinTablesRequest::getColumnsToAddList,
                    CrossJoinTablesGrpcImpl::doJoin);
        }

        public static Table doJoin(final Table lhs, final Table rhs,
                final MatchPair[] columnsToMatch, final MatchPair[] columnsToAdd,
                final CrossJoinTablesRequest request) {
            int reserveBits = request.getReserveBits();
            if (reserveBits <= 0) {
                return lhs.join(rhs, columnsToMatch, columnsToAdd); // use the default number of reserve_bits
            } else {
                return lhs.join(rhs, columnsToMatch, columnsToAdd, reserveBits);
            }
        }
    }

    @Singleton
    public static class ExactJoinTablesGrpcImpl extends JoinTablesGrpcImpl<ExactJoinTablesRequest> {

        private static final MultiDependencyFunction<ExactJoinTablesRequest> EXTRACT_DEPS =
                (request) -> Lists.newArrayList(request.getLeftId(), request.getRightId());

        @Inject
        public ExactJoinTablesGrpcImpl(final LiveTableMonitor liveTableMonitor) {
            super(liveTableMonitor, BatchTableRequest.Operation::getExactJoin, ExactJoinTablesRequest::getResultId,
                    EXTRACT_DEPS,
                    ExactJoinTablesRequest::getColumnsToMatchList, ExactJoinTablesRequest::getColumnsToAddList,
                    ExactJoinTablesGrpcImpl::doJoin);
        }

        public static Table doJoin(final Table lhs, final Table rhs,
                final MatchPair[] columnsToMatch, final MatchPair[] columnsToAdd,
                final ExactJoinTablesRequest request) {
            return lhs.exactJoin(rhs, columnsToMatch, columnsToAdd);
        }
    }

    @Singleton
    public static class LeftJoinTablesGrpcImpl extends JoinTablesGrpcImpl<LeftJoinTablesRequest> {

        private static final MultiDependencyFunction<LeftJoinTablesRequest> EXTRACT_DEPS =
                (request) -> Lists.newArrayList(request.getLeftId(), request.getRightId());

        @Inject
        public LeftJoinTablesGrpcImpl(final LiveTableMonitor liveTableMonitor) {
            super(liveTableMonitor, BatchTableRequest.Operation::getLeftJoin, LeftJoinTablesRequest::getResultId,
                    EXTRACT_DEPS,
                    LeftJoinTablesRequest::getColumnsToMatchList, LeftJoinTablesRequest::getColumnsToAddList,
                    LeftJoinTablesGrpcImpl::doJoin);
        }

        public static Table doJoin(final Table lhs, final Table rhs,
                final MatchPair[] columnsToMatch, final MatchPair[] columnsToAdd,
                final LeftJoinTablesRequest request) {
            return lhs.leftJoin(rhs, columnsToMatch, columnsToAdd);
        }
    }

    @Singleton
    public static class NaturalJoinTablesGrpcImpl extends JoinTablesGrpcImpl<NaturalJoinTablesRequest> {

        private static final MultiDependencyFunction<NaturalJoinTablesRequest> EXTRACT_DEPS =
                (request) -> Lists.newArrayList(request.getLeftId(), request.getRightId());

        @Inject
        public NaturalJoinTablesGrpcImpl(final LiveTableMonitor liveTableMonitor) {
            super(liveTableMonitor, BatchTableRequest.Operation::getNaturalJoin, NaturalJoinTablesRequest::getResultId,
                    EXTRACT_DEPS,
                    NaturalJoinTablesRequest::getColumnsToMatchList, NaturalJoinTablesRequest::getColumnsToAddList,
                    NaturalJoinTablesGrpcImpl::doJoin);
        }

        public static Table doJoin(final Table lhs, final Table rhs,
                final MatchPair[] columnsToMatch, final MatchPair[] columnsToAdd,
                final NaturalJoinTablesRequest request) {
            return lhs.naturalJoin(rhs, columnsToMatch, columnsToAdd);
        }
    }
}
