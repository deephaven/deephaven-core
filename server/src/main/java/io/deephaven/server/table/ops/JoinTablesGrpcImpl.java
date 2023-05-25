/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.api.AsOfJoinRule;
import io.deephaven.api.ReverseAsOfJoinRule;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.api.expression.ExpressionException;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.AsOfJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.CrossJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.ExactJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.LeftJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.NaturalJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionState;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public abstract class JoinTablesGrpcImpl<T> extends GrpcTableOperation<T> {
    @FunctionalInterface
    protected interface RealTableOperation<T> {
        Table apply(Table lhs, Table rhs, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, T request);
    }

    private final Function<T, List<String>> getColMatchList;
    private final Function<T, List<String>> getColAddList;
    private final UpdateGraphProcessor updateGraphProcessor;
    private final RealTableOperation<T> realTableOperation;

    protected JoinTablesGrpcImpl(final UpdateGraphProcessor updateGraphProcessor,
            final PermissionFunction<T> permission,
            final Function<BatchTableRequest.Operation, T> getRequest,
            final Function<T, Ticket> getTicket,
            final MultiDependencyFunction<T> getDependencies,
            final Function<T, List<String>> getColMatchList,
            final Function<T, List<String>> getColAddList,
            final RealTableOperation<T> realTableOperation) {
        super(permission, getRequest, getTicket, getDependencies);
        this.updateGraphProcessor = updateGraphProcessor;
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
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
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
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    err.getMessage() + ": " + err.getProblemExpression());
        }

        final Table lhs = sourceTables.get(0).get();
        final Table rhs = sourceTables.get(1).get();

        final Table result;
        if (!lhs.isRefreshing() && !rhs.isRefreshing()) {
            result = realTableOperation.apply(lhs, rhs, columnsToMatch, columnsToAdd, request);
        } else {
            result = updateGraphProcessor.sharedLock().computeLocked(
                    () -> realTableOperation.apply(lhs, rhs, columnsToMatch, columnsToAdd, request));
        }
        return result;
    }

    @Singleton
    public static class AsOfJoinTablesGrpcImpl extends JoinTablesGrpcImpl<AsOfJoinTablesRequest> {

        private static final MultiDependencyFunction<AsOfJoinTablesRequest> EXTRACT_DEPS =
                (request) -> List.of(request.getLeftId(), request.getRightId());

        @Inject
        protected AsOfJoinTablesGrpcImpl(
                final TableServiceContextualAuthWiring authWiring,
                final UpdateGraphProcessor updateGraphProcessor) {
            super(updateGraphProcessor, authWiring::checkPermissionAsOfJoinTables,
                    BatchTableRequest.Operation::getAsOfJoin, AsOfJoinTablesRequest::getResultId,
                    EXTRACT_DEPS,
                    AsOfJoinTablesRequest::getColumnsToMatchList, AsOfJoinTablesRequest::getColumnsToAddList,
                    AsOfJoinTablesGrpcImpl::doJoin);
        }

        @Override
        public void validateRequest(final AsOfJoinTablesRequest request) throws StatusRuntimeException {
            super.validateRequest(request);

            if (request.getAsOfMatchRule() == AsOfJoinTablesRequest.MatchRule.UNRECOGNIZED) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Unrecognized as-of match rule");
            }
        }

        public static Table doJoin(final Table lhs, final Table rhs,
                final MatchPair[] columnsToMatch, final MatchPair[] columnsToAdd,
                final AsOfJoinTablesRequest request) {
            final List<MatchPair> match = Arrays.asList(columnsToMatch);
            final List<MatchPair> add = Arrays.asList(columnsToAdd);
            switch (request.getAsOfMatchRule()) {
                case LESS_THAN:
                    return lhs.aj(rhs, match, add, AsOfJoinRule.LESS_THAN);
                case LESS_THAN_EQUAL:
                    return lhs.aj(rhs, match, add, AsOfJoinRule.LESS_THAN_EQUAL);
                case GREATER_THAN:
                    return lhs.raj(rhs, match, add, ReverseAsOfJoinRule.GREATER_THAN);
                case GREATER_THAN_EQUAL:
                    return lhs.raj(rhs, match, add, ReverseAsOfJoinRule.GREATER_THAN_EQUAL);
                default:
                    throw new RuntimeException("Unsupported join type: " + request.getAsOfMatchRule());
            }
        }
    }

    @Singleton
    public static class CrossJoinTablesGrpcImpl extends JoinTablesGrpcImpl<CrossJoinTablesRequest> {

        private static final MultiDependencyFunction<CrossJoinTablesRequest> EXTRACT_DEPS =
                (request) -> List.of(request.getLeftId(), request.getRightId());

        @Inject
        public CrossJoinTablesGrpcImpl(
                final TableServiceContextualAuthWiring authWiring,
                final UpdateGraphProcessor updateGraphProcessor) {
            super(updateGraphProcessor, authWiring::checkPermissionCrossJoinTables,
                    BatchTableRequest.Operation::getCrossJoin, CrossJoinTablesRequest::getResultId,
                    EXTRACT_DEPS,
                    CrossJoinTablesRequest::getColumnsToMatchList, CrossJoinTablesRequest::getColumnsToAddList,
                    CrossJoinTablesGrpcImpl::doJoin);
        }

        public static Table doJoin(final Table lhs, final Table rhs,
                final MatchPair[] columnsToMatch, final MatchPair[] columnsToAdd,
                final CrossJoinTablesRequest request) {
            final List<MatchPair> match = Arrays.asList(columnsToMatch);
            final List<MatchPair> add = Arrays.asList(columnsToAdd);
            int reserveBits = request.getReserveBits();
            if (reserveBits <= 0) {
                return lhs.join(rhs, match, add); // use the default number of reserve_bits
            } else {
                return lhs.join(rhs, match, add, reserveBits);
            }
        }
    }

    @Singleton
    public static class ExactJoinTablesGrpcImpl extends JoinTablesGrpcImpl<ExactJoinTablesRequest> {

        private static final MultiDependencyFunction<ExactJoinTablesRequest> EXTRACT_DEPS =
                (request) -> List.of(request.getLeftId(), request.getRightId());

        @Inject
        public ExactJoinTablesGrpcImpl(
                final TableServiceContextualAuthWiring authWiring,
                final UpdateGraphProcessor updateGraphProcessor) {
            super(updateGraphProcessor, authWiring::checkPermissionExactJoinTables,
                    BatchTableRequest.Operation::getExactJoin, ExactJoinTablesRequest::getResultId,
                    EXTRACT_DEPS,
                    ExactJoinTablesRequest::getColumnsToMatchList, ExactJoinTablesRequest::getColumnsToAddList,
                    ExactJoinTablesGrpcImpl::doJoin);
        }

        public static Table doJoin(final Table lhs, final Table rhs,
                final MatchPair[] columnsToMatch, final MatchPair[] columnsToAdd,
                final ExactJoinTablesRequest request) {
            return lhs.exactJoin(rhs, Arrays.asList(columnsToMatch), Arrays.asList(columnsToAdd));
        }
    }

    @Singleton
    public static class LeftJoinTablesGrpcImpl extends JoinTablesGrpcImpl<LeftJoinTablesRequest> {

        private static final MultiDependencyFunction<LeftJoinTablesRequest> EXTRACT_DEPS =
                (request) -> List.of(request.getLeftId(), request.getRightId());

        @Inject
        public LeftJoinTablesGrpcImpl(
                final TableServiceContextualAuthWiring authWiring,
                final UpdateGraphProcessor updateGraphProcessor) {
            super(updateGraphProcessor, authWiring::checkPermissionLeftJoinTables,
                    BatchTableRequest.Operation::getLeftJoin, LeftJoinTablesRequest::getResultId,
                    EXTRACT_DEPS,
                    LeftJoinTablesRequest::getColumnsToMatchList, LeftJoinTablesRequest::getColumnsToAddList,
                    LeftJoinTablesGrpcImpl::doJoin);
        }

        public static Table doJoin(final Table lhs, final Table rhs,
                final MatchPair[] columnsToMatch, final MatchPair[] columnsToAdd,
                final LeftJoinTablesRequest request) {
            throw Exceptions.statusRuntimeException(Code.UNIMPLEMENTED, "LeftJoinTables is currently unimplemented");
        }
    }

    @Singleton
    public static class NaturalJoinTablesGrpcImpl extends JoinTablesGrpcImpl<NaturalJoinTablesRequest> {

        private static final MultiDependencyFunction<NaturalJoinTablesRequest> EXTRACT_DEPS =
                (request) -> List.of(request.getLeftId(), request.getRightId());

        @Inject
        public NaturalJoinTablesGrpcImpl(
                final TableServiceContextualAuthWiring authWiring,
                final UpdateGraphProcessor updateGraphProcessor) {
            super(updateGraphProcessor, authWiring::checkPermissionNaturalJoinTables,
                    BatchTableRequest.Operation::getNaturalJoin, NaturalJoinTablesRequest::getResultId,
                    EXTRACT_DEPS,
                    NaturalJoinTablesRequest::getColumnsToMatchList, NaturalJoinTablesRequest::getColumnsToAddList,
                    NaturalJoinTablesGrpcImpl::doJoin);
        }

        public static Table doJoin(final Table lhs, final Table rhs,
                final MatchPair[] columnsToMatch, final MatchPair[] columnsToAdd,
                final NaturalJoinTablesRequest request) {
            return lhs.naturalJoin(rhs, Arrays.asList(columnsToMatch), Arrays.asList(columnsToAdd));
        }
    }
}
