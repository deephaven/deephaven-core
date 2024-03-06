//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.api.AsOfJoinMatch;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.AjRajTablesRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.grpc.Common;
import io.deephaven.server.grpc.GrpcErrorHelper;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.util.SafeCloseable;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public abstract class AjRajGrpcImpl extends GrpcTableOperation<AjRajTablesRequest> {

    @Singleton
    public static class AjGrpcImpl extends AjRajGrpcImpl {
        @Inject
        public AjGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
            super(
                    authWiring::checkPermissionAjTables,
                    Operation::getAj,
                    AsOfJoinMatch::parseForAj);
        }
    }

    @Singleton
    public static class RajGrpcImpl extends AjRajGrpcImpl {
        @Inject
        public RajGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
            super(
                    authWiring::checkPermissionRajTables,
                    Operation::getRaj,
                    AsOfJoinMatch::parseForRaj);
        }
    }

    private final Function<String, AsOfJoinMatch> joinMatchParser;

    private AjRajGrpcImpl(
            PermissionFunction<AjRajTablesRequest> permission,
            Function<Operation, AjRajTablesRequest> getRequest,
            Function<String, AsOfJoinMatch> joinMatchParser) {
        super(
                permission,
                getRequest,
                AjRajTablesRequest::getResultId,
                AjRajGrpcImpl::refs);
        this.joinMatchParser = Objects.requireNonNull(joinMatchParser);
    }

    @Override
    public void validateRequest(AjRajTablesRequest request) throws StatusRuntimeException {
        GrpcErrorHelper.checkHasField(request, AjRajTablesRequest.LEFT_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, AjRajTablesRequest.RIGHT_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, AjRajTablesRequest.AS_OF_COLUMN_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        Common.validate(request.getLeftId());
        Common.validate(request.getRightId());
        try {
            for (String exactMatch : request.getExactMatchColumnsList()) {
                JoinMatch.parse(exactMatch);
            }
            joinMatchParser.apply(request.getAsOfColumn());
            for (String addColumn : request.getColumnsToAddList()) {
                JoinAddition.parse(addColumn);
            }
        } catch (IllegalArgumentException e) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, e.getMessage());
        }
    }

    @Override
    public Table create(AjRajTablesRequest request, List<ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 2);
        final Table left = sourceTables.get(0).get();
        final Table right = sourceTables.get(1).get();
        final List<JoinMatch> exactMatches = JoinMatch.from(request.getExactMatchColumnsList());
        final List<JoinAddition> columnsToAdd = JoinAddition.from(request.getColumnsToAddList());
        final AsOfJoinMatch asOfMatch = joinMatchParser.apply(request.getAsOfColumn());
        // noinspection unused
        try (final SafeCloseable _lock = lock(left, right)) {
            return left.asOfJoin(right, exactMatches, asOfMatch, columnsToAdd);
        }
    }

    private SafeCloseable lock(Table left, Table right) {
        if (left.isRefreshing() || right.isRefreshing()) {
            return left.getUpdateGraph(right).sharedLock().lockCloseable();
        }
        return null;
    }

    private static List<TableReference> refs(AjRajTablesRequest request) {
        return List.of(request.getLeftId(), request.getRightId());
    }
}
