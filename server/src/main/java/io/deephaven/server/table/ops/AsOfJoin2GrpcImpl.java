package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.api.AsOfJoinMatch;
import io.deephaven.api.AsOfJoinRule;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.AsOfJoinTables2Request;
import io.deephaven.proto.backplane.grpc.AsOfJoinTables2Request.AsOfRule;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
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

@Singleton
public final class AsOfJoin2GrpcImpl extends GrpcTableOperation<AsOfJoinTables2Request> {

    private final UpdateGraphProcessor updateGraphProcessor;

    @Inject
    public AsOfJoin2GrpcImpl(
            final TableServiceContextualAuthWiring authWiring,
            final UpdateGraphProcessor updateGraphProcessor) {
        super(
                authWiring::checkPermissionAsOfJoinTables2,
                BatchTableRequest.Operation::getAsOfJoin2,
                AsOfJoinTables2Request::getResultId,
                AsOfJoin2GrpcImpl::refs);
        this.updateGraphProcessor = updateGraphProcessor;
    }

    @Override
    public void validateRequest(AsOfJoinTables2Request request) throws StatusRuntimeException {
        GrpcErrorHelper.checkHasField(request, AsOfJoinTables2Request.LEFT_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, AsOfJoinTables2Request.RIGHT_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, AsOfJoinTables2Request.LEFT_COLUMN_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, AsOfJoinTables2Request.RULE_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, AsOfJoinTables2Request.RIGHT_COLUMN_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        Common.validate(request.getLeftId());
        Common.validate(request.getRightId());
        try {
            for (String exactMatch : request.getExactMatchColumnsList()) {
                JoinMatch.parse(exactMatch);
            }
            ColumnName.parse(request.getLeftColumn());
            adapt(request.getRule());
            ColumnName.parse(request.getRightColumn());
            for (String addColumn : request.getColumnsToAddList()) {
                JoinAddition.parse(addColumn);
            }
        } catch (IllegalArgumentException e) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, e.getMessage());
        }
    }

    @Override
    public Table create(AsOfJoinTables2Request request, List<ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 2);
        final Table left = sourceTables.get(0).get();
        final Table right = sourceTables.get(1).get();
        final List<JoinMatch> exactMatches = JoinMatch.from(request.getExactMatchColumnsList());
        final List<JoinAddition> columnsToAdd = JoinAddition.from(request.getColumnsToAddList());
        final AsOfJoinMatch asOfMatch = AsOfJoinMatch.of(
                ColumnName.parse(request.getLeftColumn()),
                adapt(request.getRule()),
                ColumnName.parse(request.getRightColumn()));
        // noinspection unused
        try (final SafeCloseable _lock = lock(left, right)) {
            return left.asOfJoin(right, exactMatches, asOfMatch, columnsToAdd);
        }
    }

    private SafeCloseable lock(Table left, Table right) {
        return left.isRefreshing() || right.isRefreshing() ? updateGraphProcessor.sharedLock().lockCloseable() : null;
    }

    private static List<TableReference> refs(AsOfJoinTables2Request request) {
        return List.of(request.getLeftId(), request.getRightId());
    }

    private static AsOfJoinRule adapt(AsOfRule rule) {
        switch (rule) {
            case GEQ:
                return AsOfJoinRule.GREATER_THAN_EQUAL;
            case GT:
                return AsOfJoinRule.GREATER_THAN;
            case LEQ:
                return AsOfJoinRule.LESS_THAN_EQUAL;
            case LT:
                return AsOfJoinRule.LESS_THAN;
            default:
                throw new IllegalArgumentException(String.format("Unexpected rule: %s", rule));
        }
    }
}
