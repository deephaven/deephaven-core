/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.api.JoinMatch;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.WhereInRequest;
import io.deephaven.server.grpc.Common;
import io.deephaven.server.grpc.GrpcErrorHelper;
import io.deephaven.server.session.SessionState;
import io.deephaven.util.SafeCloseable;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Objects;

@Singleton
public class WhereInGrpcImpl extends GrpcTableOperation<WhereInRequest> {

    private static List<TableReference> refs(WhereInRequest request) {
        return List.of(request.getLeftId(), request.getRightId());
    }

    private final UpdateGraphProcessor updateGraphProcessor;

    @Inject
    public WhereInGrpcImpl(
            final TableServiceContextualAuthWiring authWiring,
            final UpdateGraphProcessor updateGraphProcessor) {
        super(authWiring::checkPermissionWhereIn, BatchTableRequest.Operation::getWhereIn,
                WhereInRequest::getResultId, WhereInGrpcImpl::refs);
        this.updateGraphProcessor = Objects.requireNonNull(updateGraphProcessor);
    }

    @Override
    public final void validateRequest(WhereInRequest request) throws StatusRuntimeException {
        GrpcErrorHelper.checkHasField(request, WhereInRequest.LEFT_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, WhereInRequest.RIGHT_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkRepeatedFieldNonEmpty(request, WhereInRequest.COLUMNS_TO_MATCH_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        Common.validate(request.getLeftId());
        Common.validate(request.getRightId());
    }

    @Override
    public final Table create(final WhereInRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 2);
        Assert.gtZero(request.getColumnsToMatchCount(), "request.getColumnsToMatchCount()");
        final Table left = sourceTables.get(0).get();
        final Table right = sourceTables.get(1).get();
        final List<JoinMatch> columnsToMatch = JoinMatch.from(request.getColumnsToMatchList());
        try (final SafeCloseable _lock = lock(left, right)) {
            return request.getInverted() ? left.whereNotIn(right, columnsToMatch) : left.whereIn(right, columnsToMatch);
        }
    }

    private SafeCloseable lock(Table left, Table right) {
        return left.isRefreshing() || right.isRefreshing() ? updateGraphProcessor.sharedLock().lockCloseable() : null;
    }
}
