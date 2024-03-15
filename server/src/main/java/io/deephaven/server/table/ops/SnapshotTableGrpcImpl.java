//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.SnapshotTableRequest;
import io.deephaven.server.grpc.Common;
import io.deephaven.server.grpc.GrpcErrorHelper;
import io.deephaven.server.session.SessionState;
import io.deephaven.util.SafeCloseable;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public final class SnapshotTableGrpcImpl extends GrpcTableOperation<SnapshotTableRequest> {

    @Inject
    public SnapshotTableGrpcImpl(final TableServiceContextualAuthWiring auth) {
        super(
                auth::checkPermissionSnapshot,
                Operation::getSnapshot,
                SnapshotTableRequest::getResultId,
                SnapshotTableRequest::getSourceId);
    }

    @Override
    public void validateRequest(SnapshotTableRequest request) throws StatusRuntimeException {
        GrpcErrorHelper.checkHasField(request, SnapshotTableRequest.SOURCE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        Common.validate(request.getSourceId());
    }

    @Override
    public Table create(
            final SnapshotTableRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        final Table base = sourceTables.get(0).get();
        try (final SafeCloseable ignored = lock(base)) {
            return base.snapshot();
        }
    }

    private SafeCloseable lock(Table base) {
        if (base.isRefreshing()) {
            return base.getUpdateGraph().sharedLock().lockCloseable();
        } else {
            return null;
        }
    }
}
