//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.preview.ColumnPreviewManager;
import io.deephaven.proto.backplane.grpc.ApplyPreviewColumnsRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.server.session.SessionState;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class ApplyPreviewColumnsGrpcImpl extends GrpcTableOperation<ApplyPreviewColumnsRequest> {
    @Inject
    protected ApplyPreviewColumnsGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionApplyPreviewColumns, BatchTableRequest.Operation::getApplyPreviewColumns,
                ApplyPreviewColumnsRequest::getResultId, ApplyPreviewColumnsRequest::getSourceId);
    }

    @Override
    public Table create(final ApplyPreviewColumnsRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        final Table source = sourceTables.get(0).get();
        return ColumnPreviewManager.applyPreview(source);
    }
}
