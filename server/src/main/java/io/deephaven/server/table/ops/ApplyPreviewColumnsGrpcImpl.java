/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.auth.AuthContext;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.preview.ColumnPreviewManager;
import io.deephaven.proto.backplane.grpc.ApplyPreviewColumnsRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.table.TableServicePrivilege;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class ApplyPreviewColumnsGrpcImpl extends GrpcTableOperation<ApplyPreviewColumnsRequest> {
    @Inject
    protected ApplyPreviewColumnsGrpcImpl() {
        super(BatchTableRequest.Operation::getApplyPreviewColumns, ApplyPreviewColumnsRequest::getResultId,
                ApplyPreviewColumnsRequest::getSourceId);
    }

    @Override
    public Table create(final AuthContext authContext,
            final ApplyPreviewColumnsRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        authContext.requirePrivilege(TableServicePrivilege.CAN_APPLY_PREVIEW_COLUMNS);
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        return ColumnPreviewManager.applyPreview(sourceTables.get(0).get());
    }
}
