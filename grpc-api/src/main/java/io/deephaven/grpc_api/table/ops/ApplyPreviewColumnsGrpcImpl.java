package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.remote.preview.ColumnPreviewManager;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.proto.backplane.grpc.ApplyPreviewColumnsRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;

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
    public Table create(ApplyPreviewColumnsRequest request, List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        return ColumnPreviewManager.applyPreview(sourceTables.get(0).get());
    }
}
