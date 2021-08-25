package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import com.google.rpc.Code;
import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SelectDistinctRequest;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Singleton
public class SelectDistinctGrpcImpl extends GrpcTableOperation<SelectDistinctRequest> {
    @Inject
    public SelectDistinctGrpcImpl() {
        super(BatchTableRequest.Operation::getSelectDistinct, SelectDistinctRequest::getResultId,
                SelectDistinctRequest::getSourceId);
    }

    @Override
    public Table create(final SelectDistinctRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        final Table parent = sourceTables.get(0).get();

        // explicitly disallow column expressions
        final Set<String> requestedMissing = new HashSet<>(request.getColumnNamesList());
        requestedMissing.removeAll(parent.getDefinition().getColumnNameMap().keySet());
        if (!requestedMissing.isEmpty()) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "column(s) not found: " + String.join(", ", requestedMissing));
        }

        return parent.selectDistinct(request.getColumnNamesList());
    }
}
