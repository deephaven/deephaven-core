package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.DropColumnsRequest;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class DropColumnsGrpcImpl extends GrpcTableOperation<DropColumnsRequest> {

    @Inject
    public DropColumnsGrpcImpl() {
        super(BatchTableRequest.Operation::getDropColumns, DropColumnsRequest::getResultId,
                DropColumnsRequest::getSourceId);
    }

    @Override
    public Table create(final DropColumnsRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        return sourceTables.get(0).get().dropColumns(request.getColumnNamesList());
    }
}
