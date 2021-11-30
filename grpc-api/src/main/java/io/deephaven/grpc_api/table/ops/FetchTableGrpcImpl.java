package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.FetchTableRequest;

import javax.inject.Inject;
import java.util.List;

public class FetchTableGrpcImpl extends GrpcTableOperation<FetchTableRequest> {
    @Inject()
    protected FetchTableGrpcImpl() {
        super(BatchTableRequest.Operation::getFetchTable, FetchTableRequest::getResultId,
                FetchTableRequest::getSourceId);
    }

    @Override
    public Table create(FetchTableRequest request, List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        return sourceTables.get(0).get();
    }
}
