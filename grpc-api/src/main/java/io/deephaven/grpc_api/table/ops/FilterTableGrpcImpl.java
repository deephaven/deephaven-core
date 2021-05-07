package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.FilterTableRequest;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class FilterTableGrpcImpl extends GrpcTableOperation<FilterTableRequest> {

    @Inject
    public FilterTableGrpcImpl() {
        super(BatchTableRequest.Operation::getFilter, FilterTableRequest::getResultId, FilterTableRequest::getSourceId);
    }

    @Override
    public Table create(final FilterTableRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        throw new UnsupportedOperationException("not yet implemented");
    }
}
