package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.select.SelectFilter;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.table.validation.ColumnExpressionValidator;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.UnstructuredFilterTableRequest;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class UnstructuredFilterTableGrpcImpl extends GrpcTableOperation<UnstructuredFilterTableRequest> {

    @Inject
    public UnstructuredFilterTableGrpcImpl() {
        super(BatchTableRequest.Operation::getUnstructuredFilter, UnstructuredFilterTableRequest::getResultId,
                UnstructuredFilterTableRequest::getSourceId);
    }

    @Override
    public Table create(final UnstructuredFilterTableRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        final Table parent = sourceTables.get(0).get();
        final String[] filters = request.getFiltersList().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        final SelectFilter[] selectFilters = ColumnExpressionValidator.validateSelectFilters(filters, parent);
        return parent.where(selectFilters);
    }
}
