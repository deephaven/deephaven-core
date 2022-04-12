package io.deephaven.server.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.UnstructuredFilterTableRequest;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.table.validation.ColumnExpressionValidator;

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
        final WhereFilter[] whereFilters = ColumnExpressionValidator.validateSelectFilters(filters, parent);
        return parent.where(whereFilters);
    }
}
