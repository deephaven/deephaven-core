package io.deephaven.server.table.ops;

import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ColumnStatisticsRequest;
import io.deephaven.server.session.SessionState;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class ColumnStatisticsGrpcImpl extends GrpcTableOperation<ColumnStatisticsRequest> {
    @Inject
    public ColumnStatisticsGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionColumnStatistics, BatchTableRequest.Operation::getColumnStatistics,
                ColumnStatisticsRequest::getResultId, ColumnStatisticsRequest::getSourceId);
    }

    @Override
    public Table create(ColumnStatisticsRequest request, List<SessionState.ExportObject<Table>> sourceTables) {
        return null;
    }
}
