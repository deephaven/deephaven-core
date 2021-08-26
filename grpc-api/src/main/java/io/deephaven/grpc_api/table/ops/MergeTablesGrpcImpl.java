package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import com.google.rpc.Code;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.MergeTablesRequest;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class MergeTablesGrpcImpl extends GrpcTableOperation<MergeTablesRequest> {

    private final LiveTableMonitor liveTableMonitor;

    @Inject
    public MergeTablesGrpcImpl(final LiveTableMonitor liveTableMonitor) {
        super(BatchTableRequest.Operation::getMerge, MergeTablesRequest::getResultId,
                MergeTablesRequest::getSourceIdsList);
        this.liveTableMonitor = liveTableMonitor;
    }

    @Override
    public void validateRequest(final MergeTablesRequest request) throws StatusRuntimeException {
        if (request.getSourceIdsList().isEmpty()) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Cannot merge zero source tables.");
        }
    }

    @Override
    public Table create(final MergeTablesRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.gt(sourceTables.size(), "sourceTables.size()", 0);

        final String keyColumn = request.getKeyColumn();
        final List<Table> tables = sourceTables.stream()
                .map(SessionState.ExportObject::get)
                .collect(Collectors.toList());

        Table result;
        if (tables.stream().noneMatch(Table::isLive)) {
            result = keyColumn.isEmpty() ? TableTools.merge(tables) : TableTools.mergeSorted(keyColumn, tables);
        } else {
            result = liveTableMonitor.sharedLock().computeLocked(() -> TableTools.merge(tables));
            if (!keyColumn.isEmpty()) {
                result = result.sort(keyColumn);
            }
        }

        return result;
    }
}
