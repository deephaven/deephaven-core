package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.MergeTablesRequest;
import io.deephaven.server.session.SessionState;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class MergeTablesGrpcImpl extends GrpcTableOperation<MergeTablesRequest> {

    private final UpdateGraphProcessor updateGraphProcessor;

    @Inject
    public MergeTablesGrpcImpl(final UpdateGraphProcessor updateGraphProcessor) {
        super(BatchTableRequest.Operation::getMerge, MergeTablesRequest::getResultId,
                MergeTablesRequest::getSourceIdsList);
        this.updateGraphProcessor = updateGraphProcessor;
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
        if (tables.stream().noneMatch(table -> table.isRefreshing())) {
            result = keyColumn.isEmpty() ? TableTools.merge(tables) : TableTools.mergeSorted(keyColumn, tables);
        } else {
            result = updateGraphProcessor.sharedLock().computeLocked(() -> TableTools.merge(tables));
            if (!keyColumn.isEmpty()) {
                result = result.sort(keyColumn);
            }
        }

        return result;
    }
}
