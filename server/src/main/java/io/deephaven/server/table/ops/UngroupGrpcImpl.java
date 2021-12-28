package io.deephaven.server.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.UngroupRequest;
import io.deephaven.server.session.SessionState;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class UngroupGrpcImpl extends GrpcTableOperation<UngroupRequest> {

    private final UpdateGraphProcessor updateGraphProcessor;

    @Inject
    public UngroupGrpcImpl(final UpdateGraphProcessor updateGraphProcessor) {
        super(BatchTableRequest.Operation::getUngroup, UngroupRequest::getResultId, UngroupRequest::getSourceId);
        this.updateGraphProcessor = updateGraphProcessor;
    }

    @Override
    public Table create(final UngroupRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        final Table parent = sourceTables.get(0).get();
        final String[] columnsToUngroup =
                request.getColumnsToUngroupList().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        return updateGraphProcessor.sharedLock()
                .computeLocked(() -> parent.ungroup(request.getNullFill(), columnsToUngroup));
    }
}
