package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.UngroupRequest;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class UngroupGrpcImpl extends GrpcTableOperation<UngroupRequest> {

    private final LiveTableMonitor liveTableMonitor;

    @Inject
    public UngroupGrpcImpl(final LiveTableMonitor liveTableMonitor) {
        super(BatchTableRequest.Operation::getUngroup, UngroupRequest::getResultId,
            UngroupRequest::getSourceId);
        this.liveTableMonitor = liveTableMonitor;
    }

    @Override
    public Table create(final UngroupRequest request,
        final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        final Table parent = sourceTables.get(0).get();
        final String[] columnsToUngroup =
            request.getColumnsToUngroupList().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        return liveTableMonitor.sharedLock()
            .computeLocked(() -> parent.ungroup(request.getNullFill(), columnsToUngroup));
    }
}
