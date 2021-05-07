package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import com.google.common.collect.Lists;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.SelectColumnFactory;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.util.FunctionalInterfaces;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.table.validation.ColumnExpressionValidator;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SnapshotTableRequest;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class SnapshotTableGrpcImpl extends GrpcTableOperation<SnapshotTableRequest> {

    private final LiveTableMonitor liveTableMonitor;

    private static final MultiDependencyFunction<SnapshotTableRequest> EXTRACT_DEPS =
            (request) -> Lists.newArrayList(request.hasLeftId() ? request.getLeftId() : null, request.getRightId());

    @Inject
    public SnapshotTableGrpcImpl(final LiveTableMonitor liveTableMonitor) {
        super(BatchTableRequest.Operation::getSnapshot, SnapshotTableRequest::getResultId, EXTRACT_DEPS);
        this.liveTableMonitor = liveTableMonitor;
    }

    @Override
    public Table create(final SnapshotTableRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 2);

        final SessionState.ExportObject<Table> leftSource = sourceTables.get(0);
        final Table lhs = leftSource == null ? TableTools.emptyTable(1) : leftSource.get();
        final Table rhs = sourceTables.get(1).get();

        final String[] stampColumns = request.getStampColumnsList().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        final SelectColumn[] stampExpressions = SelectColumnFactory.getExpressions(request.getStampColumnsList());
        ColumnExpressionValidator.validateColumnExpressions(stampExpressions, stampColumns, lhs);

        final FunctionalInterfaces.ThrowingSupplier<Table, RuntimeException> doSnapshot =
                () -> lhs.snapshot(rhs, request.getDoInitialSnapshot(), stampColumns);

        final Table result;
        if (!lhs.isLive() && !rhs.isLive()) {
            result = doSnapshot.get();
        }  else {
            result = liveTableMonitor.sharedLock().computeLocked(doSnapshot);
        }
        return result;
    }
}
