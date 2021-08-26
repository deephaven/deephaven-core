package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Singleton
public class SnapshotTableGrpcImpl extends GrpcTableOperation<SnapshotTableRequest> {

    private final LiveTableMonitor liveTableMonitor;

    private static final MultiDependencyFunction<SnapshotTableRequest> EXTRACT_DEPS =
            (request) -> {
                if (request.hasLeftId()) {
                    return Arrays.asList(request.getLeftId(), request.getRightId());
                }
                return Collections.singletonList(request.getRightId());
            };

    @Inject
    public SnapshotTableGrpcImpl(final LiveTableMonitor liveTableMonitor) {
        super(BatchTableRequest.Operation::getSnapshot, SnapshotTableRequest::getResultId, EXTRACT_DEPS);
        this.liveTableMonitor = liveTableMonitor;
    }

    @Override
    public Table create(final SnapshotTableRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        final Table lhs;
        final Table rhs;
        if (sourceTables.size() == 1) {
            lhs = TableTools.emptyTable(1);
            rhs = sourceTables.get(0).get();
        } else if (sourceTables.size() == 2) {
            lhs = sourceTables.get(0).get();
            rhs = sourceTables.get(1).get();
        } else {
            throw Assert.statementNeverExecuted("Unexpected sourceTables size " + sourceTables.size());
        }

        final String[] stampColumns = request.getStampColumnsList().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        final SelectColumn[] stampExpressions = SelectColumnFactory.getExpressions(request.getStampColumnsList());
        ColumnExpressionValidator.validateColumnExpressions(stampExpressions, stampColumns, lhs);

        final FunctionalInterfaces.ThrowingSupplier<Table, RuntimeException> doSnapshot =
                () -> lhs.snapshot(rhs, request.getDoInitialSnapshot(), stampColumns);

        final Table result;
        if (!lhs.isLive() && !rhs.isLive()) {
            result = doSnapshot.get();
        } else {
            result = liveTableMonitor.sharedLock().computeLocked(doSnapshot);
        }
        return result;
    }
}
