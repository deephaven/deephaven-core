package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ColumnStatisticsRequest;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.table.stats.CharacterChunkedStats;
import io.deephaven.server.table.stats.ChunkedNumericalStatsKernel;
import io.deephaven.server.table.stats.ColumnChunkedStatsFunction;
import io.deephaven.server.table.stats.DateTimeChunkedStats;
import io.deephaven.server.table.stats.ObjectChunkedStats;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;

@Singleton
public class ColumnStatisticsGrpcImpl extends GrpcTableOperation<ColumnStatisticsRequest> {
    private static final int MAX_UNIQUE_LIMIT = 200;
    private static final int MAX_UNIQUE_DEFAULT = 20;
    private static final int MAX_UNQIUE_TO_COLLECT = 1_000_000;

    @Inject
    public ColumnStatisticsGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionComputeColumnStatistics, BatchTableRequest.Operation::getColumnStatistics,
                ColumnStatisticsRequest::getResultId, ColumnStatisticsRequest::getSourceId);
    }

    @Override
    public Table create(ColumnStatisticsRequest request, List<SessionState.ExportObject<Table>> sourceTables) {
        Table table = sourceTables.get(0).get();
        String columnName = request.getColumnName();
        final Class<?> type = table.getDefinition().getColumn(columnName).getDataType();

        // Based on the column type, make a stats function and get a column source
        final ColumnChunkedStatsFunction statsFunc;
        final ColumnSource<?> columnSource;
        if (TypeUtils.isDateTime(type)) {
            // Instant/ZonedDateTime only look at max/min and count
            statsFunc = new DateTimeChunkedStats();

            // Reinterpret the column to read only long values
            if (Instant.class.isAssignableFrom(type)) {
                columnSource = ReinterpretUtils.instantToLongSource(table.getColumnSource(columnName));
            } else if (ZonedDateTime.class.isAssignableFrom(type)) {
                columnSource = ReinterpretUtils.zonedDateTimeToLongSource(table.getColumnSource(columnName));
            } else {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Datetime columns must be Instant or ZonedDateTime");
            }
        } else if (TypeUtils.isNumeric(type)) {
            // Numeric types have a variety of statistics recorded
            statsFunc = ChunkedNumericalStatsKernel.makeChunkedNumericalStatsFactory(type);
            columnSource = table.getColumnSource(columnName);
        } else {
            // For remaining types the best we can do is count/track unique values in the column
            final int maxUnique;
            if (request.hasMaxUniqueValues()) {
                maxUnique = Math.max(request.getMaxUniqueValues(), MAX_UNIQUE_LIMIT);
            } else {
                maxUnique = MAX_UNIQUE_DEFAULT;
            }
            if (type == Character.class || type == char.class) {
                statsFunc = new CharacterChunkedStats(MAX_UNQIUE_TO_COLLECT, maxUnique);
            } else {
                statsFunc = new ObjectChunkedStats(MAX_UNQIUE_TO_COLLECT, maxUnique);
            }
            columnSource = table.getColumnSource(columnName);
        }

        // Execute the function in a snapshot
        final Mutable<Table> resultHolder = new MutableObject<>();
        ConstructSnapshot.callDataSnapshotFunction("GenerateDBDateTimeStats()",
                ConstructSnapshot.makeSnapshotControl(false, table.isRefreshing(), (NotificationStepSource) table),
                (usePrev, beforeClockValue) -> {
                    final RowSet rowSet = usePrev ? table.getRowSet().prev() : table.getRowSet();
                    resultHolder.setValue(statsFunc.processChunks(rowSet, columnSource, usePrev));
                    return true;
                });
        return resultHolder.getValue();
    }
}
