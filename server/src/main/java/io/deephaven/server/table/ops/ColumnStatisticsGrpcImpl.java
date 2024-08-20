//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnDefinition;
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
import io.deephaven.server.table.stats.ChunkedStatsKernel;
import io.deephaven.server.table.stats.DateTimeChunkedStats;
import io.deephaven.server.table.stats.ObjectChunkedStats;
import io.deephaven.util.type.NumericTypeUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;

@Singleton
public class ColumnStatisticsGrpcImpl extends GrpcTableOperation<ColumnStatisticsRequest> {
    /** Default number of unique values to collect from a column. */
    private static final int DEFAULT_UNIQUE_LIMIT =
            Configuration.getInstance().getIntegerWithDefault("ColumnStatistics.defaultUniqueLimit", 20);
    /** Maximum number of unique values to let a client request. */
    private static final int MAX_UNIQUE_LIMIT =
            Configuration.getInstance().getIntegerWithDefault("ColumnStatistics.maxUniqueLimit", 200);
    /**
     * Maximum number of unique values to collect before falling back and only collecting the count, not the most common
     * values
     */
    private static final int MAX_UNIQUE_TO_COLLECT =
            Configuration.getInstance().getIntegerWithDefault("ColumnStatistics.maxUniqueToCollect", 1_000_000);

    @Inject
    public ColumnStatisticsGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionComputeColumnStatistics, BatchTableRequest.Operation::getColumnStatistics,
                ColumnStatisticsRequest::getResultId, ColumnStatisticsRequest::getSourceId);
    }

    @Override
    public Table create(ColumnStatisticsRequest request, List<SessionState.ExportObject<Table>> sourceTables) {
        Table table = sourceTables.get(0).get().coalesce();
        String columnName = request.getColumnName();
        ColumnDefinition<Object> column = table.getDefinition().getColumn(columnName);
        if (column == null) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Table doesn't have a column with the specified name");
        }
        final Class<?> type = column.getDataType();

        // Based on the column type, make a stats function and get a column source
        final ChunkedStatsKernel statsFunc;
        final ColumnSource<?> columnSource;
        if (type == Instant.class) {
            statsFunc = new DateTimeChunkedStats();
            columnSource = ReinterpretUtils.instantToLongSource(table.getColumnSource(columnName));
        } else if (type == ZonedDateTime.class) {
            statsFunc = new DateTimeChunkedStats();
            columnSource = ReinterpretUtils.zonedDateTimeToLongSource(table.getColumnSource(columnName));
        } else if (NumericTypeUtils.isNumeric(type)) {
            // Numeric types have a variety of statistics recorded
            statsFunc = ChunkedNumericalStatsKernel.makeChunkedNumericalStats(type);
            columnSource = table.getColumnSource(columnName);
        } else {
            // For remaining types the best we can do is count/track unique values in the column
            final int maxUnique;
            if (request.hasUniqueValueLimit()) {
                maxUnique = Math.min(request.getUniqueValueLimit(), MAX_UNIQUE_LIMIT);
            } else {
                maxUnique = DEFAULT_UNIQUE_LIMIT;
            }
            if (type == Character.class || type == char.class) {
                statsFunc = new CharacterChunkedStats(MAX_UNIQUE_TO_COLLECT, maxUnique);
            } else {
                statsFunc = new ObjectChunkedStats(MAX_UNIQUE_TO_COLLECT, maxUnique);
            }
            columnSource = table.getColumnSource(columnName);
        }

        // Execute the function in a snapshot
        final Mutable<Table> resultHolder = new MutableObject<>();
        ConstructSnapshot.callDataSnapshotFunction("GenerateColumnStats()",
                ConstructSnapshot.makeSnapshotControl(false, table.isRefreshing(), (NotificationStepSource) table),
                (usePrev, beforeClockValue) -> {
                    final RowSet rowSet = usePrev ? table.getRowSet().prev() : table.getRowSet();
                    resultHolder.setValue(statsFunc.processChunks(rowSet, columnSource, usePrev));
                    return true;
                });
        return resultHolder.getValue();
    }
}
