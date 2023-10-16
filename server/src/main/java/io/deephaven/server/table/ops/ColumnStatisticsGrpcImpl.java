package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ColumnStatisticsRequest;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.table.stats.ChunkedComparableStatsKernel;
import io.deephaven.server.table.stats.ChunkedNumericalStatsKernel;
import io.deephaven.server.table.stats.DateTimeChunkedStats;
import io.deephaven.server.table.stats.GenerateComparableStatsFunction;
import io.deephaven.server.table.stats.GenerateNumericalStatsFunction;
import io.deephaven.server.table.stats.ObjectChunkedStats;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.lang.reflect.Array;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_STRING_ARRAY;
import static io.deephaven.server.table.stats.GenerateComparableStatsFunction.MAX_UNIQUE_VALUES_TO_PRINT;

@Singleton
public class ColumnStatisticsGrpcImpl extends GrpcTableOperation<ColumnStatisticsRequest> {
    @Inject
    public ColumnStatisticsGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionComputeColumnStatistics, BatchTableRequest.Operation::getColumnStatistics,
                ColumnStatisticsRequest::getResultId, ColumnStatisticsRequest::getSourceId);
    }

    @Override
    public Table create(ColumnStatisticsRequest request, List<SessionState.ExportObject<Table>> sourceTables) {
        request.getMaxUniqueValues();// TODO

        Table table = sourceTables.get(0).get();
        String columnName = request.getColumnName();
        final Class<?> type = table.getDefinition().getColumn(columnName).getDataType();
        if (TypeUtils.isDateTime(type)) {
            final Mutable<Table> resultHolder = new MutableObject<>();

            final ColumnSource<?> columnSource;
            if (Instant.class.isAssignableFrom(type)) {
                columnSource = ReinterpretUtils.instantToLongSource(table.getColumnSource(columnName));
            } else if (ZonedDateTime.class.isAssignableFrom(type)) {
                columnSource = ReinterpretUtils.zonedDateTimeToLongSource(table.getColumnSource(columnName));
            } else {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Datetime columns must be Instant or ZonedDateTime");
            }
            ConstructSnapshot.callDataSnapshotFunction("GenerateDBDateTimeStats()",
                    ConstructSnapshot.makeSnapshotControl(false, table.isRefreshing(), (NotificationStepSource) table),
                    (usePrev, beforeClockValue) -> {
                        resultHolder.setValue(DateTimeChunkedStats.getStats(table, columnSource, usePrev));
                        return true;
                    });

            return resultHolder.getValue();
        } else if (TypeUtils.isNumeric(type)) {
            return new GenerateNumericalStatsFunction(columnName).call(table);
        } else {
            return new GenerateComparableStatsFunction(columnName).call(table);
        }
    }
}
