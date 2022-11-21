package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Count;
import io.deephaven.api.agg.FirstRowKey;
import io.deephaven.api.agg.LastRowKey;
import io.deephaven.api.agg.Partition;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationColumns;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationCount;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationPartition;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationRowKey;

class AggregationAdapter {
    public static void validate(io.deephaven.proto.backplane.grpc.Aggregation aggregation) {
        // It's a bit unfortunate that generated protobuf objects don't have the names as constants (like it does with
        // field numbers). For example, Aggregation.TYPE_NAME.
        GrpcErrorHelper.checkHasOneOf(aggregation, "type");
        switch (aggregation.getTypeCase()) {
            case COLUMNS:
                validate(aggregation.getColumns());
                break;
            case COUNT:
            case FIRST_ROW_KEY:
            case LAST_ROW_KEY:
            case PARTITION:
                // No "structural" verification here; will rely on parsing later
                break;
            case TYPE_NOT_SET:
                throw new IllegalStateException("checkHasOneOf should have caught TYPE_NOT_SET");
            default:
                throw GrpcUtil.statusRuntimeException(Code.INTERNAL,
                        String.format("Server is missing Aggregation case %s", aggregation.getTypeCase()));
        }
    }

    public static void validate(AggregationColumns columns) {
        GrpcErrorHelper.checkHasField(columns, AggregationColumns.SPEC_FIELD_NUMBER);
        GrpcErrorHelper.checkRepeatedFieldNonEmpty(columns, AggregationColumns.MATCH_PAIRS_FIELD_NUMBER);
        AggSpecAdapter.validate(columns.getSpec());
    }

    public static Aggregation adapt(io.deephaven.proto.backplane.grpc.Aggregation aggregation) {
        switch (aggregation.getTypeCase()) {
            case COLUMNS:
                return adapt(aggregation.getColumns());
            case COUNT:
                return adapt(aggregation.getCount());
            case FIRST_ROW_KEY:
                return adaptFirst(aggregation.getFirstRowKey());
            case LAST_ROW_KEY:
                return adaptLast(aggregation.getLastRowKey());
            case PARTITION:
                return adapt(aggregation.getPartition());
            case TYPE_NOT_SET:
                // Note: we don't expect this case to be hit - it should be noticed earlier, and can provide a better
                // error message via validate.
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Aggregation type not set");
            default:
                throw GrpcUtil.statusRuntimeException(Code.INTERNAL,
                        String.format("Server is missing Aggregation case %s", aggregation.getTypeCase()));
        }
    }

    public static Aggregation adapt(AggregationColumns aggregationColumns) {
        final AggSpec spec = AggSpecAdapter.adapt(aggregationColumns.getSpec());
        return Aggregation.of(spec, aggregationColumns.getMatchPairsList());
    }

    public static Count adapt(AggregationCount count) {
        return Aggregation.AggCount(count.getColumnName());
    }

    public static FirstRowKey adaptFirst(AggregationRowKey key) {
        return Aggregation.AggFirstRowKey(key.getColumnName());
    }

    public static LastRowKey adaptLast(AggregationRowKey key) {
        return Aggregation.AggLastRowKey(key.getColumnName());
    }

    public static Partition adapt(AggregationPartition partition) {
        return partition.hasIncludeGroupByColumns()
                ? Aggregation.AggPartition(partition.getColumnName(), partition.getIncludeGroupByColumns())
                : Aggregation.AggPartition(partition.getColumnName());
    }
}
