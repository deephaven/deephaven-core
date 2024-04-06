//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Pair;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.ColumnAggregation;
import io.deephaven.api.agg.Count;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.agg.spec.AggSpecCountDistinct;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.List;
import java.util.Map;

final class AggregateCallAdapterImpl {
    private static final Map<SqlAggFunction, AggregationFunction> map = Map.ofEntries(
            Map.entry(SqlStdOperatorTable.MIN, aggSpecFunction(AggSpec.min())),
            Map.entry(SqlStdOperatorTable.MAX, aggSpecFunction(AggSpec.max())),
            Map.entry(SqlStdOperatorTable.AVG, aggSpecFunction(AggSpec.avg())),
            Map.entry(SqlStdOperatorTable.SUM, aggSpecFunction(AggSpec.sum())),
            Map.entry(SqlStdOperatorTable.ANY_VALUE, aggSpecFunction(AggSpec.first())),
            Map.entry(SqlStdOperatorTable.FIRST_VALUE, aggSpecFunction(AggSpec.first())),
            Map.entry(SqlStdOperatorTable.LAST_VALUE, aggSpecFunction(AggSpec.last())),
            Map.entry(SqlStdOperatorTable.STDDEV, aggSpecFunction(AggSpec.std())),
            Map.entry(SqlStdOperatorTable.VARIANCE, aggSpecFunction(AggSpec.var())),
            Map.entry(SqlStdOperatorTable.COUNT, AggregateCallAdapterImpl::count),
            Map.entry(SqlStdOperatorTable.APPROX_COUNT_DISTINCT, AggregateCallAdapterImpl::count));

    public static Aggregation aggregation(AggregateCall aggregateCall, ColumnName out, List<ColumnName> ins) {
        final AggregationFunction function = map.get(aggregateCall.getAggregation());
        if (function == null) {
            throw new UnsupportedOperationException("Unsupported aggregation " + aggregateCall.getAggregation());
        }
        return function.aggregation(aggregateCall, out, ins);
    }

    private static AggregationFunction aggSpecFunction(AggSpec aggSpec) {
        return (call, out, ins) -> aggSpec(aggSpec, call, out, ins);
    }

    private static Aggregation aggSpec(AggSpec aggSpec, AggregateCall call, ColumnName out, List<ColumnName> ins) {
        if (call.isDistinct()) {
            throw new UnsupportedOperationException("Deephaven does not support distinct aggregations");
        }
        if (ins.size() != 1) {
            throw new IllegalArgumentException();
        }
        return aggSpec.aggregation(Pair.of(ins.get(0), out));
    }


    private static Aggregation count(AggregateCall call, ColumnName out, List<ColumnName> ins) {
        if (call.isDistinct()) {
            if (ins.size() != 1) {
                throw new IllegalArgumentException();
            }
            return ColumnAggregation.of(AggSpecCountDistinct.of(!call.ignoreNulls()), Pair.of(ins.get(0), out));
        }
        if (call.ignoreNulls() || !ins.isEmpty()) {
            // SQLTODO(emulate-count-non-null)
            // There's potential that we could emulate non-null count by using a formula and summing.
            // Alternatively, see https://github.com/deephaven/deephaven-core/issues/3515.
            throw new UnsupportedOperationException("Deephaven count() does not support ignoring nulls");
        }
        return Count.of(out);
    }

    private interface AggregationFunction {
        Aggregation aggregation(AggregateCall call, ColumnName out, List<ColumnName> ins);
    }
}
