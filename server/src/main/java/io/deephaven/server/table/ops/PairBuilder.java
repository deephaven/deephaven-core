package io.deephaven.server.table.ops;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Pair;

class PairBuilder {
    public static Pair adapt(io.deephaven.proto.backplane.grpc.Pair pair) {
        final ColumnName output = ColumnName.of(pair.getOutputColumnName());
        return pair.getInputColumnName().isEmpty() ? output : Pair.of(ColumnName.of(pair.getInputColumnName()), output);
    }
}
