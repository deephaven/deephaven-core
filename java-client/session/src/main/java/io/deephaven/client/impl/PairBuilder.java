package io.deephaven.client.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Pair;

class PairBuilder {

    public static io.deephaven.proto.backplane.grpc.Pair adapt(Pair pair) {
        if (pair instanceof ColumnName) {
            return adapt((ColumnName) pair);
        }
        return io.deephaven.proto.backplane.grpc.Pair.newBuilder()
                .setOutputColumnName(pair.output().name())
                .setInputColumnName(pair.input().name())
                .build();
    }

    private static io.deephaven.proto.backplane.grpc.Pair adapt(ColumnName pair) {
        return io.deephaven.proto.backplane.grpc.Pair.newBuilder()
                .setOutputColumnName(pair.name())
                .build();
    }
}
