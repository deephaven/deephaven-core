package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateColumnSourceFillBenchmark {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode
                .charToAllButBoolean("engine/table/src/benchmark/java/io/deephaven/engine/table/impl/sources/CharHelper.java");
    }
}
