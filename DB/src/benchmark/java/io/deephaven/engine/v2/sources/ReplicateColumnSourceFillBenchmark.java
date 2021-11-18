package io.deephaven.engine.table.impl.sources;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateColumnSourceFillBenchmark {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode
                .charToAllButBoolean("DB/src/benchmark/java/io/deephaven/engine/table/impl/sources/CharHelper.java");
    }
}
