package io.deephaven.engine.v2.sources;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateColumnSourceFillBenchmark {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean("DB/src/benchmark/java/io/deephaven/engine/v2/sources/CharHelper.java");
    }
}
