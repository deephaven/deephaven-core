package io.deephaven.db.v2.sources;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateColumnSourceFillBenchmark {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(CharHelper.class, ReplicatePrimitiveCode.BENCHMARK_SRC);
    }
}
