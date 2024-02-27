//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateColumnSourceFillBenchmark {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean("replicateColumnSourceFillBenchmark",
                "engine/benchmark/src/benchmark/java/io/deephaven/benchmark/engine/sources/CharHelper.java");
    }
}
