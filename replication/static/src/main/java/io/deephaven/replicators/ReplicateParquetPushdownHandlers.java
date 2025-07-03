//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicateParquetPushdownHandlers {
    private static final String TASK = "replicateParquetPushdownHandlers";

    public static void main(String[] args) throws IOException {
        charToAllButBoolean(TASK,
                "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/pushdown/CharPushdownHandler.java");
    }
}
