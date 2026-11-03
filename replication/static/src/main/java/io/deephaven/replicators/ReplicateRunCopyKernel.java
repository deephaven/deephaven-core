//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBoolean;
import static io.deephaven.replication.ReplicatePrimitiveCode.charToObject;

/**
 * Code generation for the Parquet page store's typed run copies.
 */
public class ReplicateRunCopyKernel {
    private static final String TASK = "replicateRunCopyKernel";

    private static final String CHAR_RUN_COPY_KERNEL_PATH =
            "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/pagestore/CharRunCopyKernel.java";

    public static void main(String... args) throws IOException {
        charToAllButBoolean(TASK, CHAR_RUN_COPY_KERNEL_PATH);
        ReplicationUtils.fixupChunkAttributes(charToObject(TASK, CHAR_RUN_COPY_KERNEL_PATH));
    }
}
