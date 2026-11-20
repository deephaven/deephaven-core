//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import static io.deephaven.replication.ReplicationUtils.redirectOutputToReplicated;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateCopyKernel {
    private static final String TASK = "replicateCopyKernel";

    public static void main(String[] args) throws IOException {
        redirectOutputToReplicated(TASK);

        ReplicatePrimitiveCode.charToAll(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/util/copy/CharCopyKernel.java");
    }
}
