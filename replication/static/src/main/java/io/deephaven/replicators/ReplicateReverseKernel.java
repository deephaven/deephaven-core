//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import static io.deephaven.replication.ReplicationUtils.redirectOutputToReplicated;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateReverseKernel {
    private static final String TASK = "replicateReverseKernel";

    public static void main(String[] args) throws IOException {
        redirectOutputToReplicated(TASK);

        ReplicatePrimitiveCode
                .charToAll(TASK,
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/util/reverse/CharReverseKernel.java");
    }
}
