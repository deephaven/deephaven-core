//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import static io.deephaven.replication.ReplicationUtils.redirectOutputToReplicated;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateFillFromElementKernel {
    private static final String TASK = "replicateFillFromElementKernel";

    public static void main(String[] args) throws IOException {
        redirectOutputToReplicated(TASK);

        final String charClassPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/util/fill/CharFillFromElementKernel.java";
        ReplicatePrimitiveCode.charToAll(TASK, charClassPath);
        ReplicatePrimitiveCode.charToObject(TASK, charClassPath);
    }
}
