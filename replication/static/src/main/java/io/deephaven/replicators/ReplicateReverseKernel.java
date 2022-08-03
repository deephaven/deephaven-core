/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateReverseKernel {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode
                .charToAll(
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/util/reverse/CharReverseKernel.java");
    }
}
