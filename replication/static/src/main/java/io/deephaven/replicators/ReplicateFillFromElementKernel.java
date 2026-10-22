//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateFillFromElementKernel {
    public static void main(String[] args) throws IOException {
        final String charClassPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/util/fill/CharFillFromElementKernel.java";
        ReplicatePrimitiveCode.charToAll("replicateFillFromElementKernel", charClassPath);
        ReplicatePrimitiveCode.charToObject("replicateFillFromElementKernel", charClassPath);
    }
}
