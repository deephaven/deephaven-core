package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateCopyKernel {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAll(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/util/copy/CharCopyKernel.java");
    }
}
