package io.deephaven.replicators;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateReverseKernel {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode
                .charToAll("DB/src/main/java/io/deephaven/engine/table/impl/utils/reverse/CharReverseKernel.java");
    }
}
