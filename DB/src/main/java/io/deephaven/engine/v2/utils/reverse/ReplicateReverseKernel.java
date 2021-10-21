package io.deephaven.engine.v2.utils.reverse;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateReverseKernel {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode
                .charToAll("DB/src/main/java/io/deephaven/engine/v2/utils/reverse/CharReverseKernel.java");
    }
}
