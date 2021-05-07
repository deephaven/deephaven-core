package io.deephaven.db.v2.utils.reverse;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateReverseKernel {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAll(CharReverseKernel.class, ReplicatePrimitiveCode.MAIN_SRC);
    }
}
