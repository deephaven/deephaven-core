package io.deephaven.engine.v2.utils.copy;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateCopyKernel {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAll(CharCopyKernel.class, ReplicatePrimitiveCode.MAIN_SRC);
    }
}
