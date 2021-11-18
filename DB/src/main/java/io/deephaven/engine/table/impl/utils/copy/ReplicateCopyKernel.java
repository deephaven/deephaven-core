package io.deephaven.engine.table.impl.utils.copy;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateCopyKernel {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAll("DB/src/main/java/io/deephaven/engine/table/impl/utils/copy/CharCopyKernel.java");
    }
}
