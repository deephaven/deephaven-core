package io.deephaven.db.v2.select.setinclusion;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateSetInclusionKernel {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(CharSetInclusionKernel.class,
            ReplicatePrimitiveCode.MAIN_SRC);
    }
}
