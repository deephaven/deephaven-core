package io.deephaven.engine.v2.select.setinclusion;

import java.io.IOException;

import static io.deephaven.compilertools.ReplicatePrimitiveCode.charToAllButBoolean;

public class ReplicateSetInclusionKernel {
    public static void main(String[] args) throws IOException {
        charToAllButBoolean("DB/src/main/java/io/deephaven/engine/v2/select/setinclusion/CharSetInclusionKernel.java");
    }
}
