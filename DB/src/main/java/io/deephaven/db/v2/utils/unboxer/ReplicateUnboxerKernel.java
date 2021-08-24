package io.deephaven.db.v2.utils.unboxer;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateUnboxerKernel {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(CharUnboxer.class,
            ReplicatePrimitiveCode.MAIN_SRC);
    }
}
