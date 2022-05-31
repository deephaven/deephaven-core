package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateUnboxerKernel {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/util/unboxer/CharUnboxer.java");
    }
}
