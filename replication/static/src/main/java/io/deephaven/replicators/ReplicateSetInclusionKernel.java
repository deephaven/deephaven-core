//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBoolean;

public class ReplicateSetInclusionKernel {
    public static void main(String[] args) throws IOException {
        charToAllButBoolean("replicateSetInclusionKernel",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/select/setinclusion/CharSetInclusionKernel.java");
    }
}
