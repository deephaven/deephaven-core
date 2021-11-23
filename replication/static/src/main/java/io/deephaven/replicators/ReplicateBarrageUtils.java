/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateBarrageUtils {
    public static void main(final String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(
                "extensions/barrage/src/main/java/io/deephaven/extensions/barrage/chunk/CharChunkInputStreamGenerator.java");
        ReplicatePrimitiveCode.charToAll(
                "extensions/barrage/src/main/java/io/deephaven/extensions/barrage/chunk/array/CharArrayExpansionKernel.java");
    }
}
