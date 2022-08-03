/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateRingChunkSources {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/ring/CharacterRingChunkSource.java");
    }
}
