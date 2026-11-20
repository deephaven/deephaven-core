//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import static io.deephaven.replication.ReplicationUtils.redirectOutputToReplicated;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateRingChunkSources {
    private static final String TASK = "replicateRingChunkSources";

    public static void main(String[] args) throws IOException {
        redirectOutputToReplicated(TASK);

        ReplicatePrimitiveCode.charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/ring/CharacterRingChunkSource.java");
    }
}
