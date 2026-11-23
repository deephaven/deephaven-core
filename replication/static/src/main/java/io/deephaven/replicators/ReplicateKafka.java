//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;


import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateKafka {
    private static final String TASK = "replicateKafka";

    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(TASK,
                "extensions/kafka/src/main/java/io/deephaven/kafka/ingest/GenericRecordCharFieldCopier.java");
        ReplicatePrimitiveCode.charToAllButBoolean(TASK,
                "extensions/kafka/src/main/java/io/deephaven/kafka/ingest/JsonNodeCharFieldCopier.java");
    }
}
