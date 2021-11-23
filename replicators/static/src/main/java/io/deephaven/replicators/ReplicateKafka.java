/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.replicators;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateKafka {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode
                .charToAllButBoolean("Kafka/src/main/java/io/deephaven/kafka/ingest/GenericRecordCharFieldCopier.java");
        ReplicatePrimitiveCode
                .charToAllButBoolean("Kafka/src/main/java/io/deephaven/kafka/ingest/JsonNodeCharFieldCopier.java");
    }
}
