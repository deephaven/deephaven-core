/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka.ingest;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateKafka {
    public static void main(String [] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(GenericRecordCharFieldCopier.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(JsonNodeCharFieldCopier.class, ReplicatePrimitiveCode.MAIN_SRC);
    }
}
