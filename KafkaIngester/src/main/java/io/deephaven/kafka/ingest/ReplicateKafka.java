package io.deephaven.kafka.ingest;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateKafka {
    public static void main(String [] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(GenericRecordCharFieldCopier.class, ReplicatePrimitiveCode.MAIN_SRC);
    }
}
