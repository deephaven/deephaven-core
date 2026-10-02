//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.deephaven.kafka.testcontainers.KafkaService;
import io.deephaven.protobuf.test.FooBar;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class ConfluentProtobufCompatTest {

    /**
     * Regression test meant to better guard against Deephaven's runtime protobuf version falling behind the protoc
     * version that Confluent uses to generate their protobufs.
     *
     * <pre>{@code
     * com.google.protobuf.RuntimeVersion$ProtobufRuntimeVersionException: Detected incompatible Protobuf Gencode/Runtime versions when loading MetaProto: gencode 4.34.0, runtime 4.33.6. Runtime version cannot be older than the linked gencode version.
     * 	at app//com.google.protobuf.RuntimeVersion.validateProtobufGencodeVersionImpl(RuntimeVersion.java:153)
     * 	at app//com.google.protobuf.RuntimeVersion.validateProtobufGencodeVersion(RuntimeVersion.java:72)
     * 	at app//io.confluent.protobuf.MetaProto.<clinit>(MetaProto.java:12)
     * 	at app//io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.<clinit>(ProtobufSchema.java:305)
     * }</pre>
     *
     * @see KafkaToolsIntegrationTest#protobufSchemaRegistryTest(KafkaService, TestInfo) for a fuller integration test
     */
    @Test
    void canConstructConfluentProtobufSchemaWithDeephavenCompiledProto() {
        new ProtobufSchema(FooBar.getDescriptor());
    }
}
