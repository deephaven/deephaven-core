//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.deephaven.kafka.protobuf.Protocol;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.Objects;

class ProtobufDeserializers {

    static <T extends Message> Deserializer<T> of(Protocol protocol, Parser<T> parser) {
        if (protocol == Protocol.serdes()) {
            return new Serdes<>(ParseFrom.of(parser));
        }
        if (protocol == Protocol.raw()) {
            return new Raw<>(ParseFrom.of(parser));
        }
        throw new IllegalStateException("Unexpected protocol: " + protocol);
    }

    static Deserializer<DynamicMessage> of(Protocol protocol, Descriptor descriptor) {
        if (protocol == Protocol.serdes()) {
            return new Serdes<>(ParseFrom.of(descriptor));
        }
        if (protocol == Protocol.raw()) {
            return new Raw<>(ParseFrom.of(descriptor));
        }
        throw new IllegalStateException("Unexpected protocol: " + protocol);
    }

    /**
     * A simple abstraction that allows us to easily adapt {@link Parser} and {@link Descriptor} based parsing logic
     * because it's not as simple to adapt {@link Descriptor} into a {@link Parser}.
     */
    private interface ParseFrom<T extends Message> {
        static <T extends Message> ParseFrom<T> of(Parser<T> parser) {
            return parser::parseFrom;
        }

        static ParseFrom<DynamicMessage> of(Descriptor descriptor) {
            return (data, offset, len) -> DynamicMessage.newBuilder(descriptor).mergeFrom(data, offset, len).build();
        }

        T parseFrom(byte[] data, int offset, int len) throws InvalidProtocolBufferException;
    }


    private static final class Raw<T extends Message> implements Deserializer<T> {
        private final ParseFrom<T> parser;

        Raw(ParseFrom<T> parser) {
            this.parser = Objects.requireNonNull(parser);
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            try {
                return parser.parseFrom(data, 0, data.length);
            } catch (InvalidProtocolBufferException e) {
                throw new SerializationException(e);
            }
        }
    }


    /**
     * An implementation of the kafka serdes protocol that uses a <b>fixed</b> protobuf Protobuf {@link Descriptor} or
     * {@link Parser}. This guarantees that all parsed messages have exactly the same
     * {@link Message#getDescriptorForType() Message Descriptor}. This is in contrast with
     * {@link KafkaProtobufDeserializer}, whose parsing logic is dynamic depending on the schema ID from {@link Message}
     * to {@link Message}. For {@link KafkaProtobufDeserializer}, only messages with the same schema ID will have the
     * same {@link Message#getDescriptorForType() Message Descriptor}.
     *
     * <p>
     * The benefit of guaranteeing a fixed {@link Descriptor} is that downstream consumer logic can be greatly
     * simplified.
     *
     * <p>
     * The potential drawback is that downstream consumers are no longer aware of schema changes.
     *
     * <p>
     * In the context of Deephaven, this means that we don't need to be responsible for handling schema-change logic and
     * can instead rely on the underlying compatibility guarantees that the Protobuf encoding format provides (and any
     * additional guarantees that the schema registry may enforce). Given that our underlying Deephaven table
     * definitions are immutable and we don't have a notion of "table definition changes", in the current kafka
     * architecture, we can't pass schema change information through a {@link io.deephaven.engine.table.Table} anyways.
     *
     * @param <T> the message type.
     */
    private static final class Serdes<T extends Message> implements Deserializer<T> {
        private static final byte MAGIC_BYTE = 0x0;
        private static final int MAGIC_SIZE = Byte.BYTES;
        private static final int SCHEMA_ID_SIZE = Integer.BYTES;
        private static final int MESSAGE_IX_MIN_SIZE = Byte.BYTES;
        private static final int MESSAGE_IX_OFFSET = MAGIC_SIZE + SCHEMA_ID_SIZE;
        private static final int SERDES_MIN_SIZE = MAGIC_SIZE + SCHEMA_ID_SIZE + MESSAGE_IX_MIN_SIZE;

        private final ParseFrom<T> parser;

        Serdes(ParseFrom<T> parser) {
            this.parser = Objects.requireNonNull(parser);
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            if (data.length < SERDES_MIN_SIZE) {
                throw new SerializationException(
                        String.format("Not serdes protocol, expected at least %d bytes", SERDES_MIN_SIZE));
            }
            if (data[0] != MAGIC_BYTE) {
                throw new SerializationException("Not serdes protocol, magic byte is incorrect");
            }
            // No need to parse schema id
            // No need to parse io.confluent.kafka.schemaregistry.protobuf.MessageIndexes
            final int protobufOffset = protobufOffset(data);
            try {
                return parser.parseFrom(data, protobufOffset, data.length - protobufOffset);
            } catch (InvalidProtocolBufferException e) {
                throw new SerializationException(e);
            }
        }

        private static int protobufOffset(byte[] data) {
            final ByteBuffer bb = ByteBuffer.wrap(data, MESSAGE_IX_OFFSET, data.length - MESSAGE_IX_OFFSET);
            // We need to go through the motions of parsing io.confluent.kafka.schemaregistry.protobuf.MessageIndexes,
            // it's variable-sized. See io.confluent.kafka.schemaregistry.protobuf.MessageIndexes.readFrom, impl driven
            // by org.apache.kafka.common.utils.ByteUtils.readVarint.
            final int numMessageIxs = ByteUtils.readVarint(bb);
            for (int i = 0; i < numMessageIxs; ++i) {
                ByteUtils.readVarint(bb);
            }
            return bb.position();
        }
    }
}
