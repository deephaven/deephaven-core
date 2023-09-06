package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;

import java.util.Objects;

class MessageParserAdapter implements MessageParser {
    private final MessageParserSingle messageParserSingle;

    MessageParserAdapter(MessageParserSingle messageParserSingle) {
        this.messageParserSingle = Objects.requireNonNull(messageParserSingle);
    }

    @Override
    public Descriptor canonicalDescriptor() {
        return messageParserSingle.canonicalDescriptor();
    }

    @Override
    public ProtobufFunctions messageParsers(Descriptor descriptor, ProtobufDescriptorParserOptions options,
            FieldPath fieldPath) {
        return ProtobufFunctions.unnamed(messageParserSingle.messageParser(descriptor, options, fieldPath));
    }
}
