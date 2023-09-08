/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.deephaven.functions.TypedFunction;

/**
 * A specialized / simplified version of {@link MessageParser} that produces a single unnamed function.
 */
public interface MessageParserSingle extends MessageParser {

    /**
     * The message parsing function.
     *
     * @param descriptor the actual descriptor
     * @param options the parser options
     * @param fieldPath the field path
     * @return the message parsing function
     */
    TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options,
            FieldPath fieldPath);

    @Override
    default ProtobufFunctions messageParsers(Descriptor descriptor, ProtobufDescriptorParserOptions options,
            FieldPath fieldPath) {
        return ProtobufFunctions.unnamed(messageParser(descriptor, options, fieldPath));
    }
}
