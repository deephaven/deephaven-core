/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import io.deephaven.functions.TypedFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * The interface for creating {@link com.google.protobuf.Message message} {@link ProtobufFunctions protobuf functions}.
 */
public interface MessageParser {

    /**
     * The built-in message parsers.
     *
     * @return the built-in parsers
     */
    static List<MessageParser> builtin() {
        return List.of();
    }

    /**
     * Equivalent to {@code ServiceLoader.load(MessageParser.class)}.
     *
     * @return the service-loader single-valued message parsers
     * @see ServiceLoader#load(Class)
     */
    static Iterable<MessageParser> serviceLoaders() {
        return ServiceLoader.load(MessageParser.class);
    }

    /**
     * The default single-valued message parsers. Is the concatenation of {@link #builtin()}, the
     * {@link #adapt(MessageParserSingle) adapted} {@link MessageParserSingle#builtin()}, {@link #serviceLoaders()}, and
     * the {@link #adapt(MessageParserSingle) adapted} {@link MessageParserSingle#serviceLoaders()}.
     * 
     * @return the default message parsers
     */
    static List<MessageParser> defaults() {
        final List<MessageParser> out = new ArrayList<>(builtin());
        for (MessageParserSingle messageParserSingle : MessageParserSingle.builtin()) {
            out.add(adapt(messageParserSingle));
        }
        for (MessageParser parser : serviceLoaders()) {
            out.add(parser);
        }
        for (MessageParserSingle parser : MessageParserSingle.serviceLoaders()) {
            out.add(adapt(parser));
        }
        return out;
    }

    /**
     * Adapts the single message parser to a message parser by using {@link ProtobufFunctions#unnamed(TypedFunction)}
     * against the single returned {@link TypedFunction} from the
     * {@link MessageParserSingle#messageParser(Descriptor, ProtobufDescriptorParserOptions, FieldPath)} of
     * {@code parser}.
     *
     * @param parser the single message parser
     * @return the message parser
     */
    static MessageParser adapt(MessageParserSingle parser) {
        return new MessageParserAdapter(parser);
    }

    /**
     * The canonical descriptor for the message.
     *
     * @return the descriptor
     */
    Descriptor canonicalDescriptor();

    /**
     * The protobuf functions.
     *
     * @param descriptor the actual descriptor
     * @param options the parser options
     * @param fieldPath the field path
     * @return the protobuf functions
     */
    ProtobufFunctions messageParsers(Descriptor descriptor, ProtobufDescriptorParserOptions options,
            FieldPath fieldPath);
}
