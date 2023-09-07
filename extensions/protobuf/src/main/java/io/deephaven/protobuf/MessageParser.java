/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
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
     * <table>
     * <tr>
     * <th>{@link Message message} type</th>
     * <th>{@link TypedFunction function} type</th>
     * </tr>
     * <tr>
     * <td>{@link com.google.protobuf.Timestamp}</td>
     * <td>{@link java.time.Instant}</td>
     * </tr>
     * <tr>
     * <td>{@link com.google.protobuf.Duration}</td>
     * <td>{@link java.time.Duration}</td>
     * </tr>
     * <tr>
     * <td>{@link com.google.protobuf.BoolValue}</td>
     * <td>{@code boolean}</td>
     * </tr>
     * <tr>
     * <td>{@link com.google.protobuf.Int32Value}</td>
     * <td>{@code int}</td>
     * </tr>
     * <tr>
     * <td>{@link com.google.protobuf.UInt32Value}</td>
     * <td>{@code int}</td>
     * </tr>
     * <tr>
     * <td>{@link com.google.protobuf.Int64Value}</td>
     * <td>{@code long}</td>
     * </tr>
     * <tr>
     * <td>{@link com.google.protobuf.UInt64Value}</td>
     * <td>{@code long}</td>
     * </tr>
     * <tr>
     * <td>{@link com.google.protobuf.FloatValue}</td>
     * <td>{@code float}</td>
     * </tr>
     * <tr>
     * <td>{@link com.google.protobuf.DoubleValue}</td>
     * <td>{@code double}</td>
     * </tr>
     * <tr>
     * <td>{@link com.google.protobuf.StringValue}</td>
     * <td>{@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link com.google.protobuf.BytesValue}</td>
     * <td>{@code byte[]} (or {@link com.google.protobuf.ByteString}, see {@link FieldOptions#bytes()})</td>
     * </tr>
     * <tr>
     * <td>{@link com.google.protobuf.Any}</td>
     * <td>{@link com.google.protobuf.Any}</td>
     * </tr>
     * <tr>
     * <td>{@link com.google.protobuf.FieldMask}</td>
     * <td>{@link com.google.protobuf.FieldMask}</td>
     * </tr>
     * </table>
     *
     * @return the built-in parsers
     */
    static List<MessageParser> builtin() {
        return Builtin.parsers();
    }

    /**
     * Equivalent to {@code ServiceLoader.load(MessageParser.class)}.
     *
     * @return the service-loader message parsers
     * @see ServiceLoader#load(Class)
     */
    static Iterable<MessageParser> serviceLoaders() {
        return ServiceLoader.load(MessageParser.class);
    }

    /**
     * The default single-valued message parsers. Is the concatenation of {@link #builtin()} and
     * {@link #serviceLoaders()}.
     * 
     * @return the default message parsers
     */
    static List<MessageParser> defaults() {
        final List<MessageParser> out = new ArrayList<>(builtin());
        for (MessageParser parser : serviceLoaders()) {
            out.add(parser);
        }
        return out;
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
