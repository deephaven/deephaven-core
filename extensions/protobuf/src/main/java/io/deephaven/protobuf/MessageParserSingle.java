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
 * A simplified interface for {@link MessageParser}. In cases where a {@link Message} is parsed into a single value,
 * this interface provides stronger typing and is easier to implement.
 *
 * @see MessageParser#adapt(MessageParserSingle)
 */
public interface MessageParserSingle {

    /**
     * The built-in single-valued message parsers.
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
     * <td>{@code byte[]}</td>
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
    static List<MessageParserSingle> builtin() {
        return Builtin.parsers();
    }

    /**
     * Equivalent to {@code ServiceLoader.load(SingleValuedMessageParser.class)}.
     *
     * @return the service-loader single-valued message parsers
     * @see ServiceLoader#load(Class)
     */
    static Iterable<MessageParserSingle> serviceLoaders() {
        return ServiceLoader.load(MessageParserSingle.class);
    }

    /**
     * The canonical descriptor for the message.
     *
     * @return the descriptor
     */
    Descriptor canonicalDescriptor();

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
}
