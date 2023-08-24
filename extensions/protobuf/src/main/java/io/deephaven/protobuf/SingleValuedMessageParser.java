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

public interface SingleValuedMessageParser {

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
    static List<SingleValuedMessageParser> builtin() {
        return Builtin.parsers();
    }

    /**
     * Equivalent to {@code ServiceLoader.load(SingleValuedMessageParser.class)}.
     *
     * @return the service-loader single-valued message parsers
     * @see ServiceLoader#load(Class)
     */
    static Iterable<SingleValuedMessageParser> serviceLoaders() {
        return ServiceLoader.load(SingleValuedMessageParser.class);
    }

    /**
     * The default single-valued message parsers. Is the concatenation of {@link #builtin()} and
     * {@link #serviceLoaders()}.
     * 
     * @return the default single-valued message parsers.
     *
     */
    static List<SingleValuedMessageParser> defaults() {
        final List<SingleValuedMessageParser> out = new ArrayList<>(builtin());
        for (SingleValuedMessageParser parser : serviceLoaders()) {
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
     *
     * @param descriptor the actual descriptor
     * @param options the parser options
     * @return the message parsing function
     */
    TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options);
}
