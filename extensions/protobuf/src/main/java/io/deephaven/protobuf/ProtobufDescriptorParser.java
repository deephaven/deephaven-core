/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message;

public final class ProtobufDescriptorParser {

    /**
     * Creates {@link Message message} {@link ProtobufFunctions functions} than can parse messages according to the
     * given {@code descriptor} and {@code options}. The resulting message functions require that the passed-in
     * {@link Message#getDescriptorForType() messages' descriptor} be the same instance as {@code descriptor}.
     *
     * <p>
     * Parsing proceeds through each {@link Descriptor#getFields() descriptor field} that matches
     * {@link FieldOptions#include()}}. By default, this is {@code true} for all fields.
     *
     * <p>
     * For simple types, the fields are parsed as:
     *
     * <table>
     * <tr>
     * <th>{@link Type protobuf type}</th>
     * <th>function type (no presence)</th>
     * <th>function type (presence)</th>
     * <th>function type (repeated)</th>
     * </tr>
     * <tr>
     * <td>{@link Type#INT32 int32}</td>
     * <td>{@code int}</td>
     * <td>{@link Integer}</td>
     * <td>{@code int[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#UINT32 uint32} (1)</td>
     * <td>{@code int}</td>
     * <td>{@link Integer}</td>
     * <td>{@code int[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#SINT32 sint32}</td>
     * <td>{@code int}</td>
     * <td>{@link Integer}</td>
     * <td>{@code int[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#FIXED32 fixed32} (1)</td>
     * <td>{@code int}</td>
     * <td>{@link Integer}</td>
     * <td>{@code int[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#SFIXED32 sfixed32}</td>
     * <td>{@code int}</td>
     * <td>{@link Integer}</td>
     * <td>{@code int[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#INT64 int64}</td>
     * <td>{@code long}</td>
     * <td>{@link Long}</td>
     * <td>{@code long[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#UINT64 uint64} (1)</td>
     * <td>{@code long}</td>
     * <td>{@link Long}</td>
     * <td>{@code long[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#SINT64 sint64}</td>
     * <td>{@code long}</td>
     * <td>{@link Long}</td>
     * <td>{@code long[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#FIXED64 fixed64} (1)</td>
     * <td>{@code long}</td>
     * <td>{@link Long}</td>
     * <td>{@code long[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#SFIXED64 sfixed64}</td>
     * <td>{@code long}</td>
     * <td>{@link Long}</td>
     * <td>{@code long[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#FLOAT float}</td>
     * <td>{@code float}</td>
     * <td>{@link Float}</td>
     * <td>{@code float[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#DOUBLE double}</td>
     * <td>{@code double}</td>
     * <td>{@link Double}</td>
     * <td>{@code double[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#BOOL bool}</td>
     * <td>{@code boolean}</td>
     * <td>{@link Boolean}</td>
     * <td>{@code boolean[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#STRING string}</td>
     * <td>{@link String}</td>
     * <td>{@link String}</td>
     * <td>{@code String[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#BYTES bytes}</td>
     * <td>{@code byte[]}</td>
     * <td>{@code byte[]}</td>
     * <td>{@code byte[][]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#BYTES bytes} (2)</td>
     * <td>{@link com.google.protobuf.ByteString ByteString}</td>
     * <td>{@link com.google.protobuf.ByteString ByteString}</td>
     * <td>{@code ByteString[]}</td>
     * </tr>
     * <tr>
     * <td>{@link Type#ENUM enum}</td>
     * <td>{@link EnumValueDescriptor EnumValueDescriptor}</td>
     * <td>{@link EnumValueDescriptor EnumValueDescriptor}</td>
     * <td>{@code EnumValueDescriptor[]}</td>
     * </tr>
     * </table>
     *
     * ^1 Unsigned 32-bit and 64-bit integers are represented using their signed counterpart, with the top bit being
     * stored in the sign bit. This matches the Java protobuf behavior,
     * <a href="https://protobuf.dev/programming-guides/proto3/#scalar">scalar</a>. Users may use
     * {@link Integer#toUnsignedLong(int)} or {@link Long#toUnsignedString(long)} /
     * {@link java.math.BigInteger#BigInteger(String)} to adapt as appropriate.
     *
     * <p>
     * ^2 The default behavior for {@link Type#BYTES bytes} is {@code byte[]}. To parse as
     * {@link com.google.protobuf.ByteString ByteString} instead, configure {@link FieldOptions#bytes()} for the field.
     *
     * <p>
     * For {@link Type#MESSAGE message} (and {@link Type#GROUP group} if proto2) fields, the
     * {@link ProtobufDescriptorParserOptions#parsers()} are used for well-known message types (for example,
     * {@link com.google.protobuf.Timestamp} to {@link java.time.Instant}), otherwise parsing continues recursively with
     * the the {@link FieldDescriptor#getMessageType() field's message descriptor}. To skip parsing as a well-known
     * type, configure {@link FieldOptions#wellKnown()} for the field. If the field is repeated, the function return
     * type will be the array-type with component type equal to what the non-repeated field function return type would
     * be (for example, {@code repeated com.google.protobuf.Timestamp} will result in {@code java.time.Instant[]}).
     *
     * <p>
     * Protobuf maps are a special case, which result in a function type that returns a {@code Map<Object, Object>},
     * where the keys are the equivalent {@code KeyType} and the values are the equivalent {@code ValueType}. To parse
     * as a {@code repeated MapFieldEntry} instead of a {@code map}, configure {@link FieldOptions#map()} for the field.
     *
     * <p>
     * The {@link FieldPath} context is kept during traversal and is an important part of the returned message
     * functions. Callers will typically use the returned field path to assign appropriate names to the functions.
     *
     * @param descriptor the descriptor
     * @param options the options
     * @return the parsed protobuf functions
     * @see <a href="https://protobuf.dev/programming-guides/proto3/">protobuf programming guide</a>
     * @see <a href="https://protobuf.dev/programming-guides/field_presence/">protobuf field presence</a>
     * @see <a href="https://protobuf.dev/programming-guides/proto3/#maps">protobuf maps</a>
     */
    public static ProtobufFunctions parse(Descriptor descriptor, ProtobufDescriptorParserOptions options) {
        return new ProtobufDescriptorParserImpl(options).translate(descriptor);
    }
}
