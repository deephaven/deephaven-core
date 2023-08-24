/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.functions.BooleanFunction;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * The {@link ProtobufDescriptorParser} options.
 *
 * @see ProtobufDescriptorParser#parse(Descriptor, ProtobufDescriptorParserOptions)
 */
@Immutable
@BuildableStyle
public abstract class ProtobufDescriptorParserOptions {

    public static Builder builder() {
        return ImmutableProtobufDescriptorParserOptions.builder();
    }

    /**
     * Creates a default options instance.
     *
     * @return the options
     */
    public static ProtobufDescriptorParserOptions defaults() {
        return builder().build();
    }

    /**
     * Controls which fields paths are included. When {@code false} for a field, that field (and subfields) will not be
     * parsed. By default, is {@code true} for all fields.
     *
     * @return the fields to include function
     */
    @Default
    public BooleanFunction<FieldPath> include() {
        return BooleanFunction.ofTrue();
    }

    /**
     * Controls which single-valued message parsers to use. By default, is {@link SingleValuedMessageParser#defaults()}.
     * 
     * @return the single-valued message parsers
     */
    @Default
    public List<SingleValuedMessageParser> parsers() {
        return SingleValuedMessageParser.defaults();
    }

    /**
     * Controls the behavior for matched well-known types. When {@code true} for a field, the well-known type will be
     * used, otherwise that field will be parsed recursively. By default, is {@code true} for all fields.
     *
     * @return the well-known field behavior
     */
    @Default
    public BooleanFunction<FieldPath> parseAsWellKnown() {
        return BooleanFunction.ofTrue();
    }

    /**
     * Controls the behavior for {@link Type#BYTES bytes} fields. When {@code true} for a field, that field will be
     * parsed as {@code byte[]} ({@code byte[][]} for repeated), otherwise that field will be parsed as
     * {@link com.google.protobuf.ByteString ByteString} ({@code ByteString[]} for repeated). By default, is
     * {@code true} for all fields.
     * 
     * @return the {@link Type#BYTES bytes} behavior
     */
    @Default
    public BooleanFunction<FieldPath> parseAsBytes() {
        return BooleanFunction.ofTrue();
    }

    /**
     * Controls the behavior for {@code map} fields. When {@code true} for a field, that field will be parsed as
     * {@code Map<KeyType, ValueType>}, otherwise that field will be parsed as if it were {@code repeated MapFieldEntry}
     * field. By default, is {@code true} for all fields.
     *
     * @return the {@code map} behavior
     */
    @Default
    public BooleanFunction<FieldPath> parseAsMap() {
        return BooleanFunction.ofTrue();
    }

    public interface Builder {
        Builder include(BooleanFunction<FieldPath> includeFunction);

        Builder parsers(List<SingleValuedMessageParser> parsers);

        Builder parseAsWellKnown(BooleanFunction<FieldPath> parseAsWellKnownFunction);

        Builder parseAsBytes(BooleanFunction<FieldPath> parseAsBytesFunction);

        Builder parseAsMap(BooleanFunction<FieldPath> parseAsMapFunction);

        ProtobufDescriptorParserOptions build();
    }
}
