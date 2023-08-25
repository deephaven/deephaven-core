/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.functions.BooleanFunction;
import io.deephaven.protobuf.FieldOptions.BytesBehavior;
import io.deephaven.protobuf.FieldOptions.MapBehavior;
import io.deephaven.protobuf.FieldOptions.WellKnownBehavior;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.function.Function;

/**
 * The {@link ProtobufDescriptorParser} options.
 *
 * @see ProtobufDescriptorParser#parse(Descriptor, ProtobufDescriptorParserOptions)
 */
@Immutable
@BuildableStyle
public abstract class ProtobufDescriptorParserOptions {
    private static final Function<FieldPath, FieldOptions> DEFAULT_FIELD_OPTIONS = fp -> FieldOptions.defaults();
    private static final ProtobufDescriptorParserOptions DEFAULTS = builder().build();

    public static Builder builder() {
        return ImmutableProtobufDescriptorParserOptions.builder();
    }

    /**
     * Creates a default options instance.
     *
     * @return the options
     */
    public static ProtobufDescriptorParserOptions defaults() {
        return DEFAULTS;
    }

    /**
     * Equivalent to {@code fieldPath -> FieldOptions.defaults()}.
     *
     * @return the field options
     */
    @Default
    public Function<FieldPath, FieldOptions> fieldOptions() {
        return DEFAULT_FIELD_OPTIONS;
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

    public interface Builder {
        Builder fieldOptions(Function<FieldPath, FieldOptions> fieldOptions);

        Builder parsers(List<SingleValuedMessageParser> parsers);

        ProtobufDescriptorParserOptions build();
    }
}
