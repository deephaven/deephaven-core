//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import io.deephaven.annotations.BuildableStyle;
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
     * The field options, allows the caller to specify different options for different field paths. Equivalent to
     * {@code fieldPath -> FieldOptions.defaults()}.
     *
     * @return the field options
     */
    @Default
    public Function<FieldPath, FieldOptions> fieldOptions() {
        return DEFAULT_FIELD_OPTIONS;
    }

    /**
     * Controls which message parsers to use. By default, is {@link MessageParser#defaults()}.
     * 
     * @return the single-valued message parsers
     */
    @Default
    public List<MessageParser> parsers() {
        return MessageParser.defaults();
    }

    public interface Builder {
        Builder fieldOptions(Function<FieldPath, FieldOptions> fieldOptions);

        Builder parsers(List<MessageParser> parsers);

        ProtobufDescriptorParserOptions build();
    }
}
