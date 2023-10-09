/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.protobuf.FieldPath;
import io.deephaven.protobuf.ProtobufDescriptorParserOptions;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

/**
 * The kafka protobuf options. This will get the {@link com.google.protobuf.Descriptors.Descriptor protobuf descriptor}
 * according to the {@link #descriptorProvider()} and create {@link com.google.protobuf.Message message} parsing
 * functions according to
 * {@link io.deephaven.protobuf.ProtobufDescriptorParser#parse(Descriptor, ProtobufDescriptorParserOptions)}.
 *
 * @see Consume#protobufSpec(ProtobufConsumeOptions)
 * @see <a href=
 *      "https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-protobuf.html">kafka
 *      protobuf serdes</a>
 */
@Immutable
@BuildableStyle
public abstract class ProtobufConsumeOptions {

    @FunctionalInterface
    public interface FieldPathToColumnName {
        /**
         * Creates a unique column name from {@code fieldPath} and {@code indexOccurrence}. Implementations will need to
         * take notice when {@code indexOccurrence > 0}, as that means a column name for {@code fieldPath} has already
         * been generated {@code indexOccurrence} times.
         * 
         * @param fieldPath the field path
         * @param indexOccurrence the number of times a column name for fieldPath has already been generated
         * @return the column name
         */
        ColumnName columnName(FieldPath fieldPath, int indexOccurrence);
    }

    /**
     * The builder.
     *
     * @return the builder
     */
    public static Builder builder() {
        return ImmutableProtobufConsumeOptions.builder();
    }

    /**
     * Joins the name paths with underscores, appending {@code indexOccurrence + 1} if {@code indexOccurrence != 0}.
     *
     * @param path the path
     * @param indexOccurrence the number of times this field path has been used
     * @return the underscore joined path names
     */
    public static ColumnName joinNamePathWithUnderscore(FieldPath path, int indexOccurrence) {
        final String simple = String.join("_", path.namePath());
        return ColumnName.of(indexOccurrence == 0 ? simple : simple + "_" + (indexOccurrence + 1));
    }

    /**
     * The descriptor provider.
     *
     * @return the descriptor provider
     */
    public abstract DescriptorProvider descriptorProvider();

    /**
     * The protocol for decoding the payload. When {@link #descriptorProvider()} is a {@link DescriptorSchemaRegistry},
     * {@link Protocol#serdes()} will be used by default; when {@link #descriptorProvider()} is a
     * {@link DescriptorMessageClass}, {@link Protocol#raw()} will be used by default.
     *
     * @return the payload protocol
     */
    @Default
    public Protocol protocol() {
        final DescriptorProvider dp = descriptorProvider();
        if (dp instanceof DescriptorSchemaRegistry) {
            return Protocol.serdes();
        }
        if (dp instanceof DescriptorMessageClass) {
            return Protocol.raw();
        }
        throw new IllegalStateException(String.format("Unexpected %s class: %s",
                DescriptorProvider.class.getSimpleName(), dp.getClass().getName()));
    }

    /**
     * The descriptor parsing options. By default, is {@link ProtobufDescriptorParserOptions#defaults()}.
     *
     * @return the descriptor parsing options
     */
    @Default
    public ProtobufDescriptorParserOptions parserOptions() {
        return ProtobufDescriptorParserOptions.defaults();
    }

    /**
     * The function to turn field paths into column names. By default, is the function
     * {@link #joinNamePathWithUnderscore(FieldPath, int)}}.
     *
     * @return the function to create column names
     */
    @Default
    public FieldPathToColumnName pathToColumnName() {
        return ProtobufConsumeOptions::joinNamePathWithUnderscore;
    }

    public interface Builder {
        Builder protocol(Protocol protocol);

        Builder descriptorProvider(DescriptorProvider descriptorProvider);

        Builder parserOptions(ProtobufDescriptorParserOptions options);

        Builder pathToColumnName(FieldPathToColumnName pathToColumnName);

        ProtobufConsumeOptions build();
    }
}
