/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.protobuf.FieldPath;
import io.deephaven.protobuf.ProtobufDescriptorParserOptions;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.OptionalInt;
import java.util.function.Function;

/**
 * The kafka protobuf options. This will fetch the {@link com.google.protobuf.Descriptors.Descriptor protobuf
 * descriptor} for the {@link #schemaSubject()} from the schema registry using version {@link #schemaVersion()} and
 * create {@link com.google.protobuf.Message message} parsing functions according to
 * {@link io.deephaven.protobuf.ProtobufDescriptorParser#parse(Descriptor, ProtobufDescriptorParserOptions)}. These
 * functions will be adapted to handle schema changes.
 *
 * <p>
 * For purposes of reproducibility across restarts where schema changes may occur, it is advisable for callers to set a
 * specific {@link #schemaVersion()}. This will ensure the resulting {@link io.deephaven.engine.table.TableDefinition
 * table definition} will not change across restarts. This gives the caller an explicit opportunity to update any
 * downstream consumers when updating {@link #schemaVersion()} if necessary.
 *
 * @see Consume#protobufSpec(ProtobufConsumeOptions)
 * @see <a href=
 *      "https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-protobuf.html">kafka
 *      protobuf serdes</a>
 */
@Immutable
@BuildableStyle
public abstract class ProtobufConsumeOptions {

    /**
     * The builder.
     *
     * @return the builder
     */
    public static Builder builder() {
        return ImmutableProtobufConsumeOptions.builder();
    }

    /**
     * Joins the name paths with underscores. Equivalent to {@code String.join("_", path.namePath())}.
     *
     * @param path the path
     * @return the underscore joined path names
     */
    public static String joinNamePathWithUnderscore(FieldPath path) {
        return String.join("_", path.namePath());
    }

    /**
     * The schema subject to fetch from the schema registry.
     *
     * @return the schema subject
     */
    public abstract String schemaSubject();

    /**
     * The schema version to fetch from the schema registry. When not set, the latest schema will be fetched.
     *
     * @return the schema version, or none for latest
     */
    public abstract OptionalInt schemaVersion();

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
     * {@link #joinNamePathWithUnderscore(FieldPath)}.
     *
     * @return the function to create column names
     */
    @Default
    public Function<FieldPath, String> pathToColumnName() {
        return ProtobufConsumeOptions::joinNamePathWithUnderscore;
    }

    public interface Builder {

        Builder schemaSubject(String schemaSubject);

        Builder schemaVersion(int schemaVersion);

        Builder parserOptions(ProtobufDescriptorParserOptions options);

        Builder pathToColumnName(Function<FieldPath, String> pathToColumnName);

        ProtobufConsumeOptions build();
    }
}
