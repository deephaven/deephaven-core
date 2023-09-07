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

import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;

/**
 * The kafka protobuf options. This will fetch the {@link com.google.protobuf.Descriptors.Descriptor protobuf
 * descriptor} for the {@link #schemaSubject()} from the schema registry using version {@link #schemaVersion()} and
 * create {@link com.google.protobuf.Message message} parsing functions according to
 * {@link io.deephaven.protobuf.ProtobufDescriptorParser#parse(Descriptor, ProtobufDescriptorParserOptions)}. These
 * functions will be adapted to handle schema changes.
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
     * The schema subject to fetch from the schema registry.
     *
     * @return the schema subject
     */
    public abstract String schemaSubject();

    /**
     * The schema version to fetch from the schema registry. When not set, the latest schema will be fetched.
     *
     * <p>
     * For purposes of reproducibility across restarts where schema changes may occur, it is advisable for callers to
     * set this. This will ensure the resulting {@link io.deephaven.engine.table.TableDefinition table definition} will
     * not change across restarts. This gives the caller an explicit opportunity to update any downstream consumers
     * before bumping schema versions.
     *
     * @return the schema version, or none for latest
     */
    public abstract OptionalInt schemaVersion();

    /**
     * The fully-qualified protobuf {@link com.google.protobuf.Message} name, for example "com.example.MyMessage". This
     * message's {@link Descriptor} will be used as the basis for the resulting table's
     * {@link io.deephaven.engine.table.TableDefinition definition}. When not set, the first message descriptor in the
     * protobuf schema will be used.
     *
     * <p>
     * It is advisable for callers to explicitly set this.
     *
     * @return the schema message name
     */
    public abstract Optional<String> schemaMessageName();

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

        Builder schemaSubject(String schemaSubject);

        Builder schemaVersion(int schemaVersion);

        Builder schemaMessageName(String schemaMessageName);

        Builder parserOptions(ProtobufDescriptorParserOptions options);

        Builder pathToColumnName(FieldPathToColumnName pathToColumnName);

        ProtobufConsumeOptions build();
    }
}
