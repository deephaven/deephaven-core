//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.proto.flight.util.MessageHelper;
import io.deephaven.proto.flight.util.SchemaHelper;
import io.deephaven.util.annotations.FinalDefault;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;

/**
 * Interface for mapping between Deephaven and Apache Arrow types.
 * <p>
 * The default implementation is {@link DefaultBarrageTypeMapping}, but can be overridden or extended as needed to
 * customize Deephaven's Apache Arrow Flight integration.
 */
public interface BarrageTypeMapping {

    /**
     * The table definition and any table attribute metadata.
     */
    class BarrageTableDefinition {
        public final TableDefinition tableDefinition;
        public final Map<String, Object> attributes;
        public final boolean isFlat;

        public BarrageTableDefinition(
                @NotNull final TableDefinition tableDefinition,
                @NotNull final Map<String, Object> attributes,
                final boolean isFlat) {
            this.tableDefinition = tableDefinition;
            this.attributes = Collections.unmodifiableMap(attributes);
            this.isFlat = isFlat;
        }
    }

    /**
     * Map a Deephaven column type to its default Apache Arrow type.
     *
     * @param type The Deephaven column type
     * @param componentType The Deephaven column component type
     * @return The Apache Arrow type preferred for wire-format
     */
    ArrowType mapToArrowType(
            @NotNull Class<?> type,
            @NotNull Class<?> componentType);

    /**
     * Map an Apache Arrow type to its default Deephaven column type.
     *
     * @param arrowType The Apache Arrow wire-format type
     * @return The Deephaven column type
     */
    BarrageTypeInfo mapFromArrowType(
            @NotNull ArrowType arrowType);

    /**
     * Compute the Apache Arrow Schema from a BarrageTableDefinition.
     *
     * @param tableDefinition The table definition to convert
     * @return The Apache Arrow Schema
     */
    Schema schemaFrom(
            @NotNull BarrageTableDefinition tableDefinition);

    /**
     * Compute the BarrageTableDefinition from an Apache Arrow Schema using default mappings and/or column metadata to
     * determine the desired Deephaven column types.
     *
     * @param schema The Apache Arrow Schema to convert
     * @return The BarrageTableDefinition
     */
    BarrageTableDefinition tableDefinitionFrom(
            @NotNull Schema schema);

    /**
     * Compute the Apache Arrow Schema from a Deephaven Table.
     *
     * @param table The table to convert
     * @return The Apache Arrow Schema
     */
    @FinalDefault
    default Schema schemaFrom(
            @NotNull final Table table) {
        return schemaFrom(new BarrageTableDefinition(table.getDefinition(), table.getAttributes(), table.isFlat()));
    }

    /**
     * Compute the Apache Arrow wire-format Schema bytes from a BarrageTableDefinition.
     *
     * @param tableDefinition The table definition to convert
     * @return The Apache Arrow wire-format Schema bytes
     */
    @FinalDefault
    default ByteString schemaBytesFrom(
            @NotNull final BarrageTableDefinition tableDefinition) {
        // note that flight expects the Schema to be wrapped in a Message prefixed by a 4-byte identifier
        // (to detect end-of-stream in some cases) followed by the size of the flatbuffer message

        final FlatBufferBuilder builder = new FlatBufferBuilder();
        final int schemaOffset = schemaFrom(tableDefinition).getSchema(builder);
        builder.finish(MessageHelper.wrapInMessage(builder, schemaOffset,
                org.apache.arrow.flatbuf.MessageHeader.Schema));

        return ByteStringAccess.wrap(MessageHelper.toIpcBytes(builder));
    }

    /**
     * Compute the Apache Arrow wire-format Schema bytes from a Deephaven Table.
     *
     * @param table The table to convert
     * @return The Apache Arrow wire-format Schema bytes
     */
    @FinalDefault
    default ByteString schemaBytesFrom(
            @NotNull final Table table) {
        return schemaBytesFrom(
                new BarrageTableDefinition(table.getDefinition(), table.getAttributes(), table.isFlat()));
    }

    /**
     * Compute the BarrageTableDefinition from Apache Arrow wire-format Schema bytes.
     *
     * @param schema The Apache Arrow wire-format Schema bytes (wrapped in a Message)
     * @return The BarrageTableDefinition
     */
    @FinalDefault
    default BarrageTableDefinition tableDefinitionFromSchemaBytes(
            @NotNull final ByteString schema) {
        return tableDefinitionFrom(Schema.convertSchema(SchemaHelper.flatbufSchema(schema.asReadOnlyByteBuffer())));
    }
}
