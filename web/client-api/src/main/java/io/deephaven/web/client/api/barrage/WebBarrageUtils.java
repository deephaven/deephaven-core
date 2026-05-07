//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import elemental2.dom.DomGlobal;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.proto.backplane.grpc.DeephavenTableMetadata;
import io.deephaven.proto.backplane.grpc.InputTableColumnInfo;
import io.deephaven.web.client.api.ColumnRestriction;
import io.deephaven.web.client.api.barrage.def.ColumnDefinition;
import io.deephaven.web.client.api.barrage.def.InitialTableDefinition;
import io.deephaven.web.client.api.barrage.def.InputTableMetadata;
import io.deephaven.web.client.api.barrage.def.TableAttributesDefinition;
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionConverter;
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionUtils;
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionValidator;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.shared.data.*;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.Schema;
import com.google.protobuf.Any;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;

/**
 * Utility to read barrage record batches.
 */
public class WebBarrageUtils {
    public static final int FLATBUFFER_MAGIC = 0x6E687064;

    private static final Map<String, ColumnRestrictionConverter> restrictionConverters = new HashMap<>();
    private static final Map<String, ColumnRestrictionValidator> restrictionValidators = new HashMap<>();

    static {
        // Register built-in parsers and validators
        registerColumnRestrictionType("IntegerRangeRestriction",
                ColumnRestrictionUtils::convertIntegerRangeRestriction,
                ColumnRestrictionUtils::validateIntegerRange);
        registerColumnRestrictionType("DoubleRangeRestriction",
                ColumnRestrictionUtils::convertDoubleRangeRestriction,
                ColumnRestrictionUtils::validateDoubleRange);
        registerColumnRestrictionType("NotNullRestriction",
                ColumnRestrictionUtils::convertNotNullRestriction,
                ColumnRestrictionUtils::validateNotNull);
        registerColumnRestrictionType("NonEmptyRestriction",
                ColumnRestrictionUtils::convertNonEmptyRestriction,
                ColumnRestrictionUtils::validateNonEmpty);
        registerColumnRestrictionType("StringListRestriction",
                ColumnRestrictionUtils::convertStringListRestriction,
                ColumnRestrictionUtils::validateStringList);
    }

    /**
     * Register a parser and optional client-side validator for a column restriction type. Calling this from JavaScript
     * allows downstream consumers to extend the restriction system with their own types.
     *
     * <p>
     * The {@code parser} converts a raw protobuf {@code Any} message into a
     * {@link io.deephaven.web.client.api.ColumnRestriction}. The {@code validator} (if provided) is a function that
     * takes a proposed value and the restriction's data object and returns a human-readable error message if the value
     * is invalid, or {@code null} if it is valid.
     *
     * @param restrictionType The restriction type name (e.g., "IntegerRangeRestriction")
     * @param parser Converts protobuf bytes into a ColumnRestriction
     * @param validator Optional client-side validation function; may be {@code null}
     */
    public static void registerColumnRestrictionType(String restrictionType,
            ColumnRestrictionConverter parser,
            ColumnRestrictionValidator validator) {
        restrictionConverters.put(restrictionType, parser);
        if (validator != null) {
            restrictionValidators.put(restrictionType, validator);
        }
    }

    public static ByteBuffer wrapMessage(FlatBufferBuilder innerBuilder, byte messageType) {
        FlatBufferBuilder outerBuilder = new FlatBufferBuilder(1024);
        int messageOffset = BarrageMessageWrapper.createMsgPayloadVector(outerBuilder, innerBuilder.dataBuffer());
        int offset =
                BarrageMessageWrapper.createBarrageMessageWrapper(outerBuilder, FLATBUFFER_MAGIC, messageType,
                        messageOffset);
        outerBuilder.finish(offset);
        return outerBuilder.dataBuffer();
    }

    public static ByteBuffer emptyMessage() {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);
        int offset = BarrageMessageWrapper.createBarrageMessageWrapper(builder, FLATBUFFER_MAGIC,
                BarrageMessageType.None, 0);
        builder.finish(offset);
        return builder.dataBuffer();
    }

    public static InitialTableDefinition readTableDefinition(ByteBuffer flightSchemaMessage) {
        return readTableDefinition(readSchemaMessage(flightSchemaMessage));
    }

    public static InitialTableDefinition readTableDefinition(Schema schema) {
        ColumnDefinition[] cols = readColumnDefinitions(schema);

        TableAttributesDefinition attributes = new TableAttributesDefinition(
                keyValuePairs("deephaven:attribute.", schema.customMetadataLength(), schema::customMetadata),
                keyValuePairs("deephaven:attribute_type.", schema.customMetadataLength(), schema::customMetadata),
                keyValuePairs("deephaven:unsent.attribute.", schema.customMetadataLength(), schema::customMetadata)
                        .keySet());

        // Parse input table metadata if present
        InputTableMetadata inputTableMetadata = parseInputTableMetadata(schema, cols);

        return new InitialTableDefinition()
                .setAttributes(attributes)
                .setColumns(cols)
                .setInputTableMetadata(inputTableMetadata);
    }

    /**
     * Parses input table metadata from the schema's custom metadata and column definitions.
     *
     * @param schema the schema containing the custom metadata with the base64-encoded table metadata
     * @param cols the column definitions to match against the column info in the table metadata
     * @return an InputTableMetadata object containing the column restrictions, or null if no valid metadata is found
     */
    private static InputTableMetadata parseInputTableMetadata(Schema schema, ColumnDefinition[] cols) {
        // Extract the tableMetadata from schema custom metadata
        final Map<String, String> schemaMetadata =
                keyValuePairs("deephaven:", schema.customMetadataLength(), schema::customMetadata);

        final String tableMetadataBase64 = schemaMetadata.get("tableMetadata");
        if (tableMetadataBase64 == null || tableMetadataBase64.isEmpty()) {
            return null;
        }

        final InputTableMetadata metadata = new InputTableMetadata();
        try {
            // Decode the base64 string to bytes and parse the DeephavenTableMetadata
            final byte[] bytes = DomGlobal.atob(tableMetadataBase64).getBytes(StandardCharsets.ISO_8859_1);
            final ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
            buffer.put(bytes);
            buffer.flip();
            final DeephavenTableMetadata tableMetadata = DeephavenTableMetadata.parseFrom(buffer);

            if (!tableMetadata.hasInputTableMetadata()) {
                return null;
            }

            // Get the column info map
            final Map<String, InputTableColumnInfo> columnInfoMap =
                    tableMetadata.getInputTableMetadata().getColumnInfoMap();

            // Extract column restrictions from the column info map
            for (ColumnDefinition col : cols) {
                final String columnName = col.getName();
                final InputTableColumnInfo columnInfo = columnInfoMap.get(columnName);

                if (columnInfo == null) {
                    JsLog.warn("parseInputTableMetadata: No column info found for column " + columnName);
                    continue;
                }

                final List<Any> restrictionsList = columnInfo.getRestrictionsList();
                final InputTableMetadata.ColumnRestrictions colRestrictions =
                        new InputTableMetadata.ColumnRestrictions();

                for (Any restrictionAny : restrictionsList) {
                    // Get the restriction type and look up the converter
                    String restrictionType = ColumnRestrictionUtils.getRestrictionType(restrictionAny.getTypeUrl());
                    ColumnRestrictionConverter converter = restrictionConverters.get(restrictionType);
                    if (converter != null) {
                        ColumnRestriction restriction = converter.convert(restrictionAny);
                        ColumnRestrictionValidator validator = restrictionValidators.get(restrictionType);
                        if (validator != null) {
                            restriction.setValidator(validator);
                        }
                        colRestrictions.addRestriction(restriction);
                    } else {
                        JsLog.error("No converter registered for restriction type: " + restrictionType);
                    }
                }

                if (colRestrictions.getRestrictions().length > 0) {
                    metadata.addColumnRestrictions(columnName, colRestrictions);
                }
            }
        } catch (Exception e) {
            JsLog.error("Failed to parse input table metadata:", e);
            return null;
        }

        return metadata;
    }

    private static ColumnDefinition[] readColumnDefinitions(Schema schema) {
        ColumnDefinition[] cols = new ColumnDefinition[(int) schema.fieldsLength()];
        for (int i = 0; i < schema.fieldsLength(); i++) {
            cols[i] = new ColumnDefinition(i, schema.fields(i));
        }
        return cols;
    }

    /**
     * Reads the buffer into a Message and unwraps the Schema within. The buffer's contents are consumed. We expect this
     * payload to consist of
     * <ul>
     * <li>IPC_CONTINUATION_TOKEN (4-byte int of -1)</li>
     * <li>message size (4-byte int)</li>
     * <li>a Message wrapping the schema</li>
     * </ul>
     */
    public static Schema readSchemaMessage(ByteBuffer flightSchemaMessage) {
        flightSchemaMessage.order(ByteOrder.LITTLE_ENDIAN);
        int contToken = flightSchemaMessage.getInt();
        if (contToken != -1) {
            throw new IllegalStateException("Expected -1 for first four bytes of schema payload");
        }
        int size = flightSchemaMessage.getInt();
        if (size > flightSchemaMessage.remaining()) {
            throw new IllegalStateException("Schema message size " + size + " is larger than remaining buffer "
                    + flightSchemaMessage.remaining());
        }
        Message headerMessage = Message.getRootAsMessage(flightSchemaMessage);

        if (headerMessage.headerType() != MessageHeader.Schema) {
            throw new IllegalStateException(
                    "Expected a schema payload, got " + MessageHeader.name(headerMessage.headerType()));
        }
        Schema schema = new Schema();
        headerMessage.header(schema);
        return schema;
    }

    public static Map<String, String> keyValuePairs(String filterPrefix, double count,
            IntFunction<KeyValue> accessor) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < count; i++) {
            KeyValue pair = accessor.apply(i);
            String key = pair.key();
            if (key.startsWith(filterPrefix)) {
                key = key.substring(filterPrefix.length());
                String oldValue = map.put(key, pair.value());
                assert oldValue == null : key + " had " + oldValue + ", replaced with " + pair.value();
            }
        }
        return map;
    }

    public static ByteBuffer serializeRanges(Set<RangeSet> rangeSets) {
        final RangeSet s;
        if (rangeSets.isEmpty()) {
            return ByteBuffer.allocate(0);
        } else if (rangeSets.size() == 1) {
            s = rangeSets.iterator().next();
        } else {
            s = new RangeSet();
            for (RangeSet rangeSet : rangeSets) {
                s.addRangeSet(rangeSet);
            }
        }

        return CompressedRangeSetReader.writeRange(s);
    }
}
