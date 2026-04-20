//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import elemental2.core.*;
import elemental2.dom.DomGlobal;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.proto.backplane.grpc.DeephavenTableMetadata;
import io.deephaven.proto.backplane.grpc.InputTableColumnInfo;
import io.deephaven.web.client.api.barrage.def.ColumnDefinition;
import io.deephaven.web.client.api.barrage.def.InitialTableDefinition;
import io.deephaven.web.client.api.barrage.def.InputTableMetadata;
import io.deephaven.web.client.api.barrage.def.TableAttributesDefinition;
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionConverter;
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionUtils;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.shared.data.*;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.Schema;
import com.google.protobuf.Any;
import org.gwtproject.nio.TypedArrayHelper;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

    static {

        // Register default converters
        registerColumnRestrictionConverter("IntegerRangeRestriction", ColumnRestrictionUtils::convertIntegerRangeRestriction);
        registerColumnRestrictionConverter("DoubleRangeRestriction", ColumnRestrictionUtils::convertDoubleRangeRestriction);
        registerColumnRestrictionConverter("NotNullRestriction", ColumnRestrictionUtils::convertNotNullRestriction);
        registerColumnRestrictionConverter("NonEmptyRestriction", ColumnRestrictionUtils::convertNonEmptyRestriction);
        registerColumnRestrictionConverter("StringListRestriction", ColumnRestrictionUtils::convertStringListRestriction);
    }

    /**
     * Register a converter for a specific restriction type.
     *
     * @param restrictionType The type name of the restriction (e.g., "IntegerRangeRestriction")
     * @param converter The converter function to convert the restriction data
     */
    public static void registerColumnRestrictionConverter(String restrictionType, ColumnRestrictionConverter converter) {
        restrictionConverters.put(restrictionType, converter);
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

    private static InputTableMetadata parseInputTableMetadata(Schema schema, ColumnDefinition[] cols) {
        // Extract the tableMetadata from schema custom metadata
        Map<String, String> schemaMetadata =
                keyValuePairs("deephaven:", schema.customMetadataLength(), schema::customMetadata);

        String tableMetadataBase64 = schemaMetadata.get("tableMetadata");
        if (tableMetadataBase64 == null || tableMetadataBase64.isEmpty()) {
            return null;
        }

        InputTableMetadata metadata = new InputTableMetadata();

        try {
            JsLog.warn("parseInputTableMetadata: Decoding base64...");
            // Decode base64 to Uint8Array
            Uint8Array bytes = decodeBase64(tableMetadataBase64);
            JsLog.warn("parseInputTableMetadata: Decoded " + bytes.length + " bytes");

            JsLog.warn("parseInputTableMetadata: Deserializing DeephavenTableMetadata...");
            // Convert Uint8Array to ByteBuffer and deserialize using Java protobuf parseFrom
            ByteBuffer buffer = TypedArrayHelper.wrap(bytes);
            DeephavenTableMetadata tableMetadata = DeephavenTableMetadata.parseFrom(buffer);
            JsLog.warn("parseInputTableMetadata: Successfully deserialized DeephavenTableMetadata");

            if (!tableMetadata.hasInputTableMetadata()) {
                JsLog.warn("parseInputTableMetadata: No input table metadata present");
                return metadata;
            }

            io.deephaven.proto.backplane.grpc.InputTableMetadata protoInputTableMetadata =
                    tableMetadata.getInputTableMetadata();
            JsLog.warn("parseInputTableMetadata: Got InputTableMetadata");

            // Get the column info map
            Object columnInfoMapRaw = protoInputTableMetadata.getColumnInfoMap();

            if (columnInfoMapRaw == null) {
                JsLog.warn("parseInputTableMetadata: columnInfoMap is null");
                return metadata;
            }

            JsLog.warn("parseInputTableMetadata: Processing " + cols.length + " columns");
            // Extract column restrictions from the column info map
            for (ColumnDefinition col : cols) {
                String columnName = col.getName();
                InputTableColumnInfo columnInfo = getColumnInfoFromMap(columnInfoMapRaw, columnName);

                if (columnInfo != null) {
                    JsLog.warn("parseInputTableMetadata: Found column info for: " + columnName);
                    List<Any> restrictionsList = columnInfo.getRestrictionsList();

                    if (restrictionsList != null && !restrictionsList.isEmpty()) {
                        JsLog.warn("parseInputTableMetadata: Column " + columnName + " has " + restrictionsList.size() + " restrictions");
                        InputTableMetadata.ColumnRestrictions colRestrictions =
                                new InputTableMetadata.ColumnRestrictions();

                        for (int i = 0; i < restrictionsList.size(); i++) {
                            Object restrictionAny = restrictionsList.get(i);

                            // Get the restriction type and look up the converter
                            String restrictionType = ColumnRestrictionUtils.getRestrictionType(restrictionAny);
                            JsLog.warn("parseInputTableMetadata: Restriction " + i + " type: " + restrictionType);

                            if (restrictionType != null) {
                                ColumnRestrictionConverter converter = restrictionConverters.get(restrictionType);

                                if (converter != null) {
                                    JsLog.warn("parseInputTableMetadata: Converting restriction...");
                                    io.deephaven.web.client.api.ColumnRestriction restriction =
                                            converter.convert(jsinterop.base.Js.cast(restrictionAny));
                                    if (restriction != null) {
                                        colRestrictions.addRestriction(restriction);
                                        JsLog.warn("parseInputTableMetadata: Successfully added restriction");
                                    } else {
                                        JsLog.warn("parseInputTableMetadata: Converter returned null for " + restrictionType);
                                    }
                                } else {
                                    JsLog.warn("No converter registered for restriction type: " + restrictionType);
                                }
                            }
                        }

                        metadata.addColumnRestrictions(columnName, colRestrictions);
                    }
                }
            }
            JsLog.warn("parseInputTableMetadata: Successfully parsed all restrictions");
        } catch (Exception e) {
            JsLog.warn("Failed to parse input table metadata:", e);
        }

        return metadata;
    }

    // Helper method to extract InputTableColumnInfo from the map
    private static native InputTableColumnInfo getColumnInfoFromMap(Object map, String key) /*-{
        if (!map || !map.get) return null;
        return map.get(key);
    }-*/;

    // Decode base64 string to Uint8Array using elemental2
    private static Uint8Array decodeBase64(String base64) {
        // Use DomGlobal.atob() to decode base64 to binary string
        String binaryString = DomGlobal.atob(base64);

        // Convert binary string to Uint8Array
        Uint8Array bytes = new Uint8Array(binaryString.length());
        for (int i = 0; i < binaryString.length(); i++) {
            bytes.setAt(i, (double) (binaryString.charAt(i) & 0xff));
        }
        return bytes;
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
