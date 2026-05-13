//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.web.client.api.BigDecimalWrapper;
import io.deephaven.web.client.api.BigIntegerWrapper;
import io.deephaven.web.client.api.DateWrapper;
import io.deephaven.web.client.api.barrage.def.ColumnDefinition;
import io.deephaven.web.client.api.barrage.def.InitialTableDefinition;
import io.deephaven.web.client.api.barrage.def.TableAttributesDefinition;
import io.deephaven.web.shared.data.*;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.Schema;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;

/**
 * Utility to read barrage record batches.
 */
public class WebBarrageUtils {
    public static final int FLATBUFFER_MAGIC = 0x6E687064;

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
        return new InitialTableDefinition()
                .setAttributes(attributes)
                .setColumns(cols);
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

    public static Class<?> stringToClass(String t) {
        return switch (t) {
            case "boolean", "java.lang.Boolean" -> boolean.class;
            case "char", "java.lang.Character" -> char.class;
            case "byte", "java.lang.Byte" -> byte.class;
            case "int", "java.lang.Integer" -> int.class;
            case "short", "java.lang.Short" -> short.class;
            case "long", "java.lang.Long" -> long.class;
            case "java.lang.Float", "float" -> float.class;
            case "java.lang.Double", "double" -> double.class;
            case "java.time.Instant" -> DateWrapper.class;
            case "java.math.BigInteger" -> BigIntegerWrapper.class;
            case "java.math.BigDecimal" -> BigDecimalWrapper.class;
            default -> Object.class;
        };
    }
}
