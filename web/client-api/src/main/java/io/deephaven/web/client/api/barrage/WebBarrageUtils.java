//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import elemental2.core.*;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.web.client.api.barrage.def.ColumnDefinition;
import io.deephaven.web.client.api.barrage.def.InitialTableDefinition;
import io.deephaven.web.client.api.barrage.def.TableAttributesDefinition;
import io.deephaven.web.shared.data.*;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.Schema;
import org.gwtproject.nio.TypedArrayHelper;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;

/**
 * Utility to read barrage record batches.
 */
public class WebBarrageUtils {
    public static final int FLATBUFFER_MAGIC = 0x6E687064;

    public static Uint8Array wrapMessage(FlatBufferBuilder innerBuilder, byte messageType) {
        FlatBufferBuilder outerBuilder = new FlatBufferBuilder(1024);
        int messageOffset = BarrageMessageWrapper.createMsgPayloadVector(outerBuilder, innerBuilder.dataBuffer());
        int offset =
                BarrageMessageWrapper.createBarrageMessageWrapper(outerBuilder, FLATBUFFER_MAGIC, messageType,
                        messageOffset);
        outerBuilder.finish(offset);
        ByteBuffer byteBuffer = outerBuilder.dataBuffer();
        return bbToUint8ArrayView(byteBuffer);
    }

    public static Uint8Array bbToUint8ArrayView(ByteBuffer byteBuffer) {
        ArrayBufferView view = TypedArrayHelper.unwrap(byteBuffer);
        return new Uint8Array(view.buffer, byteBuffer.position() + view.byteOffset, byteBuffer.remaining());
    }

    public static Uint8Array emptyMessage() {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);
        int offset = BarrageMessageWrapper.createBarrageMessageWrapper(builder, FLATBUFFER_MAGIC,
                BarrageMessageType.None, 0);
        builder.finish(offset);
        return bbToUint8ArrayView(builder.dataBuffer());
    }

    public static InitialTableDefinition readTableDefinition(Uint8Array flightSchemaMessage) {
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

    public static Schema readSchemaMessage(Uint8Array flightSchemaMessage) {
        // we conform to flight's schema representation of:
        // - IPC_CONTINUATION_TOKEN (4-byte int of -1)
        // - message size (4-byte int)
        // - a Message wrapping the schema
        ByteBuffer bb = TypedArrayHelper.wrap(flightSchemaMessage);
        bb.position(bb.position() + 8);
        Message headerMessage = Message.getRootAsMessage(bb);

        assert headerMessage.headerType() == MessageHeader.Schema;
        return (Schema) headerMessage.header(new Schema());
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
