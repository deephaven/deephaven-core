package io.deephaven.client.impl;

import com.google.auto.service.AutoService;
import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.proto.flight.util.MessageHelper;
import io.deephaven.qst.table.TableHeader;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.vector.types.pojo.Schema;


@AutoService(SchemaBytes.class)
public final class SchemaBytesImpl implements SchemaBytes {

    @Override
    public byte[] toSchemaBytes(TableHeader header) {
        final Schema schema = SchemaAdapter.of(header);
        final FlatBufferBuilder builder = new FlatBufferBuilder();
        final int schemaOffset = schema.getSchema(builder);
        final int messageOffset = MessageHelper.wrapInMessage(builder, schemaOffset, MessageHeader.Schema);
        builder.finish(messageOffset);
        return MessageHelper.toIpcBytes(builder);
    }
}
