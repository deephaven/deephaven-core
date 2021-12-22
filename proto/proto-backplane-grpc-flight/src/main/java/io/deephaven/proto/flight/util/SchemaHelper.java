package io.deephaven.proto.flight.util;

import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.ByteBuffer;

public class SchemaHelper {

    private static final int MESSAGE_OFFSET = 8;

    /**
     * Creates a schema from an export response.
     *
     * @param response the response
     * @return the schema
     */
    public static Schema schema(ExportedTableCreationResponse response) {
        return Schema.convertSchema(flatbufSchema(response));
    }

    /**
     * Creates a flatbuf schema from an export response.
     *
     * @param response the response
     * @return the flatbuf schema
     */
    public static org.apache.arrow.flatbuf.Schema flatbufSchema(ExportedTableCreationResponse response) {
        return flatbufSchema(response.getSchemaHeader().asReadOnlyByteBuffer());
    }

    /**
     * Creates a flatbuf Schema from raw bytes of a Message.
     * 
     * @param bb a bytebuffer that contains a schema in a message
     * @return a flatbuf schema
     */
    public static org.apache.arrow.flatbuf.Schema flatbufSchema(ByteBuffer bb) {
        if (bb.remaining() < MESSAGE_OFFSET) {
            throw new IllegalArgumentException("Not enough bytes for Message/Schema");
        }
        bb.position(bb.position() + MESSAGE_OFFSET);
        final Message message = Message.getRootAsMessage(bb);
        if (message.headerType() != MessageHeader.Schema) {
            throw new IllegalArgumentException("Expected header type Schema");
        }
        final org.apache.arrow.flatbuf.Schema schema = new org.apache.arrow.flatbuf.Schema();
        message.header(schema);
        return schema;
    }
}
