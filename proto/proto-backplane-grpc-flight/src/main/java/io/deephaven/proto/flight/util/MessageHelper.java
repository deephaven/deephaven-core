package io.deephaven.proto.flight.util;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteStringAccess;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MetadataVersion;

import java.nio.ByteBuffer;

public class MessageHelper {

    // per flight specification: 0xFFFFFFFF value is the first 4 bytes of a valid IPC message
    private static final int IPC_CONTINUATION_TOKEN = -1;

    public static int wrapInMessage(final FlatBufferBuilder builder, final int headerOffset, final byte headerType) {
        return wrapInMessage(builder, headerOffset, headerType, 0);
    }

    public static int wrapInMessage(final FlatBufferBuilder builder, final int headerOffset, final byte headerType,
            final int bodyLength) {
        Message.startMessage(builder);
        Message.addHeaderType(builder, headerType);
        Message.addHeader(builder, headerOffset);
        Message.addVersion(builder, MetadataVersion.V5);
        Message.addBodyLength(builder, bodyLength);
        return Message.endMessage(builder);
    }

    public static byte[] toIpcBytes(FlatBufferBuilder builder) {
        final ByteBuffer msg = builder.dataBuffer();
        int padding = msg.remaining() % 8;
        if (padding != 0) {
            padding = 8 - padding;
        }
        // 4 * 2 is for two ints; IPC_CONTINUATION_TOKEN followed by size of schema payload
        final byte[] byteMsg = new byte[msg.remaining() + 4 * 2 + padding];
        intToBytes(IPC_CONTINUATION_TOKEN, byteMsg, 0);
        intToBytes(msg.remaining(), byteMsg, 4);
        msg.get(byteMsg, 8, msg.remaining());
        return byteMsg;
    }

    private static void intToBytes(int value, byte[] bytes, int offset) {
        bytes[offset + 3] = (byte) (value >>> 24);
        bytes[offset + 2] = (byte) (value >>> 16);
        bytes[offset + 1] = (byte) (value >>> 8);
        bytes[offset] = (byte) (value);
    }
}
