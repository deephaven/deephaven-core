//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;

public class ArrowIpcUtil {
    public static long serialize(OutputStream outputStream, Schema schema) throws IOException {
        // not buffered. no flushing needed. not closing write channel
        return MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);
    }

    public static Schema deserialize(InputStream in) throws IOException {
        // not buffered. not closing read channel
        return MessageSerializer.deserializeSchema(new ReadChannel(Channels.newChannel(in)));
    }

    public static Schema deserialize(byte[] buf, int offset, int length) throws IOException {
        return deserialize(new ByteArrayInputStream(buf, offset, length));
    }
}
