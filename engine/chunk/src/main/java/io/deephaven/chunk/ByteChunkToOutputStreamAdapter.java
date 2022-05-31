package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;

import java.io.IOException;
import java.io.OutputStream;

public class ByteChunkToOutputStreamAdapter {
    public static void write(OutputStream stream, ByteChunk<? extends Any> chunk, int srcOffset, int length) throws IOException {
        stream.write(chunk.data, chunk.offset + srcOffset, length);
    }
}
