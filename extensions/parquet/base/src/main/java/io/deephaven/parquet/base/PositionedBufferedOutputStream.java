package io.deephaven.parquet.base;

import org.jetbrains.annotations.NotNull;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;

public final class PositionedBufferedOutputStream extends BufferedOutputStream {

    private final SeekableByteChannel writeChannel;

    public PositionedBufferedOutputStream(@NotNull final SeekableByteChannel writeChannel, final int size) {
        super(Channels.newOutputStream(writeChannel), size);
        this.writeChannel = writeChannel;
    }

    /**
     * Get the total number of bytes written to this stream
     */
    long position() throws IOException {
        // Number of bytes buffered in the stream + bytes written to the underlying channel
        return this.count + writeChannel.position();
    }
}
