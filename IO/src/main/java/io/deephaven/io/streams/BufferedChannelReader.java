/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.streams;

import io.deephaven.base.verify.Require;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class BufferedChannelReader {
    private final String filename;
    private final int blockSize;
    private final int readSize;
    private ByteBuffer bb;
    private ReadableByteChannel channel;
    private int read;
    private int limit;

    /**
     * Guarantees that each buffer from readNext() will have remaining() >= blockSize [that is, until it starts winding
     * down on the end of the file] When it needs to refresh, it will read <= readSize from the channel
     */
    public BufferedChannelReader(final String filename, int blockSize, int readSize) {
        Require.leq(blockSize, "blockSize", readSize, "readSize");
        this.filename = filename;
        this.blockSize = blockSize;
        this.readSize = readSize;
    }

    public ByteBuffer readNext() throws IOException {
        if (bb == null) {
            bb = ByteBuffer.allocate(readSize); // faster to use heap BB since we are going to be parsing out of it
            bb.flip();
            channel = new FileInputStream(filename).getChannel(); // Channels.newChannel(WFileUtil.openPossiblyCompressedFile(path));
        }

        bb.limit(limit);
        if (read != -1 && bb.remaining() < blockSize) {
            bb.compact();
            read = channel.read(bb);
            bb.flip();
        }
        limit = bb.limit();
        return bb;
    }

    public ByteBuffer getBb() {
        return bb;
    }

    public int getRead() {
        return read;
    }

    public void close() throws IOException {
        if (channel != null)
            channel.close();
    }
}
