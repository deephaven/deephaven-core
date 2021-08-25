/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.streams;

import java.nio.ByteBuffer;
import java.io.IOException;

public interface ByteBufferSink {
    /**
     * Dispose of the contents of the buffer b, probably by writing them to a channel, and return a
     * new buffer in which writing can continue. The returned buffer must have at least need bytes
     * of space remaining. The return value may be the same buffer, as long as it's remaining()
     * value has been increased to be >= need.
     * 
     * @param b the buffer whose contents need to be disposed of.
     * @return the buffer in which further output should be written.
     */
    ByteBuffer acceptBuffer(ByteBuffer b, int need) throws IOException;

    /**
     * Dispose of the contents of the final buffer in an output sequence, probably by writing them
     * to a channel. Note that the argument buffer may be empty. Then do whatever it takes to
     * release the resources of the sink, probably by closing a channel.
     */
    void close(ByteBuffer b) throws IOException;
}
