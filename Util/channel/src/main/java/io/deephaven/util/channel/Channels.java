//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import java.io.InputStream;
import java.nio.channels.ReadableByteChannel;

public final class Channels {

    /**
     * Constructs a stream that reads bytes from the given {@code channel}. Closing the resulting input stream does
     * <b>not</b> close the {@code channel}.
     *
     * @param channel the channel from which bytes will be read
     * @return the new input stream
     */
    public static InputStream newInputStreamNoClose(ReadableByteChannel channel) {
        return java.nio.channels.Channels.newInputStream(ReadableByteChannelNoClose.of(channel));
    }
}
