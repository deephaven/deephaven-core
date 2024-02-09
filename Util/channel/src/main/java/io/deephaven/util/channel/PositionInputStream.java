/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.channel;

import com.google.common.io.CountingInputStream;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;

public final class PositionInputStream extends FilterInputStream {

    /**
     * Wraps a channel-backed input stream {@code in}, ensuring upon {@link #close()} that {@code channel's}
     * {@link SeekableByteChannel#position()} has been advanced the exact amount of bytes that have been consumed from
     * the <i>resulting</i> input stream. {@code in} is closed during {@link #close()}; as such, the caller must ensure
     * that closing {@code in} does _not_ close {@code channel}. To remain valid, the caller must ensure that the
     * resulting input stream isn't re-wrapped by any downstream code in a way that would adversely effect the position
     * (such as wrapping the resulting input stream with buffering).
     *
     * @param channel the channel
     * @param in the input stream based on the channel
     * @return a positional input stream
     * @throws IOException if an IO exception occurs
     */
    public static InputStream of(SeekableByteChannel channel, InputStream in) throws IOException {
        return new PositionInputStream(channel, in);
    }

    private final SeekableByteChannel ch;
    private final long position;

    private PositionInputStream(SeekableByteChannel ch, InputStream in) throws IOException {
        super(new CountingInputStream(in));
        this.ch = Objects.requireNonNull(ch);
        this.position = ch.position();
    }

    @Override
    public void close() throws IOException {
        super.close();
        ch.position(position + ((CountingInputStream) in).getCount());
    }
}
