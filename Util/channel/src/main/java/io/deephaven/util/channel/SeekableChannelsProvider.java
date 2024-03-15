//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.deephaven.base.FileUtils.convertToURI;

public interface SeekableChannelsProvider extends SafeCloseable {

    /**
     * Wraps {@link SeekableChannelsProvider#getInputStream(SeekableByteChannel)} to ensure the channel's position is
     * incremented the exact amount that has been consumed from the resulting input stream. To remain valid, the caller
     * must ensure that the resulting input stream isn't re-wrapped by any downstream code in a way that would adversely
     * affect the position (such as re-wrapping the resulting input stream with buffering).
     *
     * <p>
     * Equivalent to {@code ChannelPositionInputStream.of(ch, provider.getInputStream(ch))}.
     *
     * @param provider the provider
     * @param ch the seekable channel
     * @return the position-safe input stream
     * @throws IOException if an IO exception occurs
     * @see ChannelPositionInputStream#of(SeekableByteChannel, InputStream)
     */
    static InputStream channelPositionInputStream(SeekableChannelsProvider provider, SeekableByteChannel ch)
            throws IOException {
        return ChannelPositionInputStream.of(ch, provider.getInputStream(ch));
    }

    /**
     * Create a new {@link SeekableChannelContext} object for creating read channels via this provider.
     */
    SeekableChannelContext makeContext();

    /**
     * Create a new "single-use" {@link SeekableChannelContext} object for creating read channels via this provider.
     * This is meant for contexts that have a short lifecycle and expect to read a small amount from a read channel.
     */
    default SeekableChannelContext makeSingleUseContext() {
        return makeContext();
    }

    /**
     * Check if the given context is compatible with this provider. Useful to test if we can use provided
     * {@code context} object for creating channels with this provider.
     */
    boolean isCompatibleWith(@NotNull SeekableChannelContext channelContext);

    default SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull String uriStr)
            throws IOException {
        return getReadChannel(channelContext, convertToURI(uriStr, false));
    }

    SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull URI uri)
            throws IOException;

    /**
     * Creates an {@link InputStream} from the current position of {@code channel}; closing the resulting input stream
     * does <i>not</i> close the {@code channel}. The {@link InputStream} will be buffered; either explicitly in the
     * case where the implementation uses an unbuffered {@link #getReadChannel(SeekableChannelContext, URI)}, or
     * implicitly when the implementation uses a buffered {@link #getReadChannel(SeekableChannelContext, URI)}.
     * {@code channel} must have been created by {@code this} provider. The caller can't assume the position of
     * {@code channel} after consuming the {@link InputStream}. For use-cases that require the channel's position to be
     * incremented the exact amount the {@link InputStream} has been consumed, use
     * {@link #channelPositionInputStream(SeekableChannelsProvider, SeekableByteChannel)}.
     *
     * @param channel the channel
     * @return the input stream
     * @throws IOException if an IO exception occurs
     */
    InputStream getInputStream(SeekableByteChannel channel) throws IOException;

    default SeekableByteChannel getWriteChannel(@NotNull final String path, final boolean append) throws IOException {
        return getWriteChannel(Paths.get(path), append);
    }

    SeekableByteChannel getWriteChannel(@NotNull Path path, boolean append) throws IOException;
}
