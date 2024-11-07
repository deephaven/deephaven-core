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
import java.util.stream.Stream;

import static io.deephaven.base.FileUtils.convertToURI;

public interface SeekableChannelsProvider extends SafeCloseable {

    /**
     * Wraps {@link SeekableChannelsProvider#getInputStream(SeekableByteChannel, int)} to ensure the channel's position
     * is incremented the exact amount that has been consumed from the resulting input stream. To remain valid, the
     * caller must ensure that the resulting input stream isn't re-wrapped by any downstream code in a way that would
     * adversely affect the position (such as re-wrapping the resulting input stream with buffering).
     *
     * <p>
     * Equivalent to {@code ChannelPositionInputStream.of(ch, provider.getInputStream(ch, sizeHint))}.
     *
     * @param provider the provider
     * @param ch the seekable channel
     * @param sizeHint the number of bytes the caller expects to read from the input stream
     * @return the position-safe input stream
     * @throws IOException if an IO exception occurs
     * @see ChannelPositionInputStream#of(SeekableByteChannel, InputStream)
     */
    static InputStream channelPositionInputStream(SeekableChannelsProvider provider, SeekableByteChannel ch,
            int sizeHint) throws IOException {
        return ChannelPositionInputStream.of(ch, provider.getInputStream(ch, sizeHint));
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

    /**
     * Returns true if the given URI exists in the underlying storage.
     *
     * @param uri the URI to check
     * @return true if the URI exists
     */
    boolean exists(@NotNull URI uri);

    default SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull String uriStr)
            throws IOException {
        return getReadChannel(channelContext, convertToURI(uriStr, false));
    }

    SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull URI uri)
            throws IOException;

    /**
     * Creates an {@link InputStream} from the current position of {@code channel} from which the caller expects to read
     * {@code sizeHint} number of bytes. Closing the resulting input stream does <i>not</i> close the {@code channel}.
     * The {@link InputStream} will be buffered; either explicitly in the case where the implementation uses an
     * unbuffered {@link #getReadChannel(SeekableChannelContext, URI)}, or implicitly when the implementation uses a
     * buffered {@link #getReadChannel(SeekableChannelContext, URI)}. {@code channel} must have been created by
     * {@code this} provider. The caller can't assume the position of {@code channel} after consuming the
     * {@link InputStream}. For use-cases that require the channel's position to be incremented the exact amount the
     * {@link InputStream} has been consumed, use
     * {@link #channelPositionInputStream(SeekableChannelsProvider, SeekableByteChannel, int)}.
     *
     * @param channel the channel
     * @param sizeHint the number of bytes the caller expects to read from the input stream
     * @return the input stream
     * @throws IOException if an IO exception occurs
     */
    InputStream getInputStream(SeekableByteChannel channel, int sizeHint) throws IOException;

    /**
     * Creates a {@link CompletableOutputStream} to write to the given URI.
     *
     * @param uri the URI to write to
     * @param bufferSizeHint the number of bytes the caller expects to buffer before flushing
     * @return the output stream
     * @throws IOException if an IO exception occurs
     * @see CompletableOutputStream
     */
    CompletableOutputStream getOutputStream(@NotNull final URI uri, int bufferSizeHint) throws IOException;


    /**
     * Returns a stream of URIs, the elements of which are the entries in the directory. The listing is non-recursive.
     * The URIs supplied by the stream will not have any unnecessary slashes or path separators. Also, the URIs will be
     * file URIs (not ending with "/") irrespective of whether the URI corresponds to a file or a directory. The caller
     * should manage file vs. directory handling in the processor. The caller is also responsible for closing the
     * stream, preferably using a try-with-resources block.
     *
     * @param directory the URI of the directory to list
     * @return The {@link Stream} of {@link URI}s
     */
    Stream<URI> list(@NotNull URI directory) throws IOException;

    /**
     * Returns a stream of URIs, the elements of which are all the files in the file tree rooted at the given starting
     * directory. The URIs supplied by the stream will not have any unnecessary slashes or path separators. Also, the
     * URIs will be file URIs (not ending with "/") irrespective of whether the URI corresponds to a file or a
     * directory. The caller should manage file vs. directory handling in the processor. The caller is also responsible
     * for closing the stream, preferably using a try-with-resources block.
     *
     * @param directory the URI of the directory to walk
     * @return The {@link Stream} of {@link URI}s
     */
    Stream<URI> walk(@NotNull URI directory) throws IOException;
}
