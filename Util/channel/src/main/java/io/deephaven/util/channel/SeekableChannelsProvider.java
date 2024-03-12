//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

public interface SeekableChannelsProvider extends SafeCloseable {

    /**
     * Take the file source path or URI string and convert it to a URI object.
     *
     * @param source The file source path or URI
     * @return The URI object
     */
    static URI convertToURI(final String source) {
        if (source.isEmpty()) {
            throw new IllegalArgumentException("Cannot convert empty source to URI");
        }
        final URI uri;
        try {
            uri = new URI(source);
        } catch (final URISyntaxException e) {
            // If the URI is invalid, assume it's a file path
            return convertFileToURI(new File(source));
        }
        if (uri.getScheme() == null) {
            // Convert to a "file" URI
            return convertFileToURI(new File(source));
        }
        return uri;
    }

    /**
     * Takes a file and convert it to a URI object. We should use this method instead of {@link File#toURI()} because
     * {@link File#toURI()} internally calls {@link File#isDirectory()}, which can lead to disk access.
     *
     * @param file The file
     * @return The URI object
     */
    static URI convertFileToURI(final File file) {
        String absPath = file.getAbsolutePath();
        if (absPath.isEmpty()) {
            throw new IllegalArgumentException("Cannot convert file with empty path to URI: " + file);
        }
        if (File.separatorChar != '/') {
            absPath = absPath.replace(File.separatorChar, '/');
        }
        if (absPath.charAt(0) != '/') {
            absPath = "/" + absPath;
        }
        if (absPath.startsWith("//")) {
            absPath = "//" + absPath;
        }
        try {
            return new URI("file", null, absPath, null);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Failed to convert file to URI: " + file, e);
        }
    }

    /**
     * Takes a path and convert it to a URI object. We should use this method instead of {@link Path#toUri()} because
     * {@link Path#toUri()} internally does a {@code stat} system call to extract information whether file is a
     * directory.
     *
     * @param path The path
     * @return The URI object
     */
    static URI convertPathToURI(final Path path) {
        return convertFileToURI(path.toFile());
    }

    /**
     * Wraps {@link SeekableChannelsProvider#getInputStream(SeekableByteChannel)} to ensure the channel's position is
     * incremented the exact amount that has been consumed from the resulting input stream. To remain valid, the caller
     * must ensure that the resulting input stream isn't re-wrapped by any downstream code in a way that would adversely
     * effect the position (such as re-wrapping the resulting input stream with buffering).
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
        return getReadChannel(channelContext, convertToURI(uriStr));
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
