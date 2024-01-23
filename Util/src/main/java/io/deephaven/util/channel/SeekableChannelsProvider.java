/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.channel;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

public interface SeekableChannelsProvider extends SafeCloseable {

    /**
     * Take the file source path or URI and convert it to a URI object.
     *
     * @param source The file source path or URI
     * @return The URI object
     */
    static URI convertToURI(final String source) {
        final URI uri;
        try {
            uri = new URI(source);
        } catch (final URISyntaxException e) {
            // If the URI is invalid, assume it's a file path
            return new File(source).toURI();
        }
        if (uri.getScheme() == null) {
            // Need to convert to a "file" URI
            return new File(source).toURI();
        }
        return uri;
    }

    /**
     * Create a new {@link SeekableChannelContext} object for creating read channels via this provider.
     */
    SeekableChannelContext makeContext();

    /**
     * Check if the given context is compatible with this provider. Useful to test if we can use provided
     * {@code context} object for creating channels with this provider.
     */
    boolean isCompatibleWith(@NotNull SeekableChannelContext channelContext);

    interface ContextHolder {
        void setContext(SeekableChannelContext channelContext);

        @FinalDefault
        default void clearContext() {
            setContext(null);
        }
    }

    default SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull String uriStr)
            throws IOException {
        return getReadChannel(channelContext, convertToURI(uriStr));
    }

    SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull URI uri)
            throws IOException;

    default SeekableByteChannel getWriteChannel(@NotNull final String path, final boolean append) throws IOException {
        return getWriteChannel(Paths.get(path), append);
    }

    SeekableByteChannel getWriteChannel(@NotNull Path path, boolean append) throws IOException;
}
