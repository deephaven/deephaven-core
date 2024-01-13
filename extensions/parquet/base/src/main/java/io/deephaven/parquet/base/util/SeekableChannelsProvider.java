/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base.util;

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

    interface ChannelContext extends SafeCloseable {

        ChannelContext NULL = new ChannelContext() {};

        /**
         * Release any resources associated with this context. The context should not be used afterward.
         */
        default void close() {}
    }

    /**
     * Create a new {@link ChannelContext} object for creating read and write channels via this provider.
     */
    ChannelContext makeContext();

    /**
     * Check if the given context is compatible with this provider. Useful to test if we can use provided
     * {@code channelContext} object for creating channels with this provider.
     */
    boolean isCompatibleWith(@NotNull ChannelContext channelContext);

    interface ContextHolder {
        void setContext(ChannelContext channelContext);

        @FinalDefault
        default void clearContext() {
            setContext(null);
        }
    }

    default SeekableByteChannel getReadChannel(@NotNull ChannelContext channelContext, @NotNull String uriStr)
            throws IOException {
        return getReadChannel(channelContext, convertToURI(uriStr));
    }

    SeekableByteChannel getReadChannel(@NotNull ChannelContext channelContext, @NotNull URI uri) throws IOException;

    default SeekableByteChannel getWriteChannel(@NotNull String path, final boolean append) throws IOException {
        return getWriteChannel(Paths.get(path), append);
    }

    SeekableByteChannel getWriteChannel(@NotNull Path path, boolean append) throws IOException;
}
