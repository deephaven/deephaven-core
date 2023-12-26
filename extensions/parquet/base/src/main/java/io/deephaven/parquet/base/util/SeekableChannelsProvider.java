/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base.util;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

public interface SeekableChannelsProvider {

    interface ChannelContext extends SafeCloseable {

        ChannelContext NULL = new ChannelContext() {};

        /**
         * Release any resources associated with this context. The context should not be used afterward.
         */
        default void close() {}
    }

    ChannelContext makeContext();

    interface ContextHolder {
        void setContext(ChannelContext context);

        @FinalDefault
        default void clearContext() {
            setContext(null);
        }
    }

    default SeekableByteChannel getReadChannel(@NotNull final ChannelContext context, @NotNull final String path)
            throws IOException {
        return getReadChannel(context, Paths.get(path));
    }

    SeekableByteChannel getReadChannel(@NotNull final ChannelContext context, @NotNull Path path) throws IOException;

    default SeekableByteChannel getWriteChannel(@NotNull final String path, final boolean append) throws IOException {
        return getWriteChannel(Paths.get(path), append);
    }

    SeekableByteChannel getWriteChannel(@NotNull Path path, boolean append) throws IOException;
}
