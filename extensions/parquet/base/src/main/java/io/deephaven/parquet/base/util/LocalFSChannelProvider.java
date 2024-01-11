/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base.util;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LocalFSChannelProvider implements SeekableChannelsProvider {

    @Override
    public ChannelContext makeContext() {
        return ChannelContext.NULL;
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final ChannelContext context, @NotNull final URI uri)
            throws IOException {
        // context is unused here because it is NULL
        return FileChannel.open(Path.of(uri), StandardOpenOption.READ);
    }

    @Override
    public SeekableByteChannel getWriteChannel(@NotNull final Path filePath, final boolean append) throws IOException {
        final FileChannel result = FileChannel.open(filePath,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING);
        if (append) {
            result.position(result.size());
        } else {
            result.position(0);
        }
        return result;
    }

    @Override
    public void close() {}
}
