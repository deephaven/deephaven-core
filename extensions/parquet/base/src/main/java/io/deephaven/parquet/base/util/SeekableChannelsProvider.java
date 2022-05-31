package io.deephaven.parquet.base.util;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

public interface SeekableChannelsProvider {

    default SeekableByteChannel getReadChannel(@NotNull final String path) throws IOException {
        return getReadChannel(Paths.get(path));
    }

    SeekableByteChannel getReadChannel(@NotNull Path path) throws IOException;

    default SeekableByteChannel getWriteChannel(@NotNull final String path, final boolean append) throws IOException {
        return getWriteChannel(Paths.get(path), append);
    }

    SeekableByteChannel getWriteChannel(@NotNull Path path, boolean append) throws IOException;
}
