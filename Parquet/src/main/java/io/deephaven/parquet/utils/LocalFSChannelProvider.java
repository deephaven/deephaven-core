package io.deephaven.parquet.utils;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LocalFSChannelProvider implements SeekableChannelsProvider {

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
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
}
