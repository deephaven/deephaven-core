package io.deephaven.parquet.utils;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

public interface SeekableChannelsProvider {

    default SeekableByteChannel getReadChannel(String path) throws IOException {
        return getReadChannel(Paths.get(path));
    }

    SeekableByteChannel getReadChannel(Path path) throws IOException;

    default SeekableByteChannel getWriteChannel(String filePath, boolean append) throws IOException {
        return getWriteChannel(Paths.get(filePath), append);
    }

    SeekableByteChannel getWriteChannel(Path filePath, boolean append) throws IOException;
}
