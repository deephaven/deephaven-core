//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import io.deephaven.base.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class LocalFSChannelProvider implements SeekableChannelsProvider {
    @Override
    public SeekableChannelContext makeContext() {
        // No additional context required for local FS
        return SeekableChannelContext.NULL;
    }

    @Override
    public boolean isCompatibleWith(@Nullable final SeekableChannelContext channelContext) {
        // Context is not used, hence always compatible
        return true;
    }

    @Override
    public SeekableByteChannel getReadChannel(@Nullable final SeekableChannelContext channelContext,
            @NotNull final URI uri)
            throws IOException {
        // context is unused here
        return FileChannel.open(Path.of(uri), StandardOpenOption.READ);
    }

    @Override
    public InputStream getInputStream(SeekableByteChannel channel) {
        // FileChannel is not buffered, need to buffer
        return new BufferedInputStream(Channels.newInputStreamNoClose(channel));
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
    public void list(@NotNull final URI directoryURI, @NotNull final Consumer<URI> processor)
            throws IOException {
        try (final Stream<Path> childFileStream = Files.list(Path.of(directoryURI))) {
            // Assuming that the URI is a file, not a directory. The caller should manage file vs. directory handling in
            // the processor.
            childFileStream.map(path -> FileUtils.convertToURI(path, false)).forEach(processor);
        }
    }

    @Override
    public void walk(@NotNull final URI directoryURI, @NotNull final Consumer<URI> processor)
            throws IOException {
        try (final Stream<Path> childFileStream = Files.walk(Path.of(directoryURI))) {
            // Assuming that the URI is a file, not a directory. The caller should manage file vs. directory handling in
            // the processor.
            childFileStream.map(path -> FileUtils.convertToURI(path, false)).forEach(processor);
        }
    }

    @Override
    public void close() {}
}
