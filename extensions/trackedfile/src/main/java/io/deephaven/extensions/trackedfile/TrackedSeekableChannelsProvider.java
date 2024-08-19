//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.trackedfile;

import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.util.file.FileHandle;
import io.deephaven.engine.util.file.FileHandleFactory;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.engine.util.file.TrackedSeekableByteChannel;
import io.deephaven.util.channel.Channels;
import io.deephaven.util.channel.CompletableOutputStream;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.BaseSeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.stream.Stream;

import static io.deephaven.base.FileUtils.FILE_URI_SCHEME;

/**
 * {@link SeekableChannelsProvider} implementation that is constrained by a Deephaven {@link TrackedFileHandleFactory}.
 */
final class TrackedSeekableChannelsProvider implements SeekableChannelsProvider {

    private static final int MAX_READ_BUFFER_SIZE = 1 << 16; // 64 KiB

    private final TrackedFileHandleFactory fileHandleFactory;

    TrackedSeekableChannelsProvider(@NotNull final TrackedFileHandleFactory fileHandleFactory) {
        this.fileHandleFactory = fileHandleFactory;
    }

    @Override
    public SeekableChannelContext makeContext() {
        return new BaseSeekableChannelContext();
    }

    @Override
    public boolean isCompatibleWith(@Nullable final SeekableChannelContext channelContext) {
        return channelContext instanceof BaseSeekableChannelContext;
    }

    @Override
    public boolean exists(@NotNull final URI uri) {
        return Files.exists(Path.of(uri));
    }

    @Override
    public SeekableByteChannel getReadChannel(@Nullable final SeekableChannelContext channelContext,
            @NotNull final URI uri) throws IOException {
        // context is unused here
        Assert.assertion(FILE_URI_SCHEME.equals(uri.getScheme()), "Expected a file uri, got " + uri);
        return new TrackedSeekableByteChannel(fileHandleFactory.readOnlyHandleCreator, new File(uri));
    }

    @Override
    public InputStream getInputStream(SeekableByteChannel channel, int sizeHint) {
        // The following stream will read from the channel in chunks of bufferSize bytes
        final int bufferSize = Math.min(sizeHint, MAX_READ_BUFFER_SIZE);
        return new BufferedInputStream(Channels.newInputStreamNoClose(channel), bufferSize);
    }

    @Override
    public CompletableOutputStream getOutputStream(@NotNull final URI uri, int bufferSizeHint) throws IOException {
        return new LocalCompletableOutputStream(new File(uri), this, bufferSizeHint);
    }

    @Override
    public Stream<URI> list(@NotNull final URI directory) throws IOException {
        // Assuming that the URI is a file, not a directory. The caller should manage file vs. directory handling in
        // the processor.
        return Files.list(Path.of(directory)).map(path -> FileUtils.convertToURI(path, false));
    }

    @Override
    public Stream<URI> walk(@NotNull final URI directory) throws IOException {
        // Assuming that the URI is a file, not a directory. The caller should manage file vs. directory handling in
        // the processor.
        return Files.walk(Path.of(directory)).map(path -> FileUtils.convertToURI(path, false));
    }

    SeekableByteChannel getWriteChannel(@NotNull final File destFile) throws IOException {
        return new TrackedSeekableByteChannel(new TruncateOnceFileCreator(fileHandleFactory), destFile);
    }

    private static final class TruncateOnceFileCreator implements FileHandleFactory.FileToHandleFunction {

        private static final AtomicIntegerFieldUpdater<TruncateOnceFileCreator> FIRST_TIME_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(TruncateOnceFileCreator.class, "firstTime");
        private static final int FIRST_TIME_TRUE = 1;
        private static final int FIRST_TIME_FALSE = 0;

        private final TrackedFileHandleFactory fileHandleFactory;

        private volatile int firstTime = FIRST_TIME_TRUE;

        private TruncateOnceFileCreator(@NotNull final TrackedFileHandleFactory fileHandleFactory) {
            this.fileHandleFactory = fileHandleFactory;
        }

        @NotNull
        @Override
        public FileHandle invoke(@NotNull final File file) throws IOException {
            if (FIRST_TIME_UPDATER.compareAndSet(this, FIRST_TIME_TRUE, FIRST_TIME_FALSE)) {
                return fileHandleFactory.writeTruncateCreateHandleCreator.invoke(file);
            }
            return fileHandleFactory.writeAppendCreateHandleCreator.invoke(file);
        }
    }

    @Override
    public void close() {}
}
