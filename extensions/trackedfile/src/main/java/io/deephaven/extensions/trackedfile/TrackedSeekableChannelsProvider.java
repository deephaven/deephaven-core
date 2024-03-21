//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.trackedfile;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.util.file.FileHandle;
import io.deephaven.engine.util.file.FileHandleFactory;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.engine.util.file.TrackedSeekableByteChannel;
import io.deephaven.util.channel.Channels;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static io.deephaven.extensions.trackedfile.TrackedSeekableChannelsProviderPlugin.FILE_URI_SCHEME;

/**
 * {@link SeekableChannelsProvider} implementation that is constrained by a Deephaven {@link TrackedFileHandleFactory}.
 */
final class TrackedSeekableChannelsProvider implements SeekableChannelsProvider {

    private final TrackedFileHandleFactory fileHandleFactory;

    TrackedSeekableChannelsProvider(@NotNull final TrackedFileHandleFactory fileHandleFactory) {
        this.fileHandleFactory = fileHandleFactory;
    }

    @Override
    public SeekableChannelContext makeContext() {
        // No additional context required for local FS
        return SeekableChannelContext.NULL;
    }

    @Override
    public boolean isCompatibleWith(@Nullable SeekableChannelContext channelContext) {
        // Context is not used, hence always compatible
        return true;
    }

    @Override
    public SeekableByteChannel getReadChannel(@Nullable final SeekableChannelContext channelContext,
            @NotNull final URI uri)
            throws IOException {
        // context is unused here
        Assert.assertion(FILE_URI_SCHEME.equals(uri.getScheme()), "Expected a file uri, got " + uri);
        return new TrackedSeekableByteChannel(fileHandleFactory.readOnlyHandleCreator, new File(uri));
    }

    @Override
    public InputStream getInputStream(SeekableByteChannel channel) {
        // TrackedSeekableByteChannel is not buffered, need to buffer
        return new BufferedInputStream(Channels.newInputStreamNoClose(channel));
    }

    @Override
    public SeekableByteChannel getWriteChannel(@NotNull final Path filePath, final boolean append)
            throws IOException {
        // NB: I'm not sure this is actually the intended behavior; the "truncate-once" is per-handle, not per file.
        return new TrackedSeekableByteChannel(append ? fileHandleFactory.writeAppendCreateHandleCreator
                : new TruncateOnceFileCreator(fileHandleFactory), filePath.toFile());
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
        public final FileHandle invoke(@NotNull final File file) throws IOException {
            if (FIRST_TIME_UPDATER.compareAndSet(this, FIRST_TIME_TRUE, FIRST_TIME_FALSE)) {
                return fileHandleFactory.writeTruncateCreateHandleCreator.invoke(file);
            }
            return fileHandleFactory.writeAppendCreateHandleCreator.invoke(file);
        }
    }

    @Override
    public void close() {}
}
