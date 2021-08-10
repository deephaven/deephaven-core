package io.deephaven.db.v2.locations.parquet.local;

import io.deephaven.db.util.file.FileHandle;
import io.deephaven.db.util.file.FileHandleFactory;
import io.deephaven.db.util.file.TrackedFileHandleFactory;
import io.deephaven.db.util.file.TrackedSeekableByteChannel;
import io.deephaven.parquet.utils.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * {@link SeekableChannelsProvider} implementation that is constrained by a Deephaven {@link TrackedFileHandleFactory}.
 */
public class TrackedSeekableChannelsProvider implements SeekableChannelsProvider {

    private static volatile SeekableChannelsProvider instance;

    public static SeekableChannelsProvider getInstance() {
        if (instance == null) {
            synchronized (TrackedSeekableChannelsProvider.class) {
                if (instance == null) {
                    return instance = new TrackedSeekableChannelsProvider(TrackedFileHandleFactory.getInstance());
                }
            }
        }
        return instance;
    }

    private final TrackedFileHandleFactory fileHandleFactory;

    public TrackedSeekableChannelsProvider(@NotNull final TrackedFileHandleFactory fileHandleFactory) {
        this.fileHandleFactory = fileHandleFactory;
    }

    @Override
    public final SeekableByteChannel getReadChannel(@NotNull final Path path) throws IOException {
        return new TrackedSeekableByteChannel(fileHandleFactory.readOnlyHandleCreator, path.toFile());
    }

    @Override
    public final SeekableByteChannel getWriteChannel(@NotNull final Path filePath, final boolean append) throws IOException {
        // NB: I'm not sure this is actually the intended behavior; the "truncate-once" is per-handle, not per file.
        return new TrackedSeekableByteChannel(append ? fileHandleFactory.writeAppendCreateHandleCreator : new TruncateOnceFileCreator(fileHandleFactory), filePath.toFile());
    }

    private static final class TruncateOnceFileCreator implements FileHandleFactory.FileToHandleFunction {

        private static final AtomicIntegerFieldUpdater<TruncateOnceFileCreator> FIRST_TIME_UPDATER = AtomicIntegerFieldUpdater.newUpdater(TruncateOnceFileCreator.class, "firstTime");
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
}
