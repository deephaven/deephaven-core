//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.file;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.util.thread.NamingThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple least-recently-opened "cache" for FileHandles, to avoid running up against ulimits. Will probably not achieve
 * satisfactory results if the number of file handles concurrently in active use exceeds capacity. Note that returned
 * FileHandles may be closed asynchronously by the factory.
 * <p>
 * TODO: Consider adding a lookup to enable handle sharing. Not necessary for current usage.
 */
public class TrackedFileHandleFactory implements FileHandleFactory {

    private static volatile TrackedFileHandleFactory instance;

    public static TrackedFileHandleFactory getInstance() {
        if (instance == null) {
            synchronized (TrackedFileHandleFactory.class) {
                if (instance == null) {
                    instance = new TrackedFileHandleFactory(
                            Executors.newSingleThreadScheduledExecutor(
                                    new NamingThreadFactory(TrackedFileHandleFactory.class, "cleanupScheduler", true)),
                            Configuration.getInstance().getInteger("TrackedFileHandleFactory.maxOpenFiles")) {

                        @Override
                        public void shutdown() {
                            super.shutdown();
                            getScheduler().shutdown();
                        }
                    };
                }
            }
        }
        return instance;
    }

    private final static double DEFAULT_TARGET_USAGE_RATIO = 0.9;
    private final static long DEFAULT_CLEANUP_INTERVAL_MILLIS = 60_000;

    private final ScheduledExecutorService scheduler;
    private final int capacity;
    private final double targetUsageRatio;

    private final int targetUsageThreshold;
    private final ScheduledFuture<?> cleanupJobFuture;

    private final AtomicInteger size = new AtomicInteger(0);
    private final Queue<HandleReference> handleReferences = new ConcurrentLinkedQueue<>();

    public final FileToHandleFunction readOnlyHandleCreator = FileHandleFactory.toReadOnlyHandleCreator(this);
    public final FileToHandleFunction readWriteCreateHandleCreator =
            FileHandleFactory.toReadWriteCreateHandleCreator(this);
    public final FileToHandleFunction writeAppendCreateHandleCreator =
            FileHandleFactory.toWriteAppendCreateHandleCreator(this);
    public final FileToHandleFunction writeTruncateCreateHandleCreator =
            FileHandleFactory.toWriteTruncateCreateHandleCreator(this);

    /**
     * Full constructor.
     * 
     * @param scheduler The {@link ScheduledExecutorService} to use for cleanup
     * @param capacity The total number of file handles to allow outstanding
     * @param targetUsageRatio The target usage threshold as a ratio of capacity, in [0.1, 0.9]
     * @param cleanupIntervalMillis The interval for asynchronous cleanup attempts
     */
    @VisibleForTesting
    TrackedFileHandleFactory(
            @NotNull final ScheduledExecutorService scheduler,
            final int capacity,
            final double targetUsageRatio,
            final long cleanupIntervalMillis) {
        this.scheduler = scheduler;
        this.capacity = Require.gtZero(capacity, "capacity");
        this.targetUsageRatio = Require.inRange(targetUsageRatio, 0.1, 0.9, "targetUsageRatio");
        targetUsageThreshold = Require.gtZero((int) (capacity * targetUsageRatio), "targetUsageThreshold");

        cleanupJobFuture = scheduler.scheduleAtFixedRate(
                new CleanupJob(), cleanupIntervalMillis, cleanupIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Constructor with default target usage ratio of 0.9 (90%) and cleanup attempts every 60 seconds.
     *
     * @param scheduler The {@link ScheduledExecutorService} to use for cleanup
     * @param capacity The total number of file handles to allow outstanding
     */
    @VisibleForTesting
    TrackedFileHandleFactory(@NotNull final ScheduledExecutorService scheduler, final int capacity) {
        this(scheduler, capacity, DEFAULT_TARGET_USAGE_RATIO, DEFAULT_CLEANUP_INTERVAL_MILLIS);
    }

    @VisibleForTesting
    ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    public int getCapacity() {
        return capacity;
    }

    public double getTargetUsageRatio() {
        return targetUsageRatio;
    }

    public int getTargetUsageThreshold() {
        return targetUsageThreshold;
    }

    public int getSize() {
        return size.get();
    }

    @Override
    @NotNull
    public final FileHandle makeHandle(@NotNull final File file, @NotNull final OpenOption[] openOptions)
            throws IOException {
        if (size.get() >= capacity) {
            // Synchronous cleanup at full capacity.
            cleanup();
        }
        final FileHandle handle = FileHandle.open(file.toPath(), CloseRecorder::new, openOptions);
        handleReferences.add(new HandleReference(handle));
        return handle;
    }

    private void cleanup() {
        for (final Iterator<HandleReference> handleReferenceIterator =
                handleReferences.iterator(); handleReferenceIterator.hasNext();) {
            final HandleReference handleReference = handleReferenceIterator.next();
            final FileHandle handle = handleReference.get();
            if (handle == null) {
                handleReference.reclaim();
                handleReferenceIterator.remove();
            } else if (!handle.isOpen()) {
                // NB: handle.isOpen() will invoke the close recorder as a side effect, if necessary.
                handleReferenceIterator.remove();
            }
        }

        HandleReference handleReference;
        while (size.get() > targetUsageThreshold && (handleReference = handleReferences.poll()) != null) {
            // NB: poll() might return null if targetUsageThreshold is very low and some thread has incremented size but
            // not added its handle.
            handleReference.reclaim();
        }
    }

    @SuppressWarnings("unused")
    public void closeAll() {
        HandleReference handleReference;
        while ((handleReference = handleReferences.poll()) != null) {
            handleReference.reclaim();
        }
    }

    public void shutdown() {
        cleanupJobFuture.cancel(true);
    }

    private class CleanupJob implements Runnable {

        public void run() {
            try {
                cleanup();
            } catch (Exception e) {
                throw new UncheckedDeephavenException("TrackedFileHandleFactory.CleanupJob: Unexpected exception", e);
            }
        }
    }

    private class CloseRecorder extends AtomicBoolean implements Runnable {
        private CloseRecorder() {
            size.incrementAndGet();
        }

        @Override
        public void run() {
            if (compareAndSet(false, true)) {
                size.decrementAndGet();
            }
        }
    }

    private static class HandleReference extends WeakReference<FileHandle> {

        private final FileChannel fileChannel;
        private final Runnable closeRecorder;

        private HandleReference(@NotNull final FileHandle referent) {
            super(referent);
            this.fileChannel = referent.fileChannel();
            this.closeRecorder = referent.postCloseProcedure();
        }

        private void reclaim() {
            if (fileChannel.isOpen()) {
                try {
                    fileChannel.close();
                } catch (IOException ignored) {
                    // If close fails, there's really nothing to be done about it.
                }
            }
            closeRecorder.run();
        }
    }
}
