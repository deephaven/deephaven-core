/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util.file;

import io.deephaven.net.CommBase;
import io.deephaven.base.Procedure;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.sched.Scheduler;
import io.deephaven.io.sched.TimedJob;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple least-recently-opened "cache" for FileHandles, to avoid running up against ulimits. Will probably not achieve
 * satisfactory results if the number of file handles concurrently in active use exceeds capacity. Note that returned
 * FileHandles may be closed asynchronously by the factory.
 *
 * TODO: Consider adding a lookup to enable handle sharing. Not necessary for current usage.
 */
public class TrackedFileHandleFactory implements FileHandleFactory {

    private static volatile TrackedFileHandleFactory instance;

    public static TrackedFileHandleFactory getInstance() {
        if (instance == null) {
            synchronized (TrackedFileHandleFactory.class) {
                if (instance == null) {
                    instance = new TrackedFileHandleFactory(
                            CommBase.singleThreadedScheduler("TrackedFileHandleFactory.CleanupScheduler", Logger.NULL)
                                    .start(),
                            Configuration.getInstance().getInteger("TrackedFileHandleFactory.maxOpenFiles"));
                }
            }
        }
        return instance;
    }

    private final static double DEFAULT_TARGET_USAGE_RATIO = 0.9;
    private final static long DEFAULT_CLEANUP_INTERVAL_MILLIS = 60_000;

    private final Scheduler scheduler;
    private final int capacity;
    private final double targetUsageRatio;

    private final int targetUsageThreshold;

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
     * @param scheduler The scheduler to use for cleanup
     * @param capacity The total number of file handles to allow outstanding
     * @param targetUsageRatio The target usage threshold as a ratio of capacity, in [0.1, 0.9]
     * @param cleanupIntervalMillis The interval for asynchronous cleanup attempts
     */
    public TrackedFileHandleFactory(@NotNull final Scheduler scheduler, final int capacity,
            final double targetUsageRatio, final long cleanupIntervalMillis) {
        this.scheduler = scheduler;
        this.capacity = Require.gtZero(capacity, "capacity");
        this.targetUsageRatio = Require.inRange(targetUsageRatio, 0.1, 0.9, "targetUsageRatio");
        targetUsageThreshold = Require.gtZero((int) (capacity * targetUsageRatio), "targetUsageThreshold");

        new CleanupJob(cleanupIntervalMillis).schedule();
    }

    /**
     * Constructor with default target usage ratio of 0.9 (90%) and cleanup attempts every 60 seconds.
     * 
     * @param scheduler The scheduler to use for cleanup
     * @param capacity The total number of file handles to allow outstanding
     */
    public TrackedFileHandleFactory(@NotNull final Scheduler scheduler, final int capacity) {
        this(scheduler, capacity, DEFAULT_TARGET_USAGE_RATIO, DEFAULT_CLEANUP_INTERVAL_MILLIS);
    }

    public Scheduler getScheduler() {
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
        final FileChannel fileChannel = FileChannel.open(file.toPath(), openOptions);
        final CloseRecorder closeRecorder = new CloseRecorder();
        final FileHandle handle = new FileHandle(fileChannel, closeRecorder);
        handleReferences.add(new HandleReference(handle, fileChannel, closeRecorder));
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

    private class CleanupJob extends TimedJob {

        private final long intervalMills;

        private CleanupJob(final long intervalMills) {
            this.intervalMills = intervalMills;
        }

        private void schedule() {
            scheduler.installJob(this, scheduler.currentTimeMillis() + intervalMills);
        }

        @Override
        public void timedOut() {
            try {
                cleanup();
            } catch (Exception e) {
                throw new RuntimeException("TrackedFileHandleFactory.CleanupJob: Unexpected exception", e);
            }
            schedule();
        }
    }

    private class CloseRecorder implements Procedure.Nullary {

        private final AtomicBoolean reclaimed = new AtomicBoolean(false);

        private CloseRecorder() {
            size.incrementAndGet();
        }

        @Override
        public void call() {
            if (reclaimed.compareAndSet(false, true)) {
                size.decrementAndGet();
            }
        }
    }

    private class HandleReference extends WeakReference<FileHandle> {

        private final FileChannel fileChannel;
        private final CloseRecorder closeRecorder;

        private HandleReference(@NotNull final FileHandle referent, @NotNull final FileChannel fileChannel,
                @NotNull final CloseRecorder closeRecorder) {
            super(referent);
            this.fileChannel = fileChannel;
            this.closeRecorder = closeRecorder;
        }

        private void reclaim() {
            if (fileChannel.isOpen()) {
                try {
                    fileChannel.close();
                } catch (IOException ignored) {
                    // If close fails, there's really nothing to be done about it.
                }
            }
            closeRecorder.call();
        }
    }
}
