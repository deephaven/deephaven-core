/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util.file;

import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.sched.Scheduler;
import io.deephaven.net.CommBase;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.file.OpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;


/**
 * An extension of {@link TrackedFileHandleFactory} class with ability to lookup file handles based on file path and
 * invalidate them in case the underlying file has been modified. This class is used for cases where we invalidate old
 * file handles when overwriting an existing file.
 */
public class TrackedFileHandleFactoryWithLookup extends TrackedFileHandleFactory {

    private static volatile TrackedFileHandleFactoryWithLookup instance;

    /**
     * Mapping from absolute file path to list of handles
     */
    private final Map<String, List<WeakReference<FileHandle>>> fileToHandleMap = new HashMap<>();

    public static TrackedFileHandleFactoryWithLookup getInstance() {
        if (instance == null) {
            synchronized (TrackedFileHandleFactoryWithLookup.class) {
                if (instance == null) {
                    instance = new TrackedFileHandleFactoryWithLookup(
                            CommBase.singleThreadedScheduler("TrackedFileHandleFactory.CleanupScheduler", Logger.NULL)
                                    .start(),
                            Configuration.getInstance().getInteger("TrackedFileHandleFactory.maxOpenFiles"));
                }
            }
        }
        return instance;
    }

    /**
     * Pass through constructors for the parent class {@link TrackedFileHandleFactory}
     */
    private TrackedFileHandleFactoryWithLookup(@NotNull final Scheduler scheduler, final int capacity) {
        super(scheduler, capacity);
    }

    @Override
    @NotNull
    public final FileHandle makeHandle(@NotNull final File file, @NotNull final OpenOption[] openOptions)
            throws IOException {
        final FileHandle handle = super.makeHandle(file, openOptions);
        final String filePath = file.getAbsolutePath();
        List<WeakReference<FileHandle>> handleList = fileToHandleMap.computeIfAbsent(filePath, k -> new LinkedList<>());
        handleList.add(new WeakReference<>(handle));
        return handle;
    }

    @Override
    protected void cleanup() {
        super.cleanup();
        final Iterator<Map.Entry<String, List<WeakReference<FileHandle>>>> mapIter =
                fileToHandleMap.entrySet().iterator();
        while (mapIter.hasNext()) {
            final Map.Entry<String, List<WeakReference<FileHandle>>> mapEntry = mapIter.next();
            final List<WeakReference<FileHandle>> handleList = mapEntry.getValue();
            final Iterator<WeakReference<FileHandle>> handleWeakRefIterator = handleList.iterator();
            while (handleWeakRefIterator.hasNext()) {
                final WeakReference<FileHandle> handleWeakRef = handleWeakRefIterator.next();
                final FileHandle handle = handleWeakRef.get();
                if (handle == null) {
                    handleWeakRefIterator.remove();
                }
            }
            if (handleList.isEmpty()) {
                mapIter.remove();
            }
        }
    }

    /**
     * Close all existing file channels and handles
     */
    @Override
    public void closeAll() {
        super.closeAll();
        fileToHandleMap.clear();
    }

    /**
     * Invalidate any handles associated with the {@code file} to prevent reading overwritten files.
     *
     * @param file File path
     */
    public void invalidateHandles(final File file) {
        final String filePath = file.getAbsolutePath();
        List<WeakReference<FileHandle>> handleList = fileToHandleMap.remove(filePath);
        if (handleList == null) {
            return;
        }
        for (WeakReference<FileHandle> handleWeakRef : handleList) {
            final FileHandle handle = handleWeakRef.get();
            if (handle != null) {
                handle.invalidate();
            }
        }
    }
}
