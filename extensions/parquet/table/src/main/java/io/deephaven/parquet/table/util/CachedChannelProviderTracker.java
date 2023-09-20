package io.deephaven.parquet.table.util;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.Iterator;

/**
 * Singleton class for tracking weak references of {@link CachedChannelProvider}, with ability to lookup providers based
 * on file path. This is useful to invalidate channels and file handles in case the underlying file has been modified.
 */
public class CachedChannelProviderTracker {
    private static volatile CachedChannelProviderTracker instance;

    public static CachedChannelProviderTracker getInstance() {
        if (instance == null) {
            synchronized (CachedChannelProviderTracker.class) {
                if (instance == null) {
                    instance = new CachedChannelProviderTracker();
                }
            }
        }
        return instance;
    }

    /**
     * Mapping from canonical file path to weak references of cached channel providers
     */
    private static class FileToProviderMapEntry {
        public final String fileCanonicalPath;
        public final List<WeakReference<CachedChannelProvider>> providerList;

        public FileToProviderMapEntry(final String path,
                final List<WeakReference<CachedChannelProvider>> providerList) {
            this.fileCanonicalPath = path;
            this.providerList = providerList;
        }
    }

    private final Map<String, FileToProviderMapEntry> fileToProviderMap;

    @VisibleForTesting
    static final int PROVIDER_MAP_CLEANUP_LIMIT = 100;

    private CachedChannelProviderTracker() {
        fileToProviderMap = new KeyedObjectHashMap<>(new KeyedObjectKey.Basic<>() {
            @Override
            public String getKey(FileToProviderMapEntry entry) {
                return entry.fileCanonicalPath;
            }
        });
    }

    /**
     * Register a {@link CachedChannelProvider} as associated with a particular {@code file}
     *
     * @param ccp {@link CachedChannelProvider} used to create channels for {@code file}
     * @param file File path
     */
    public final void registerCachedChannelProvider(@NotNull final CachedChannelProvider ccp, @NotNull final File file)
            throws IOException {
        final String filePath = file.getCanonicalPath();
        FileToProviderMapEntry entry = fileToProviderMap.computeIfAbsent(filePath,
                k -> new FileToProviderMapEntry(filePath, new CopyOnWriteArrayList<>()));
        if (entry.providerList.stream().anyMatch(providerWeakRef -> providerWeakRef.get() == ccp)) {
            return;
        }
        entry.providerList.add(new WeakReference<>(ccp));
        if (fileToProviderMap.size() >= PROVIDER_MAP_CLEANUP_LIMIT) {
            tryCleanup();
        }
    }

    /**
     * Invalidate all channels and providers associated with the {@code file} to prevent reading overwritten files.
     *
     * @param file File path
     */
    public final void invalidateChannels(@NotNull final File file) throws IOException {
        final String filePath = file.getCanonicalPath();
        FileToProviderMapEntry entry = fileToProviderMap.remove(filePath);
        if (entry == null) {
            return;
        }
        for (WeakReference<CachedChannelProvider> providerWeakRef : entry.providerList) {
            final CachedChannelProvider ccp = providerWeakRef.get();
            if (ccp != null) {
                ccp.invalidate(filePath);
            }
        }
    }

    /**
     * Clear any null weak-references to providers
     */
    @VisibleForTesting
    void tryCleanup() {
        final Iterator<Map.Entry<String, FileToProviderMapEntry>> mapIter = fileToProviderMap.entrySet().iterator();
        while (mapIter.hasNext()) {
            final List<WeakReference<CachedChannelProvider>> providerList = mapIter.next().getValue().providerList;
            providerList.removeIf(providerWeakRef -> providerWeakRef.get() == null);
            if (providerList.isEmpty()) {
                mapIter.remove();
            }
        }
    }

    @VisibleForTesting
    void reset() {
        fileToProviderMap.clear();
    }

    @VisibleForTesting
    int size() {
        return fileToProviderMap.size();
    }
}
