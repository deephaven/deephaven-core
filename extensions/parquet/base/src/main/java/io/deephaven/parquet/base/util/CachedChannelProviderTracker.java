package io.deephaven.parquet.base.util;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;

/**
 * Singleton class for tracking weak references of {@link CachedChannelProvider}, with ability to lookup providers based
 * on file path. This is useful to invalidate channels and file handles in case the underlying file has been modified.
 */
public class CachedChannelProviderTracker { // TODO Think of a better name
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
    private final Map<String, List<WeakReference<CachedChannelProvider>>> fileToProviderMap = new HashMap<>();
    // TODO Which map type can I use here? I want to use a map which uses open addressing in a thread safe manner. Ryan
    // suggested KeyedObjectHashMap but that seems to have constraints that key should be derivable from the value.
    // Need to discuss with someone.

    private CachedChannelProviderTracker() {}

    /**
     * Register a {@link CachedChannelProvider} as associated with a particular {@code file}
     *
     * @param ccp {@link CachedChannelProvider} used to create channels for {@code file}
     * @param file File path
     */
    public final synchronized void registerCachedChannelProvider(@NotNull final CachedChannelProvider ccp,
            @NotNull final File file)
            throws IOException {
        cleanup();
        final String filePath = file.getCanonicalPath();
        List<WeakReference<CachedChannelProvider>> providerList =
                fileToProviderMap.computeIfAbsent(filePath, k -> new LinkedList<>());
        providerList.add(new WeakReference<>(ccp));
    }

    /**
     * Invalidate all channels and providers associated with the {@code file} to prevent reading overwritten files.
     *
     * @param file File path
     */
    public final synchronized void invalidateChannels(@NotNull final File file) throws IOException {
        cleanup();
        final String filePath = file.getCanonicalPath();
        List<WeakReference<CachedChannelProvider>> providerList = fileToProviderMap.remove(filePath);
        if (providerList == null) {
            return;
        }
        for (WeakReference<CachedChannelProvider> providerWeakRef : providerList) {
            final CachedChannelProvider ccp = providerWeakRef.get();
            if (ccp != null) {
                ccp.invalidate();
            }
        }
    }

    /**
     * Clear any null weak-references
     */
    // TODO Where should I call cleanup from? Right now, I am calling it from inside public API
    private void cleanup() {
        final Iterator<Map.Entry<String, List<WeakReference<CachedChannelProvider>>>> mapIter =
                fileToProviderMap.entrySet().iterator();
        while (mapIter.hasNext()) {
            final Map.Entry<String, List<WeakReference<CachedChannelProvider>>> mapEntry = mapIter.next(); // Concurrent
                                                                                                           // access
            final List<WeakReference<CachedChannelProvider>> providerList = mapEntry.getValue();
            final Iterator<WeakReference<CachedChannelProvider>> providerWeakRefIt = providerList.iterator();
            while (providerWeakRefIt.hasNext()) {
                final WeakReference<CachedChannelProvider> providerWeakRef = providerWeakRefIt.next();
                final CachedChannelProvider ccp = providerWeakRef.get();
                if (ccp == null) {
                    providerWeakRefIt.remove();
                }
            }
            if (providerList.isEmpty()) {
                mapIter.remove();
            }
        }
    }
}
