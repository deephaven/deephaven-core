//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.datastructures.intrusive.IntrusiveSoftLRU;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A cache for {@link IntrusivePage IntrusivePages}. This data structure stores pages as {@link SoftReference soft
 * references} and maintains them as an LRU cache. External references to cached pages should be held via
 * {@link WeakReference weak references} so that as memory pressure builds the pages can be evicted from the cache.
 * Entries may also be partial pages; see {@link SparsePage}.
 */
public class PageCache<ATTR extends Any> extends IntrusiveSoftLRU<PageCache.IntrusivePage<ATTR>> {

    /**
     * Sentinel reference for a null page
     */
    private static final WeakReference<?> NULL_PAGE = new WeakReference<>(null);

    /** Strong references to every page touched while pinned; {@code null} when not pinned. */
    private static volatile Set<IntrusivePage<?>> pinned;

    /**
     * @return The null page sentinel
     */
    public static <ATTR extends Any> WeakReference<IntrusivePage<ATTR>> getNullPage() {
        // noinspection unchecked
        return (WeakReference<IntrusivePage<ATTR>>) NULL_PAGE;
    }

    /**
     * Intrusive data structure for page caching.
     */
    public static class IntrusivePage<ATTR extends Any> extends IntrusiveSoftLRU.Node.Impl<IntrusivePage<ATTR>> {

        private final ChunkPage<ATTR> page;

        public IntrusivePage(ChunkPage<ATTR> page) {
            this.page = page;
        }

        /**
         * For cache entries that are not whole pages; their {@link #getPage()} is {@code null}.
         */
        protected IntrusivePage() {
            this.page = null;
        }

        public ChunkPage<ATTR> getPage() {
            return page;
        }
    }

    public <ATTR2 extends Any> PageCache<ATTR2> castAttr() {
        // noinspection unchecked
        return (PageCache<ATTR2>) this;
    }

    public PageCache(final int initialCapacity, final int maxCapacity) {
        super(IntrusiveSoftLRU.Node.Adapter.getInstance(), initialCapacity, maxCapacity);
    }

    @Override
    public void touch(@NotNull final IntrusivePage<ATTR> page) {
        final Set<IntrusivePage<?>> localPinned = pinned;
        if (localPinned != null) {
            localPinned.add(page);
        }
        super.touch(page);
    }

    /**
     * Keep every page touched in any page cache strongly reachable until the result is closed, so that a GC can't clear
     * them between reads that a test expects to share. Process-wide and not reentrant.
     *
     * @return Closing it unpins the pages
     */
    @TestUseOnly
    static SafeCloseable pinTouchedPages() {
        pinned = ConcurrentHashMap.newKeySet();
        return () -> pinned = null;
    }
}
