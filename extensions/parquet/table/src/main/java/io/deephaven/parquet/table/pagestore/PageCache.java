package io.deephaven.parquet.table.pagestore;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.util.datastructures.intrusive.IntrusiveSoftLRU;

import java.lang.ref.WeakReference;

/**
 * A Cache for {@link IntrusivePage IntrusivePages}.  This datastructure stores pages as soft references and maintains
 * them as an LRU cache.  Externally references can be held as WeakReferences so that as memory pressure builds they
 * can be evicted from the cache.
 */
public class PageCache<ATTR extends Any> extends IntrusiveSoftLRU<PageCache.IntrusivePage<ATTR>> {

    /**
     * Sentinel reference for a null page
     */
    private static final WeakReference<?> NULL_PAGE = new WeakReference<>(null);

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
}
