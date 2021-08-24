package io.deephaven.util.datastructures.hash;

import io.deephaven.hash.IntrusiveChainedHashAdapter;
import io.deephaven.base.verify.Assert;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A pool for "free" entries to be use with IntrusiveChainedHash structures, implemented as a stack
 * using the same adapter and intrusive fields. Requires external synchronization of all methods for
 * concurrent use.
 */
public class IntrusiveChainedEntryPool<VALUE_TYPE> {

    /**
     * The adapter.
     */
    private final IntrusiveChainedHashAdapter<VALUE_TYPE> adapter;

    /**
     * The top of the stack.
     */
    private VALUE_TYPE top;

    /**
     * Construct a new pool with the supplied adapter.
     * 
     * @param adapter The adapter
     */
    public IntrusiveChainedEntryPool(
        @NotNull final IntrusiveChainedHashAdapter<VALUE_TYPE> adapter) {
        this.adapter = adapter;
    }

    /**
     * Give a currently un-linked entry to the pool.
     * 
     * @param entry The entry
     */
    public void give(@NotNull final VALUE_TYPE entry) {
        Assert.eqNull(adapter.getNext(entry), "adapter.getNext(entry)");
        if (top != null) {
            adapter.setNext(entry, top);
        }
        top = entry;
    }

    /**
     * Take an entry from the pool.
     * 
     * @return The entry taken, or null if the pool was empty
     */
    public @Nullable VALUE_TYPE take() {
        if (top == null) {
            return null;
        }
        final VALUE_TYPE result = top;
        top = adapter.getNext(top);
        adapter.setNext(result, null);
        return result;
    }
}
