//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.cache;


import it.unimi.dsi.fastutil.objects.Reference2IntMap;
import it.unimi.dsi.fastutil.objects.Reference2IntOpenHashMap;
import org.jetbrains.annotations.NotNull;

/**
 * Utility for holding strong references to otherwise unreachable classes (e.g. listeners that will be weakly held by
 * the object they subscribe to).
 */
public class RetentionCache<TYPE> {

    private final Reference2IntMap<TYPE> retainedObjectToReferenceCount = newReferenceCountMap();

    private static <T> Reference2IntMap<T> newReferenceCountMap() {
        // Preserve the Trove default capacity (10) and load factor (0.5f) rather than fastutil's 16/0.75f.
        final Reference2IntOpenHashMap<T> m = new Reference2IntOpenHashMap<>(10, 0.5f);
        m.defaultReturnValue(0);
        return m;
    }

    /**
     * Ask this RetentionCache to hold on to a reference in order to ensure that {@code referent} remains
     * strongly-reachable for the garbage collector.
     *
     * @param referent The object to hold a reference to
     * @return {@code referent}, for convenience when retaining anonymous class instances
     */
    public synchronized TYPE retain(@NotNull final TYPE referent) {
        retainedObjectToReferenceCount.put(referent, retainedObjectToReferenceCount.getInt(referent) + 1);
        return referent;
    }

    /**
     * Ask this RetentionCache to forget about a reference.
     *
     * @param referent The referent to forget the reference to
     */
    public synchronized void forget(@NotNull final TYPE referent) {
        int referenceCount = retainedObjectToReferenceCount.getInt(referent);
        if (referenceCount == 1) {
            retainedObjectToReferenceCount.removeInt(referent);
        } else {
            retainedObjectToReferenceCount.put(referent, referenceCount - 1);
        }
    }

    /**
     * Ask this RetentionCache to forget all the references it's remembering.
     */
    @SuppressWarnings("unused")
    public synchronized void forgetAll() {
        retainedObjectToReferenceCount.clear();
    }
}
