package io.deephaven.util.datastructures.intrusive;

import io.deephaven.base.verify.Require;
import io.deephaven.util.annotations.VisibleForTesting;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.util.Arrays;

public class IntrusiveSoftLRU<T> {
    private final Adapter<T> adapter;

    private final int[] nexts;
    private final int[] prevs;
    private final SoftReference<T>[] softReferences;

    private int tail;

    private static final SoftReference<?> NULL = new SoftReference<>(null);

    private static <T> SoftReference<T> getNull() {
        // noinspection unchecked
        return (SoftReference<T>) NULL;
    }

    public IntrusiveSoftLRU(@NotNull final Adapter<T> adapter, int maxSize) {
        Require.gt(maxSize, "maxSize", 1);

        this.adapter = adapter;
        this.tail = 0;

        nexts = new int[maxSize];
        prevs = new int[maxSize];

        // noinspection unchecked
        softReferences = new SoftReference[maxSize];

        Arrays.fill(softReferences, getNull());

        nexts[0] = maxSize - 1;

        for (int i = 1; i < maxSize; ++i) {
            nexts[i] = i - 1;
            prevs[i - 1] = i;
        }

        prevs[maxSize - 1] = 0;
    }

    public synchronized void touch(@NotNull T t) {
        final int slot = adapter.getSlot(t);

        if (slot == -1 || softReferences[slot].get() != t) {
            // A new entry, place it on top of the tail
            adapter.setSlot(t, tail);
            softReferences[tail] = adapter.getOwner(t);

            // Move the tail back one slot
            tail = prevs[tail];
        } else if (slot != tail) {
            // We already have an entry, we just move it to the head
            final int head = nexts[tail];

            if (slot != head) {
                // Unsplice this entry from the list
                int prev = prevs[slot];
                int next = nexts[slot];

                nexts[prev] = next;
                prevs[next] = prev;

                // Move this entry to be the new head
                nexts[slot] = head;
                nexts[tail] = slot;

                prevs[head] = slot;
                prevs[slot] = tail;
            }
        } else {
            // Just move the tail back one slot, making the entry the head
            tail = prevs[tail];
        }
    }

    public synchronized void clear() {
        Arrays.fill(softReferences, getNull());
    }

    @VisibleForTesting
    public synchronized boolean verify(int numStored) {
        final TIntSet visited = new TIntHashSet();

        int v = tail;

        for (int i = 0; i < nexts.length; ++i) {
            if (!visited.add(v)) {
                return false;
            }
            v = nexts[v];
        }

        if (v != tail) {
            return false;
        }

        if (visited.size() != numStored) {
            return false;
        }

        for (int i = 0; i < prevs.length; ++i) {
            if (!visited.remove(v)) {
                return false;
            }
            v = prevs[v];
        }

        return v == tail;
    }

    public interface Adapter<T> {

        SoftReference<T> getOwner(T t);

        int getSlot(T t);

        void setSlot(T t, int slot);
    }

    public interface Node<T extends Node<T>> {

        SoftReference<T> getOwner();

        int getSlot();

        void setSlot(int slot);

        class Impl<T extends Impl<T>> implements Node<T> {

            private final SoftReference<T> owner;
            private int slot;

            protected Impl() {
                // noinspection unchecked
                owner = new SoftReference<>((T) this);
                slot = -1;
            }

            @Override
            public SoftReference<T> getOwner() {
                return owner;
            }

            @Override
            public int getSlot() {
                return slot;
            }

            @Override
            public void setSlot(int slot) {
                this.slot = slot;
            }
        }

        class Adapter<T extends Node<T>> implements IntrusiveSoftLRU.Adapter<T> {

            private static final IntrusiveSoftLRU.Adapter<?> INSTANCE = new Adapter<>();

            public static <T extends Node<T>> IntrusiveSoftLRU.Adapter<T> getInstance() {
                // noinspection unchecked
                return (IntrusiveSoftLRU.Adapter<T>) INSTANCE;
            }

            @Override
            public SoftReference<T> getOwner(@NotNull T t) {
                return t.getOwner();
            }

            @Override
            public int getSlot(@NotNull T t) {
                return t.getSlot();
            }

            @Override
            public void setSlot(@NotNull T t, int slot) {
                t.setSlot(slot);
            }
        }
    }
}
