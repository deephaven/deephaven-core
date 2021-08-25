/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;

import gnu.trove.map.hash.TLongLongHashMap;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class ContiguousRedirectionIndexImpl implements RedirectionIndex {
    private static final long UPDATES_KEY_NOT_FOUND = -2L;

    // The current state of the world.
    private long[] redirections;
    // how many entries in redirections are actually valid
    int size;
    // How things looked on the last clock tick
    private volatile TLongLongHashMap checkpoint;
    private UpdateCommitter<ContiguousRedirectionIndexImpl> updateCommitter;

    @SuppressWarnings("unused")
    public ContiguousRedirectionIndexImpl(int initialCapacity) {
        redirections = new long[initialCapacity];
        Arrays.fill(redirections, Index.NULL_KEY);
        size = 0;
        checkpoint = null;
        updateCommitter = null;
    }

    public ContiguousRedirectionIndexImpl(long[] redirections) {
        this.redirections = redirections;
        size = redirections.length;
        checkpoint = null;
        updateCommitter = null;
    }

    @Override
    public long put(long key, long index) {
        Require.requirement(key <= Integer.MAX_VALUE && key >= 0, "key <= Integer.MAX_VALUE && key >= 0", key, "key");
        if (key >= redirections.length) {
            final long[] newRedirections = new long[Math.max((int) key + 100, redirections.length * 2)];
            System.arraycopy(redirections, 0, newRedirections, 0, redirections.length);
            Arrays.fill(newRedirections, redirections.length, newRedirections.length, Index.NULL_KEY);
            redirections = newRedirections;
        }
        final long previous = redirections[(int) key];
        if (previous == Index.NULL_KEY) {
            size++;
        }
        redirections[(int) key] = index;

        if (previous != index) {
            onRemove(key, previous);
        }
        return previous;
    }

    private synchronized void onRemove(long key, long previous) {
        if (updateCommitter == null) {
            return;
        }
        updateCommitter.maybeActivate();
        checkpoint.putIfAbsent(key, previous);
    }

    @Override
    public long get(long key) {
        if (key < 0 || key >= redirections.length) {
            return Index.NULL_KEY;
        }
        return redirections[(int) key];
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableLongChunk<Attributes.KeyIndices> mappedKeysOut,
            @NotNull final OrderedKeys keysToMap) {
        mappedKeysOut.setSize(0);
        keysToMap.forAllLongRanges((final long start, final long end) -> {
            for (long v = start; v <= end; ++v) {
                mappedKeysOut.add(redirections[(int) v]);
            }
        });
    }

    @Override
    public long getPrev(long key) {
        if (checkpoint != null) {
            synchronized (this) {
                final long result = checkpoint.get(key);
                if (result != UPDATES_KEY_NOT_FOUND) {
                    return result;
                }
            }
        }
        return get(key);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableLongChunk<Attributes.KeyIndices> mappedKeysOut,
            @NotNull final OrderedKeys keysToMap) {
        if (checkpoint == null) {
            fillChunk(fillContext, mappedKeysOut, keysToMap);
            return;
        }

        synchronized (this) {
            mappedKeysOut.setSize(0);
            keysToMap.forAllLongRanges((final long start, final long end) -> {
                for (long v = start; v <= end; ++v) {
                    long result = checkpoint.get(v);
                    if (result == UPDATES_KEY_NOT_FOUND) {
                        result = redirections[(int) v];
                    }
                    mappedKeysOut.add(result);
                }
            });
        }
    }

    @Override
    public long remove(long leftIndex) {
        final long removed = redirections[(int) leftIndex];
        redirections[(int) leftIndex] = Index.NULL_KEY;
        if (removed != Index.NULL_KEY) {
            size--;
            onRemove(leftIndex, removed);
        }
        return removed;
    }

    public synchronized void startTrackingPrevValues() {
        Assert.eqNull(updateCommitter, "updateCommitter");
        checkpoint =
                new TLongLongHashMap(Math.min(size, 1024 * 1024), 0.75f, UPDATES_KEY_NOT_FOUND, UPDATES_KEY_NOT_FOUND);
        updateCommitter = new UpdateCommitter<>(this, ContiguousRedirectionIndexImpl::commitUpdates);
    }

    private synchronized void commitUpdates() {
        checkpoint.clear();
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("{ size = ").append(size);
        int printed = 0;
        for (int ii = 0; printed < size && ii < redirections.length; ii++) {
            if (redirections[ii] == Index.NULL_KEY) {
                builder.append(", nil");
            } else {
                builder.append(", ").append(ii).append("=").append(redirections[ii]);
                printed++;
            }
        }
        if (printed != size) {
            builder.append(" ERROR, printed=").append(printed);
        }
        builder.append("}");
        return builder.toString();
    }
}
