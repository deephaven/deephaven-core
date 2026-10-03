//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.updategraph.UpdateCommitter;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class ContiguousWritableRowRedirection implements WritableRowRedirection {
    private static final long UPDATES_KEY_NOT_FOUND = -2L;

    // The current state of the world.
    private long[] redirections;
    // how many entries in redirections are actually valid
    int size;
    // How things looked on the last clock tick
    private volatile Long2LongMap checkpoint;
    private UpdateCommitter<ContiguousWritableRowRedirection> updateCommitter;

    @SuppressWarnings("unused")
    public ContiguousWritableRowRedirection(int initialCapacity) {
        redirections = new long[initialCapacity];
        Arrays.fill(redirections, RowSequence.NULL_ROW_KEY);
        size = 0;
        checkpoint = null;
        updateCommitter = null;
    }

    public ContiguousWritableRowRedirection(long[] redirections) {
        this.redirections = redirections;
        size = redirections.length;
        checkpoint = null;
        updateCommitter = null;
    }

    @Override
    public long put(long outerRowKey, long innerRowKey) {
        Require.requirement(outerRowKey <= Integer.MAX_VALUE && outerRowKey >= 0,
                "key <= Integer.MAX_VALUE && key >= 0", outerRowKey, "key");
        ensureCapacity(outerRowKey);
        final long previous = redirections[(int) outerRowKey];
        if (previous == RowSequence.NULL_ROW_KEY) {
            size++;
        }
        redirections[(int) outerRowKey] = innerRowKey;

        if (previous != innerRowKey) {
            onRemove(outerRowKey, previous);
        }
        return previous;
    }

    private void ensureCapacity(final long maxOuterRowKey) {
        if (maxOuterRowKey >= redirections.length) {
            final long[] newRedirections = new long[Math.max((int) maxOuterRowKey + 100, redirections.length * 2)];
            System.arraycopy(redirections, 0, newRedirections, 0, redirections.length);
            Arrays.fill(newRedirections, redirections.length, newRedirections.length, RowSequence.NULL_ROW_KEY);
            redirections = newRedirections;
        }
    }

    @Override
    public void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends RowKeys> innerRowKeys,
            @NotNull final LongChunk<RowKeys> outerRowKeys) {
        final LongChunk<? extends RowKeys> innerRowKeysTyped = innerRowKeys.asLongChunk();
        final int count = outerRowKeys.size();
        if (count == 0) {
            return;
        }
        long minOuterRowKey = Long.MAX_VALUE;
        long maxOuterRowKey = Long.MIN_VALUE;
        for (int ii = 0; ii < count; ++ii) {
            final long outerRowKey = outerRowKeys.get(ii);
            minOuterRowKey = Math.min(minOuterRowKey, outerRowKey);
            maxOuterRowKey = Math.max(maxOuterRowKey, outerRowKey);
        }
        Require.requirement(maxOuterRowKey <= Integer.MAX_VALUE && minOuterRowKey >= 0,
                "maxOuterRowKey <= Integer.MAX_VALUE && minOuterRowKey >= 0");
        ensureCapacity(maxOuterRowKey);

        // a NULL_ROW_KEY inner row key removes the mapping, and size counts the mapped outer row keys
        int sizeDelta = 0;
        if (updateCommitter == null) {
            for (int ii = 0; ii < count; ++ii) {
                final int outerRowKey = (int) outerRowKeys.get(ii);
                final long innerRowKey = innerRowKeysTyped.get(ii);
                final long previous = redirections[outerRowKey];
                redirections[outerRowKey] = innerRowKey;
                sizeDelta += (previous == RowSequence.NULL_ROW_KEY ? 1 : 0)
                        - (innerRowKey == RowSequence.NULL_ROW_KEY ? 1 : 0);
            }
        } else {
            synchronized (this) {
                updateCommitter.maybeActivate();
                for (int ii = 0; ii < count; ++ii) {
                    final int outerRowKey = (int) outerRowKeys.get(ii);
                    final long innerRowKey = innerRowKeysTyped.get(ii);
                    final long previous = redirections[outerRowKey];
                    redirections[outerRowKey] = innerRowKey;
                    sizeDelta += (previous == RowSequence.NULL_ROW_KEY ? 1 : 0)
                            - (innerRowKey == RowSequence.NULL_ROW_KEY ? 1 : 0);
                    if (previous != innerRowKey) {
                        checkpoint.putIfAbsent(outerRowKey, previous);
                    }
                }
            }
        }
        size += sizeDelta;
    }

    private synchronized void onRemove(long key, long previous) {
        if (updateCommitter == null) {
            return;
        }
        updateCommitter.maybeActivate();
        checkpoint.putIfAbsent(key, previous);
    }

    @Override
    public long get(long outerRowKey) {
        if (outerRowKey < 0 || outerRowKey >= redirections.length) {
            return RowSequence.NULL_ROW_KEY;
        }
        return redirections[(int) outerRowKey];
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        innerRowKeysTyped.setSize(0);
        outerRowKeys.forAllRowKeyRanges((final long start, final long end) -> {
            for (long v = start; v <= end; ++v) {
                innerRowKeysTyped.add(redirections[(int) v]);
            }
        });
    }

    @Override
    public long getPrev(long outerRowKey) {
        if (checkpoint != null) {
            synchronized (this) {
                final long result = checkpoint.get(outerRowKey);
                if (result != UPDATES_KEY_NOT_FOUND) {
                    return result;
                }
            }
        }
        return get(outerRowKey);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        if (checkpoint == null) {
            fillChunk(fillContext, innerRowKeys, outerRowKeys);
            return;
        }

        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        innerRowKeysTyped.setSize(0);
        synchronized (this) {
            outerRowKeys.forAllRowKeyRanges((final long start, final long end) -> {
                for (long v = start; v <= end; ++v) {
                    long result = checkpoint.get(v);
                    if (result == UPDATES_KEY_NOT_FOUND) {
                        result = redirections[(int) v];
                    }
                    innerRowKeysTyped.add(result);
                }
            });
        }
    }

    @Override
    public long remove(long outerRowKey) {
        final long removed = redirections[(int) outerRowKey];
        redirections[(int) outerRowKey] = RowSequence.NULL_ROW_KEY;
        if (removed != RowSequence.NULL_ROW_KEY) {
            size--;
            onRemove(outerRowKey, removed);
        }
        return removed;
    }

    public synchronized void startTrackingPrevValues() {
        Assert.eqNull(updateCommitter, "updateCommitter");
        checkpoint = new Long2LongOpenHashMap(Math.min(size, 1024 * 1024), 0.75f);
        checkpoint.defaultReturnValue(UPDATES_KEY_NOT_FOUND);
        updateCommitter = new UpdateCommitter<>(this,
                ExecutionContext.getContext().getUpdateGraph(),
                ContiguousWritableRowRedirection::commitUpdates);
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
            if (redirections[ii] == RowSequence.NULL_ROW_KEY) {
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
