/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import gnu.trove.iterator.TLongLongIterator;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.util.datastructures.hash.HashMapLockFreeK1V1;
import io.deephaven.util.datastructures.hash.HashMapLockFreeK2V2;
import io.deephaven.util.datastructures.hash.HashMapLockFreeK4V4;
import io.deephaven.util.datastructures.hash.TNullableLongLongMap;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

/**
 * This is a lock-free implementation of a WritableRowRedirection. The rules for using this class are as follows.
 *
 * Users of this class fall into two roles: 1. Readers (snapshotters), of which there can be many. 2. Writers, of which
 * there can be only one.
 *
 * Responsibilities of Readers: 1. Before you start, read the LogicalClock, note its value, and also note whether it is
 * in the Idle or Update phase. 2. If you are in the "Idle" phase, then all your calls should be to get(). 3. If you are
 * in the "Update" phase, then all your calls should be to getPrev(). 4. You must never call a mutating operation like
 * put() or remove(). 5. When you are done reading all the data you wanted, read the LogicalClock again. If it has the
 * same value (both generation and phase) as when you started, then you can rely on the data you have read. Otherwise,
 * the data you have is garbage, and you need to throw it all away and try again at step 1.
 *
 * Responsibilities of the Writer: 1. There must be only one Writer. 2. The Writer controls the LogicalClock. Put
 * another way, unlike the Reader, the Writer does not have to worry about a LogicalClock transition that would
 * invalidate its work. 3. The Writer may call read operations (get() or getPrev()) at any time, i.e. regardless of
 * phase (Idle or Update), and they will always provide valid data. 4. The Writer must only call mutating operations
 * (like put() or remove()) during an Update phase. The Writer MUST NOT call these mutating operations during an Idle
 * phase. 5. The Writer has no special responsibility when transitioning the LogicalClock from Idle to Update. 6.
 * However, upon the transition from Update to Idle, the Writer does have an additional responsibility. Namely, the
 * writer must first transition the LogicalClock (from Update generation N to Idle generation N+1), then call our method
 * commitUpdates(). A logical place to do this might be the Terminal Listener.
 *
 * Rationale that this code implements correct lock-free behavior:
 *
 * Section I: The perspective of the Reader.
 *
 * There are three cases: 1. When the LogicalClock has been in the Idle phase for the entirety of "Reader
 * Responsibilities" steps 1-5 above. 2. When it has been in the Update phase for the entirety. 3. When it started in
 * one phase and ended in the other.
 *
 * We discuss these in reverse order.
 *
 * For #3, our only responsibility is to not crash, corrupt memory, or go into an infinite loop. The get() / getPrev()
 * methods don't do any memory writes (so they can't corrupt memory). The argument that they can't crash or get into an
 * infinite loop can only be made by looking at the code. I won't try to justify it here, but hopefully we can convince
 * ourselves that this is so.
 *
 * For #2, the Reader is only calling getPrev(), which only accesses 'baseline'. The Writer's last write to 'baseline'
 * happened before it set the logical clock to Update. LogicalClock being volatile means that these writes have been
 * 'released'. Meanwhile, the Reader's first read to 'baseline' happens after it read the Update state from the
 * LogicalClock (and therefore has 'acquire' semantics).
 *
 * #1 is the most complicated. For #1, the Reader is only calling get(), which accesses both 'baseline' and 'updates'.
 * Importantly, it consults 'updates' first, and only accesses 'baseline' for keys that are not in 'updates'. The
 * Writer's last write to 'updates' happened before it set the logical clock to Idle (thus these writes have been
 * released). Meanwhile, the Reader's first read from 'updates' happens after it read the Idle state from the
 * LogicalClock (and therefore has 'acquire' semantics). Calls to get(key) where key exists in the 'updates' map are
 * thus correct. We now concern ourselves with calls to get(key) where key does not exist in 'updates'.
 *
 * The Writer spends the first part of the Idle cycle busily copying data from 'updates' back into 'baseline'. These
 * updates have these properties: 1. It does not disturb entries whose keys are are not in 'updates'. 2. When it writes
 * to the buckets in 'baseline' it changes key slots from either 'deletedSlot' or 'emptySlot' to some new key NEWKEY. We
 * can argue that these writes do not affect the operation of the Reader, so it doesn't matter whether the Reader sees
 * them or not: a) Importantly, the Reader is never looking for NEWKEY, because the Reader would have satisfied any
 * search for NEWKEY from the 'updates' map, which it consulted first. b) If NEWKEY replaces a 'deletedSlot', then the
 * Reader will skip over it as it does its probe. Because NEWKEY != key, the Reader's logic is the same whether it seems
 * NEWKEY or deletedSlot. c) If NEWKEY replaces an 'emptySlot', then the Reader will probe further than it otherwise
 * would have, but this search will be ultimately futile, because it will eventually reach an emptySlot. 3. When it is
 * finally done copying values over, the Writer will write null to the keysAndValues array inside 'updates' (this is a
 * volatile write). Writer's last write to 'baseline' happened before it wrote that null. When Reader consults 'updates'
 * and finds a null there, it will also see all the writes made to 'baseline' (acquire semantics). 4. Writer may need to
 * do a rehash. If it does, it will prepare the hashed array off to the side and then write it to
 * 'baseline.keysAndValues' with a volatile write. When reader reads 'baseline.keysAndValues' (volatile read) it will
 * either see the old array or the fully-populated new one.
 *
 * Section II: The perspective of the Writer:
 *
 * From the Writer's own perspective, the data structure is always coherent, because the Writer is the only one writing
 * to it. The only considerations are the Writer's responsibilities to the Reader. They are outlined in the above
 * section "Responsibilities of the Writer". The one thing that needs to be explained further is what happens in
 * commitUpdates().
 *
 * In commitUpdates() we iterate over the 'updates' hashtable and insert entries into the 'baseline' hashtable. If we
 * need to rehash, we do so in a way that the Reader sees either the old buckets array or the new buckets array--we need
 * to avoid the case where it sees some intermediate array which is only partially populated. To make this simple, we
 * populate the new array off to the side and do a volatile write to store its reference. The Reader's next read of it
 * is a volatile read, and it will pick it up then.
 *
 * When commitUpdates() is done, it writes a null to the 'updates.keysAndValues'. At this point all writes to the
 * 'baseline' hashtable are finished as of the time of the write of the null. Next time the Reader reads this reference
 * and finds it null, this will be an acquire and all the values in 'baseline' will be visible.
 *
 * That takes care of the transition from Update to Idle. Regarding the transition from Idle to Update, the caller does
 * not have any special responsibility, but the first call to put() inside an Update generation causes a new
 * 'keysAndValues' array to be generated, which the Reader will start to see next time it looks.
 */
public class WritableRowRedirectionLockFree implements WritableRowRedirection {
    private static final float LOAD_FACTOR =
            (float) Configuration.getInstance().getDoubleWithDefault("RowRedirectionK4V4Impl.loadFactor", 0.5);
    /**
     * The special "key not found" value used in the 'baseline' map is -1. However, for the 'updates' map, the "key not
     * found" value is -2. This allows us to use the updates map to remember removals and account for them properly.
     */
    private static final long BASELINE_KEY_NOT_FOUND = -1L;
    static {
        Assert.eq(BASELINE_KEY_NOT_FOUND, "BASELINE_KEY_NOT_FOUND", RowSequence.NULL_ROW_KEY, "RowSet.NULL_ROW_KEY");
    }
    private static final long UPDATES_KEY_NOT_FOUND = -2L;

    /**
     * How things looked at the beginning of the most recent idle cycle.
     */
    private final TNullableLongLongMap baseline;
    /**
     * Updates that have happened since the start of the most recent idle cycle.
     */
    private TNullableLongLongMap updates;

    private UpdateCommitter<WritableRowRedirectionLockFree> updateCommitter;

    WritableRowRedirectionLockFree(TNullableLongLongMap map) {
        this.baseline = map;
        // Initially, baseline == updates (i.e. they point to the same object). They will continue to point to the same
        // object until the first terminal listener notification after prev tracking is turned on (via
        // startTrackingPrevValues()).
        this.updates = map;
        this.updateCommitter = null;
    }

    /**
     * Commits the 'updates' map into the 'baseline' map, then resets the 'updates' map to empty. The only caller should
     * be Writer@Idle, via the TerminalNotification.
     */
    private static void commitUpdates(@NotNull final WritableRowRedirectionLockFree instance) {
        // This only gets called by the UpdateCommitter, and only as a result of a terminal listener notification (which
        // in turn can only happen once prev tracking has been turned on). We copy updates to baseline and reset the
        // updates map.
        final TNullableLongLongMap updates = instance.updates;
        final TNullableLongLongMap baseline = instance.baseline;
        Assert.neq(baseline, "baseline", updates, "updates");
        for (final TLongLongIterator it = updates.iterator(); it.hasNext();) {
            it.advance();
            if (it.value() == BASELINE_KEY_NOT_FOUND) {
                baseline.remove(it.key());
            } else {
                baseline.put(it.key(), it.value());
            }
        }
        updates.resetToNull();
    }

    /**
     * Gets the current value. Works correctly for Readers@Idle and Writer@Update. Readers@Update should be calling
     * getPrev(); meanwhile Writer@Idle shouldn't be calling anything. The only Reader@Update who calls this should be
     * one where the clock transitioned from Idle to Update while they were already running. Such a Reader should, upon
     * eventually noticing that this transition happened, throw away their work and start again.
     */
    @Override
    public final long get(long outerRowKey) {
        if (outerRowKey == -1) {
            return BASELINE_KEY_NOT_FOUND;
        }
        final long result = updates.get(outerRowKey);
        if (result != UPDATES_KEY_NOT_FOUND) {
            // The prior value from updates is either some ordinary previous value, or BASELINE_KEY_NOT_FOUND.
            // In either case, return it to the caller.
            return result;
        }
        // There's no entry in 'updates' so we return the entry in 'baseline'.
        return baseline.get(outerRowKey);
    }

    /**
     * Gets the previous value. Works correctly for Readers@Update and Writer@Update. Readers@Idle should be calling
     * get(); meanwhile Writer@Idle shouldn't be calling anything. The only Reader@Idle who calls this should be one who
     * encountered a transition from Update to Idle while they were already running. Such a Reader should, upon
     * eventually noticing that this transition happened, throw away their work and start again.
     */
    @Override
    public final long getPrev(long outerRowKey) {
        if (outerRowKey == -1) {
            return BASELINE_KEY_NOT_FOUND;
        }
        return baseline.get(outerRowKey);
    }

    /**
     * Puts a value. Works correctly for Writer@Update. Readers should never call this; Writers@Idle shouldn't be
     * calling anything.
     */
    @Override
    public final long put(long outerRowKey, long innerRowKey) {
        if (innerRowKey == BASELINE_KEY_NOT_FOUND) {
            throw new IllegalArgumentException(innerRowKey + " is an illegal value");
        }
        return putImpl(outerRowKey, innerRowKey);
    }

    /**
     * Removes a key. Works correctly for Writer@Update. Readers should never call this; Writers@Idle shouldn't be
     * calling anything.
     */
    @Override
    public long remove(long outerRowKey) {
        // A removal is modeled simply as a put, into the updates table, of the baseline "key_not_found" value.
        return putImpl(outerRowKey, BASELINE_KEY_NOT_FOUND);
    }

    @Override
    public void removeAll(final RowSequence outerRowKeys) {
        if (updateCommitter != null) {
            updateCommitter.maybeActivate();
        }

        outerRowKeys.forAllRowKeys(key -> updates.put(key, BASELINE_KEY_NOT_FOUND));
    }

    @Override
    public void startTrackingPrevValues() {
        Assert.eqNull(updateCommitter, "updateCommitter");
        Assert.eq(baseline, "baseline", updates, "updates");
        updates = createUpdateMap();
        updateCommitter = new UpdateCommitter<>(this, WritableRowRedirectionLockFree::commitUpdates);
    }

    /**
     * Puts a value. Permits BASELINE_KEY_NOT_FOUND as a value.
     */
    private long putImpl(long key, long value) {
        if (updateCommitter != null) {
            updateCommitter.maybeActivate();
        }
        final long result = updates.put(key, value);
        // The prior value from updates is either some legit previous value, or BASELINE_KEY_NOT_FOUND.
        // In either case, return it to the caller.
        if (result != UPDATES_KEY_NOT_FOUND) {
            return result;
        }
        return baseline.get(key);
    }

    @Override
    public void fillFromChunk(
            @NotNull ChunkSink.FillFromContext context,
            @NotNull Chunk<? extends RowKeys> innerRowKeys,
            @NotNull RowSequence outerRowKeys) {
        if (updateCommitter != null) {
            updateCommitter.maybeActivate();
        }

        final MutableInt offset = new MutableInt();
        final LongChunk<? extends RowKeys> innerRowKeysTyped = innerRowKeys.asLongChunk();
        outerRowKeys.forAllRowKeys(outerRowKey -> {
            updates.put(outerRowKey, innerRowKeysTyped.get(offset.intValue()));
            offset.increment();
        });
    }

    @Override
    public void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends RowKeys> innerRowKeys,
            @NotNull final LongChunk<RowKeys> outerRowKeys) {
        if (updateCommitter != null) {
            updateCommitter.maybeActivate();
        }

        final LongChunk<? extends RowKeys> innerRowKeysTyped = innerRowKeys.asLongChunk();
        final int size = innerRowKeysTyped.size();
        for (int ki = 0; ki < size; ++ki) {
            updates.put(outerRowKeys.get(ki), innerRowKeysTyped.get(ki));
        }
    }

    private static final int hashBucketWidth = Configuration.getInstance()
            .getIntegerForClassWithDefault(WritableRowRedirectionLockFree.class, "hashBucketWidth", 1);

    @NotNull
    private static TNullableLongLongMap createUpdateMap() {
        return createMapWithCapacity(10, LOAD_FACTOR, UPDATES_KEY_NOT_FOUND);
    }

    @NotNull
    static TNullableLongLongMap createMapWithCapacity(int initialCapacity) {
        return createMapWithCapacity(initialCapacity, LOAD_FACTOR, BASELINE_KEY_NOT_FOUND);
    }

    @NotNull
    private static TNullableLongLongMap createMapWithCapacity(int initialCapacity, float loadFactor,
            long noEntryValue) {
        switch (hashBucketWidth) {
            case 1:
                return new HashMapLockFreeK1V1(initialCapacity, loadFactor, noEntryValue);
            case 2:
                return new HashMapLockFreeK2V2(initialCapacity, loadFactor, noEntryValue);
            case 4:
                return new HashMapLockFreeK4V4(initialCapacity, loadFactor, noEntryValue);
            default:
                throw new UnsupportedOperationException("Unsupported hashBucketWidth setting: " + hashBucketWidth);
        }
    }
}
