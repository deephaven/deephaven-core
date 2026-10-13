//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.UpdateCoalescer;
import io.deephaven.extensions.barrage.chunk.BarrageCopyKernel;
import io.deephaven.extensions.barrage.chunk.BarrageCopyKernel.Runs;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.deephaven.server.barrage.BarrageMessageProducer.DELTA_CHUNK_SIZE;

/**
 * One update graph cycle's worth of change to a table, as recorded by a {@link BarrageMessageProducer} for the
 * subscribers it had at the time: the {@link TableUpdate}, the rows whose data was recorded, and that data as chunks. A
 * producer queues these until its subscribers' update interval elapses, then coalesces the queue into one
 * {@link BarrageMessage}; {@link #coalesce} is that algorithm. Compaction applies it early, in place on the pending
 * queue, so the queue does not grow with the number of cycles per interval.
 */
final class BarrageMessageDelta implements SafeCloseable {
    /**
     * The producer's subscription generation this delta was recorded under, which fixes the viewport and column set its
     * recorded rows describe. Compaction only folds deltas of one generation together, because the propagation job may
     * need to send different generations to different subscribers. Propagation itself is not so restricted: when
     * subscriptions are only removed, the producer promotes the narrower state at once and the deltas recorded before
     * and after that promotion are coalesced into one message, which is sound because the remaining subscribers' needs
     * are a subset of what both generations recorded.
     */
    final long generation;
    /** Clock step of this delta; a compacted delta spans the run it replaced. */
    final long firstStep;
    final long lastStep;
    final TableUpdate update;
    final WritableRowSet recordedAdds;
    /** Union over {@link #perColumnRecordedMods}, so {@code isNonempty()} still means "carries modified data". */
    final RowSet recordedMods;
    /**
     * Per-column recorded modified rows. Null for a delta recorded from a single update graph cycle, which shares one
     * {@link #recordedMods} set across every column it modified.
     *
     * <p>
     * Coalescing a run breaks that sharing, because different cycles modify different columns: if one cycle modifies
     * column A on rows 10 and 11 and the next modifies column B on rows 20 and 21, no single set describes both. So a
     * compacted delta needs one row set per column, and {@link #recordedMods} becomes their union so that "does this
     * carry modified data" still works. Non-null exactly for {@link #modifiedColumns}. Columns that shared a mapping at
     * compaction share one set; {@link #close} closes each distinct set once.
     *
     * <p>
     * Indexed by the table's column index, a column's position in the table definition and in the producer's chunk
     * sources, as {@link #addChunks} and {@link #modChunks} are, so the array spans every column of the table whether
     * subscribed or not.
     */
    @Nullable
    final RowSet[] perColumnRecordedMods;
    final BitSet subscribedColumns;
    final BitSet modifiedColumns;

    /**
     * Per-column chunk storage for added rows, indexed by the table's column index so that the outer array spans every
     * column of the table whether subscribed or not. {@code addChunks[columnIndex]} is an array of {@link WritableChunk
     * WritableChunks}, each holding exactly {@code DELTA_CHUNK_SIZE} rows except the last; the copy kernel locates a
     * row by dividing its position by {@code DELTA_CHUNK_SIZE}, so every delta, recorded or compacted, must keep that
     * layout. Null for columns not in {@code subscribedColumns}. Slots may be set to null after detachment for
     * zero-copy transfer.
     */
    final WritableChunk<Values>[][] addChunks;

    /**
     * Per-column chunk storage for modified rows, laid out like {@link #addChunks}. Null for columns not in
     * {@code modifiedColumns}. Slots may be set to null after detachment for zero-copy transfer.
     */
    final WritableChunk<Values>[][] modChunks;

    /**
     * Heap footprint of {@link #addChunks} and {@link #modChunks} as recorded. Chunks later detached for zero-copy
     * transfer are still counted; that happens only as the pending list is being discarded.
     */
    final long chunkBytes;

    /**
     * @param update this delta takes ownership of the update; a caller that does not own it must pass a
     *        {@link TableUpdateImpl#copy(TableUpdate) copy}
     * @param perColumnRecordedMods null when every modified column shares {@code recordedMods}
     */
    BarrageMessageDelta(final long generation, final long firstStep, final long lastStep,
            final TableUpdate update,
            final WritableRowSet recordedAdds, final RowSet recordedMods,
            @Nullable final RowSet[] perColumnRecordedMods,
            final BitSet subscribedColumns, final BitSet modifiedColumns,
            final WritableChunk<Values>[][] addChunks,
            final WritableChunk<Values>[][] modChunks) {
        this.generation = generation;
        this.firstStep = firstStep;
        this.lastStep = lastStep;
        this.update = update;
        this.recordedAdds = recordedAdds;
        this.recordedMods = recordedMods;
        this.perColumnRecordedMods = perColumnRecordedMods;
        this.subscribedColumns = subscribedColumns;
        this.modifiedColumns = modifiedColumns;
        this.addChunks = addChunks;
        this.modChunks = modChunks;
        this.chunkBytes = chunkArrayBytes(addChunks) + chunkArrayBytes(modChunks);
    }

    /**
     * Uses chunk capacity rather than size: the whole array is allocated however little of it is filled, and pooled
     * chunks round up to a power of two.
     */
    static long chunkArrayBytes(final WritableChunk<Values>[][] chunkArrays) {
        if (chunkArrays == null) {
            return 0;
        }
        long bytes = 0;
        for (final WritableChunk<Values>[] chunks : chunkArrays) {
            if (chunks == null) {
                continue;
            }
            for (final WritableChunk<Values> chunk : chunks) {
                if (chunk != null) {
                    bytes += (long) chunk.capacity() * chunk.elementBytes();
                }
            }
        }
        return bytes;
    }

    /**
     * Reduce a run of deltas to one delta describing the same net change: the rows that survive the run, and the
     * recorded data for those rows. Rows modified repeatedly are kept once, with their last value; rows added and then
     * removed within the run are not kept at all.
     *
     * <p>
     * This is the only place a run of deltas is coalesced. Propagation calls it to build the message for a run, and
     * compaction calls it to replace a run in place; the result is the same delta either way. The surviving data is
     * copied into fresh chunks, and the run's deltas are left intact. If it fails, everything it allocated is released.
     *
     * @param deltas the run, in recording order, at least two deltas; still owned by the caller
     * @param baseRowSet the propagated row set as of immediately before the run
     * @param chunkSources the producer's column sources, for each column's chunk type
     * @return a new delta, which the caller owns
     */
    static BarrageMessageDelta coalesce(final List<BarrageMessageDelta> deltas, final RowSet baseRowSet,
            final ChunkSource.WithPrev<Values>[] chunkSources) {
        final int numDeltas = deltas.size();
        if (numDeltas < 2) {
            throw new IllegalArgumentException(
                    "coalesce requires at least two deltas; a run of one is already coalesced");
        }

        // Paranoid cleanup on exception.
        final TableUpdate update = coalesceUpdates(deltas, baseRowSet);
        try {
            final RunSummary run = RunSummary.of(deltas);
            try {
                return build(deltas, chunkSources, update, run);
            } catch (final Throwable err) {
                run.added.close();
                throw err;
            }
        } catch (final Throwable err) {
            update.release();
            throw err;
        }
    }

    /**
     * Copy the surviving data into fresh chunks and assemble the result; takes ownership of {@code update} and
     * {@code run.added}.
     */
    private static BarrageMessageDelta build(final List<BarrageMessageDelta> deltas,
            final ChunkSource.WithPrev<Values>[] chunkSources, final TableUpdate update, final RunSummary run) {
        final int numDeltas = deltas.size();
        final int numColumns = chunkSources.length;

        // Columns the run modified upstream are candidates; only those with surviving recorded rows keep data.
        // ALL is a sentinel with no columns to enumerate; spell it out so the set can be iterated and intersected
        // below.
        if (update.modifiedColumnSet() == ModifiedColumnSet.ALL) {
            run.modColumnSet.set(0, numColumns);
        } else {
            run.modColumnSet.or(update.modifiedColumnSet().extractAsBitSet());
        }
        // Keep only columns every delta recorded: a subscriber that left mid-run may have been the only one subscribed
        // to a column, and the deltas recorded don't include data for it.
        run.modColumnSet.and(run.columns);
        final BitSet addColumnSet = run.added.isEmpty() ? new BitSet() : run.columns;

        // noinspection unchecked
        final WritableChunk<Values>[][] addChunks = new WritableChunk[numColumns][];
        // noinspection unchecked
        final WritableChunk<Values>[][] modChunks = new WritableChunk[numColumns][];
        final RowSet[] perColumnRecordedMods = new RowSet[numColumns];
        final BitSet modifiedColumns = new BitSet();
        final WritableRowSet recordedMods = RowSetFactory.empty();
        try (final ColumnMappingCache mappings = new ColumnMappingCache(deltas, run.added, update.added())) {
            for (int ci = addColumnSet.nextSetBit(0); ci >= 0; ci = addColumnSet.nextSetBit(ci + 1)) {
                final ColumnMapping mapping = mappings.getOrCompute(ci);
                addChunks[ci] = copyColumn(mapping.addedRuns, chunkSources[ci].getChunkType(), deltas, ci);
            }

            for (int ci = run.modColumnSet.nextSetBit(0); ci >= 0; ci = run.modColumnSet.nextSetBit(ci + 1)) {
                final ColumnMapping mapping = mappings.getOrCompute(ci);
                if (mapping.recordedMods.isEmpty()) {
                    // modified upstream, but nothing we recorded survived the run
                    continue;
                }
                modChunks[ci] = copyColumn(mapping.modifiedRuns, chunkSources[ci].getChunkType(), deltas, ci);
                perColumnRecordedMods[ci] = mapping.recordedMods;
                modifiedColumns.set(ci);
                final RowSet extracted = mappings.extractRecordedMods(mapping);
                if (extracted != null) {
                    recordedMods.insert(extracted);
                }
            }

            // The result is stamped with the run's first generation: compaction runs are one generation, and the only
            // multi-generation run, propagation across a removal-only promotion, is sent at once and never queued.
            return new BarrageMessageDelta(deltas.get(0).generation,
                    deltas.get(0).firstStep, deltas.get(numDeltas - 1).lastStep,
                    update, run.added, recordedMods, perColumnRecordedMods,
                    (BitSet) run.columns.clone(), modifiedColumns, addChunks, modChunks);
        } catch (final Throwable err) {
            // the cache has already closed the mappings it still owned
            closeChunkArrays(addChunks);
            closeChunkArrays(modChunks);
            closeDistinct(perColumnRecordedMods);
            recordedMods.close();
            throw err;
        }
    }

    /** The run's updates as one, starting from the row set as of immediately before it. The caller owns the result. */
    private static TableUpdate coalesceUpdates(final List<BarrageMessageDelta> deltas, final RowSet baseRowSet) {
        try (final UpdateCoalescer coalescer = new UpdateCoalescer(baseRowSet, deltas.get(0).update)) {
            for (int i = 1; i < deltas.size(); ++i) {
                coalescer.update(deltas.get(i).update);
            }
            // Coalescing hands the row sets to the update, so closing the coalescer now releases only what it kept
            // for itself; a failure before this point releases all of them.
            return coalescer.coalesce();
        }
    }

    /**
     * One pass over a run of deltas, in order, collecting what the coalesced result needs before any data is copied:
     * the recorded adds that survive to the end of the run, the columns every delta recorded (the most the result can
     * carry), and the columns some delta recorded modified data for.
     */
    private static final class RunSummary {
        /** Surviving recorded adds, in the key space at the end of the run; owned by the caller of {@link #of}. */
        final WritableRowSet added = RowSetFactory.empty();
        /**
         * The columns every delta in the run recorded and stored. Within a single generation this is simply the shared
         * column set. Across the one boundary propagation does coalesce over (subscriber removal and it was the only
         * requestor for a column), the later deltas recorded a subset of the previous deltas' columns. The remaining
         * subscribers need only that subset, so the intersection loses nothing anyone still cares about we won't ask a
         * delta for a column it did not record.
         */
        final BitSet columns = new BitSet();
        final BitSet modColumnSet = new BitSet();

        static RunSummary of(final List<BarrageMessageDelta> deltas) {
            final RunSummary result = new RunSummary();
            try {
                result.columns.or(deltas.get(0).subscribedColumns);
                for (final BarrageMessageDelta delta : deltas) {
                    // Keep only the columns that are still subscribed by all deltas.
                    result.columns.and(delta.subscribedColumns);
                    // No reason to keep added rows that were removed by this delta.
                    result.added.remove(delta.update.removed());
                    // Apply any shifts to the surviving added rows.
                    delta.update.shifted().apply(result.added);
                    // Add the newly recorded adds to the surviving added rows.
                    result.added.insert(delta.recordedAdds);
                    if (delta.recordedMods.isNonempty()) {
                        // Include the columns that were modified in this delta.
                        result.modColumnSet.or(delta.modifiedColumns);
                    }
                }
                return result;
            } catch (final Throwable err) {
                result.added.close();
                throw err;
            }
        }
    }

    /**
     * The four passes of the mapping over one delta, in the order they run: which side of the output is being filled,
     * and which side of the delta supplies it.
     */
    private enum MappingPass {
        /** Output adds from the delta's recorded adds. */
        ADDS_FROM_RECORDED_ADDS(true, true),
        /** Output mods from the delta's recorded adds: a row scoped into a viewport is recorded as an add. */
        MODS_FROM_RECORDED_ADDS(false, true),
        /** Output adds from the delta's recorded mods: a row added earlier in the run and modified in this delta. */
        ADDS_FROM_RECORDED_MODS(true, false),
        /** Output mods from the delta's recorded mods. */
        MODS_FROM_RECORDED_MODS(false, false);

        final boolean fillsAdds;
        final boolean fromRecordedAdds;

        MappingPass(final boolean fillsAdds, final boolean fromRecordedAdds) {
            this.fillsAdds = fillsAdds;
            this.fromRecordedAdds = fromRecordedAdds;
        }
    }

    /**
     * Where each surviving row of one column comes from. Adds must carry data for every subscribed column, but a
     * modification is recorded only for the columns it touched, so the mapping is per column; columns that the same
     * deltas modified with the same recorded rows share one (see {@link MappingKey}).
     */
    private static final class ColumnMapping {
        /**
         * Surviving modified rows for this column; ownership passes to the compacted delta via
         * {@link ColumnMappingCache#extractRecordedMods}.
         */
        final WritableRowSet recordedMods = RowSetFactory.empty();
        final Runs addedRuns = new Runs();
        final Runs modifiedRuns = new Runs();

        /**
         * @param deltasThatModify which deltas of the run modified this column
         * @param localAdded the run's surviving recorded adds, which every column shares
         * @param coalescedAdded the coalesced update's added rows; a row added within the run is not also modified
         */
        void compute(final List<BarrageMessageDelta> deltas, final int columnIndex, final BitSet deltasThatModify,
                final RowSet localAdded, final RowSet coalescedAdded) {
            final int numDeltas = deltas.size();
            for (int i = 0; i < numDeltas; ++i) {
                final BarrageMessageDelta delta = deltas.get(i);
                recordedMods.remove(delta.update.removed());
                delta.update.shifted().apply(recordedMods);
                if (deltasThatModify.get(i)) {
                    recordedMods.insert(delta.getRecordedMods(columnIndex));
                }
            }
            recordedMods.remove(coalescedAdded);
            final long addedRows = localAdded.size();
            final long modifiedRows = recordedMods.size();

            // Walk the run latest first so each surviving row takes its value from the last delta that recorded it.
            // "Remaining" holds the surviving rows not yet sourced, as keys in the current delta's key space;
            // "unfilled" holds their output positions. Both shrink in step, so the k-th of one is the k-th of the
            // other.
            try (final WritableRowSet addedRemaining = localAdded.copy();
                    final WritableRowSet modifiedRemaining = recordedMods.copy();
                    final WritableRowSet unfilledAdds = RowSetFactory.flat(addedRows);
                    final WritableRowSet unfilledMods = RowSetFactory.flat(modifiedRows)) {
                for (int i = numDeltas - 1; i >= 0; --i) {
                    if (addedRemaining.isEmpty() && modifiedRemaining.isEmpty()) {
                        break;
                    }
                    final BarrageMessageDelta delta = deltas.get(i);
                    for (final MappingPass pass : MappingPass.values()) {
                        if (!pass.fromRecordedAdds && !deltasThatModify.get(i)) {
                            continue;
                        }
                        mapPass(pass, delta, i, columnIndex,
                                pass.fillsAdds ? addedRemaining : modifiedRemaining,
                                pass.fillsAdds ? unfilledAdds : unfilledMods,
                                pass.fillsAdds ? addedRuns : modifiedRuns);
                    }
                    delta.update.shifted().unapply(addedRemaining);
                    delta.update.shifted().unapply(modifiedRemaining);
                }

                if (!unfilledAdds.isEmpty()) {
                    Assert.assertion(false, "Error: added:" + coalescedAdded + " unfilled:" + unfilledAdds
                            + " missing:" + coalescedAdded.subSetForPositions(unfilledAdds));
                }
                Assert.eq(unfilledMods.size(), "unfilledMods.size()", 0);
            }
        }

        /**
         * Take every row of {@code remaining} that {@code delta} recorded on the pass's side from that delta: remove it
         * from {@code remaining} and its output position from {@code unfilled}, and append the resulting runs.
         */
        private static void mapPass(final MappingPass pass, final BarrageMessageDelta delta, final int deltaIndex,
                final int columnIndex, final WritableRowSet remaining, final WritableRowSet unfilled, final Runs out) {
            final RowSet deltaRecorded =
                    pass.fromRecordedAdds ? delta.recordedAdds : delta.getRecordedMods(columnIndex);
            try (final RowSet recorded = remaining.intersect(deltaRecorded);
                    final WritableRowSet originPositions = deltaRecorded.invert(recorded);
                    final RowSet destinationsInPosSpace = remaining.invert(recorded);
                    final RowSet rowsToFill = unfilled.subSetForPositions(destinationsInPosSpace)) {
                // originPositions are positions within the delta's chunks on this side; tag them with the delta and
                // side
                originPositions.shiftInPlace(BarrageCopyKernel.originOffset(deltaIndex, !pass.fromRecordedAdds));
                remaining.remove(recorded);
                unfilled.remove(rowsToFill);
                appendRuns(out, rowsToFill, originPositions);
            }
        }

        /**
         * Pair {@code destinations} with {@code encodedOrigins}, both ascending and of equal size, position by
         * position, appending to {@code out} one run for each stretch over which both are contiguous.
         */
        private static void appendRuns(final Runs out, final RowSet destinations, final RowSet encodedOrigins) {
            Assert.eq(destinations.size(), "destinations.size()", encodedOrigins.size(), "encodedOrigins.size()");
            try (final RowSet.RangeIterator dit = destinations.rangeIterator();
                    final RowSet.RangeIterator oit = encodedOrigins.rangeIterator()) {
                // Walk the two range sequences in lock step. The k-th destination pairs with the k-th origin, so the
                // pairing is one merge pass: at any moment we are somewhere inside one destination range and one
                // origin range, and the next run is as long as whichever of the two has fewer positions left.
                long destPos = 0;
                long destEnd = -1; // exhausted until the first range is loaded
                long originPos = 0;
                long originEnd = -1;
                while (true) {
                    // Advance to the next destination range once the current one is used up; running out ends the
                    // pass.
                    if (destPos > destEnd) {
                        if (!dit.hasNext()) {
                            break;
                        }
                        dit.next();
                        destPos = dit.currentRangeStart();
                        destEnd = dit.currentRangeEnd();
                    }
                    // Same for the origins. Equal sizes mean an origin range is always there when a destination is.
                    if (originPos > originEnd) {
                        Assert.assertion(oit.hasNext(), "oit.hasNext()");
                        oit.next();
                        originPos = oit.currentRangeStart();
                        originEnd = oit.currentRangeEnd();
                    }
                    // One run: contiguous on both sides until the nearer range boundary. Whichever side ends first is
                    // exhausted after this, and the top of the loop reloads it; the other side continues from where it
                    // stopped, so a range on one side may span several runs.
                    final long length = Math.min(destEnd - destPos, originEnd - originPos) + 1;
                    out.add(destPos, originPos, length);
                    destPos += length;
                    originPos += length;
                }
            }
        }
    }

    /**
     * The per-column mappings of one coalesce, shared between columns with the same {@link MappingKey}. Owns every
     * mapping's {@link ColumnMapping#recordedMods} until {@link #extractRecordedMods extracted}; {@link #close}
     * releases the rest.
     */
    private static final class ColumnMappingCache implements SafeCloseable {
        private final List<BarrageMessageDelta> deltas;
        private final RowSet localAdded;
        private final RowSet coalescedAdded;
        private final Map<MappingKey, ColumnMapping> byKey = new HashMap<>();
        private final List<ColumnMapping> all = new ArrayList<>();
        private final Set<ColumnMapping> extracted = Collections.newSetFromMap(new IdentityHashMap<>());

        ColumnMappingCache(final List<BarrageMessageDelta> deltas, final RowSet localAdded,
                final RowSet coalescedAdded) {
            this.deltas = deltas;
            this.localAdded = localAdded;
            this.coalescedAdded = coalescedAdded;
        }

        /** The mapping for {@code columnIndex}, computed on the first request for its {@link MappingKey}. */
        ColumnMapping getOrCompute(final int columnIndex) {
            final MappingKey key = MappingKey.forColumn(deltas, columnIndex);
            ColumnMapping mapping = byKey.get(key);
            if (mapping == null) {
                mapping = new ColumnMapping();
                // registered before it is computed, so close() covers a failure part way through
                all.add(mapping);
                byKey.put(key, mapping);
                mapping.compute(deltas, columnIndex, key.deltasThatModify, localAdded, coalescedAdded);
            }
            return mapping;
        }

        /**
         * Pass ownership of the mapping's {@link ColumnMapping#recordedMods} to the caller. Columns that share a
         * mapping share that one row set, so the transfer happens once: the first call returns it, later calls return
         * null, and {@link #close} leaves an extracted set alone because the caller now owns it.
         *
         * @return the row set, or null if it was already extracted for a column that shares this mapping
         */
        @Nullable
        RowSet extractRecordedMods(final ColumnMapping mapping) {
            return extracted.add(mapping) ? mapping.recordedMods : null;
        }

        @Override
        public void close() {
            for (final ColumnMapping mapping : all) {
                if (!extracted.contains(mapping)) {
                    // Close the rowsets that we have not transferred.
                    mapping.recordedMods.close();
                }
            }
        }
    }

    /**
     * Cache key for {@link ColumnMappingCache}: which deltas of the run modify the column, and for each of those that
     * is itself compacted, the identity of the row set it recorded for the column.
     *
     * <p>
     * Two columns may share a mapping only if every contributing delta recorded the same modified rows for both. A
     * recorded delta always did, so for it the pattern of which deltas modify the column is enough. A compacted delta
     * keeps one set per column but hands the same set to every column that shared a mapping when it was built, so the
     * identity of those sets tells columns that shared before, and go on sharing, from columns that diverged.
     */
    private static final class MappingKey {
        final BitSet deltasThatModify;
        private final RowSet[] compactedMods;
        private final int hash;

        static MappingKey forColumn(final List<BarrageMessageDelta> deltas, final int columnIndex) {
            final int numDeltas = deltas.size();
            final BitSet deltasThatModify = new BitSet();
            final RowSet[] compactedMods = new RowSet[numDeltas];
            for (int i = 0; i < numDeltas; ++i) {
                final BarrageMessageDelta delta = deltas.get(i);
                if (delta.modifiedColumns.get(columnIndex)) {
                    deltasThatModify.set(i);
                    if (delta.perColumnRecordedMods != null) {
                        compactedMods[i] = delta.perColumnRecordedMods[columnIndex];
                    }
                }
            }
            return new MappingKey(deltasThatModify, compactedMods);
        }

        private MappingKey(final BitSet deltasThatModify, final RowSet[] compactedMods) {
            this.deltasThatModify = deltasThatModify;
            this.compactedMods = compactedMods;
            int h = deltasThatModify.hashCode();
            for (final RowSet mods : compactedMods) {
                h = 31 * h + System.identityHashCode(mods);
            }
            this.hash = h;
        }

        @Override
        public boolean equals(final Object other) {
            if (!(other instanceof MappingKey)) {
                return false;
            }
            final MappingKey that = (MappingKey) other;
            if (hash != that.hash || !deltasThatModify.equals(that.deltasThatModify)) {
                return false;
            }
            for (int i = 0; i < compactedMods.length; ++i) {
                if (compactedMods[i] != that.compactedMods[i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    /**
     * Build one column's output chunks for the rows {@code runs} accounts for. Output chunks are cut at
     * {@link BarrageMessageProducer#DELTA_CHUNK_SIZE}, which is not a preference: the copy kernel and later compactions
     * locate a row by dividing its position by that constant, so every delta must keep that layout.
     *
     * <p>
     * How the data moves is the typed {@link BarrageCopyKernel}'s decision; this method only allocates the output and
     * hands over the runs alongside the chunks they name.
     *
     * @return the output chunks, or null if there are no rows; nothing is left allocated if the copy fails
     */
    private static WritableChunk<Values>[] copyColumn(final Runs runs, final ChunkType chunkType,
            final List<BarrageMessageDelta> deltas, final int columnIndex) {
        final long totalRows = runs.totalRows();
        final int numChunks = LongSizedDataStructure.intSize("BarrageMessageDelta",
                (totalRows + DELTA_CHUNK_SIZE - 1) / DELTA_CHUNK_SIZE);
        if (numChunks == 0) {
            return null;
        }
        // noinspection unchecked
        final WritableChunk<Values>[] dest = new WritableChunk[numChunks];
        try {
            // One full pooled chunk per DELTA_CHUNK_SIZE rows; the last asks for exactly the rows that remain and
            // receives the next power of two from the pool.
            for (int mi = 0; mi < numChunks; ++mi) {
                final int rows = (mi < numChunks - 1 || totalRows % DELTA_CHUNK_SIZE == 0)
                        ? DELTA_CHUNK_SIZE
                        : (int) (totalRows % DELTA_CHUNK_SIZE);
                dest[mi] = chunkType.makeWritableChunk(rows);
            }

            // Runs name their origin by delta index, so gather this column's chunk arrays from every delta into two
            // arrays indexed that way; references only, no data moves here.
            final int numDeltas = deltas.size();
            // noinspection unchecked
            final WritableChunk<Values>[][] colAddChunks = new WritableChunk[numDeltas][];
            // noinspection unchecked
            final WritableChunk<Values>[][] colModChunks = new WritableChunk[numDeltas][];
            for (int di = 0; di < numDeltas; ++di) {
                final BarrageMessageDelta delta = deltas.get(di);
                colAddChunks[di] = delta.addChunks[columnIndex];
                colModChunks[di] = delta.modChunks[columnIndex];
            }

            BarrageCopyKernel.makeBarrageCopyKernel(chunkType)
                    .copy(runs, dest, colAddChunks, colModChunks, DELTA_CHUNK_SIZE);
            return dest;
        } catch (final Throwable err) {
            closeChunks(dest);
            throw err;
        }
    }

    /**
     * The rows this delta recorded modified data for in {@code columnIndex}. Only meaningful for a column in
     * {@link #modifiedColumns}; every caller is already guarded by that test.
     */
    RowSet getRecordedMods(final int columnIndex) {
        if (perColumnRecordedMods == null) {
            return recordedMods;
        }
        final RowSet columnMods = perColumnRecordedMods[columnIndex];
        Assert.neqNull(columnMods, "columnMods");
        return columnMods;
    }

    /**
     * Detach and return the add chunks for {@code columnIndex}, nulling out this delta's reference to prevent
     * double-close. Used for zero-copy transfer to a {@link BarrageMessage}.
     */
    WritableChunk<Values>[] extractAddChunks(final int columnIndex) {
        final WritableChunk<Values>[] result = addChunks[columnIndex];
        addChunks[columnIndex] = null;
        return result;
    }

    /**
     * Detach and return the mod chunks for {@code columnIndex}, nulling out this delta's reference to prevent
     * double-close. Used for zero-copy transfer to a {@link BarrageMessage}.
     */
    WritableChunk<Values>[] extractModChunks(final int columnIndex) {
        final WritableChunk<Values>[] result = modChunks[columnIndex];
        modChunks[columnIndex] = null;
        return result;
    }

    @Override
    public void close() {
        update.release();
        recordedAdds.close();
        recordedMods.close();
        closeDistinct(perColumnRecordedMods);
        closeChunkArrays(addChunks);
        closeChunkArrays(modChunks);
    }

    /** Close each distinct row set in {@code rowSets} once; slots may be null or repeat. */
    static void closeDistinct(@Nullable final RowSet[] rowSets) {
        if (rowSets == null) {
            return;
        }
        final Set<RowSet> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
        for (final RowSet rowSet : rowSets) {
            if (rowSet != null) {
                distinct.add(rowSet);
            }
        }
        SafeCloseable.closeAll(distinct);
    }

    static void closeChunkArrays(final WritableChunk<Values>[][] chunkArrays) {
        if (chunkArrays == null) {
            return;
        }
        for (final WritableChunk<Values>[] chunks : chunkArrays) {
            closeChunks(chunks);
        }
    }

    static void closeChunks(@Nullable final WritableChunk<Values>[] chunks) {
        if (chunks != null) {
            SafeCloseable.closeAll(chunks);
        }
    }
}
