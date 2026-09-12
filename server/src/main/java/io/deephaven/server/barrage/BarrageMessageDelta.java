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
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.util.UpdateCoalescer;
import io.deephaven.extensions.barrage.chunk.BarrageCopyKernel;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import static io.deephaven.server.barrage.BarrageMessageProducer.DELTA_CHUNK_SIZE;

/**
 * One update graph cycle's worth of change to a table, as recorded by a {@link BarrageMessageProducer} for the
 * subscribers it had at the time: the {@link TableUpdate}, the rows whose data was recorded, and that data as chunks. A
 * producer queues these until its subscribers' update interval elapses, then coalesces the queue into one
 * {@link io.deephaven.engine.table.impl.util.BarrageMessage}; {@link #compact} is that coalescing, and the producer
 * also applies it early, in place, so the queue does not grow with the number of cycles per interval.
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
     * carry modified data" still works. Non-null exactly for {@link #modifiedColumns}.
     */
    @Nullable
    final RowSet[] perColumnRecordedMods;
    final BitSet subscribedColumns;
    final BitSet modifiedColumns;

    /**
     * Per-column chunk storage for added rows. {@code addChunks[columnIndex]} is an array of {@link WritableChunk
     * WritableChunks}, each holding exactly {@code DELTA_CHUNK_SIZE} rows except the last; the copy kernel locates a
     * row by dividing its position by {@code DELTA_CHUNK_SIZE}, so every delta, recorded or compacted, must keep that
     * layout. Null for columns not in {@code subscribedColumns}. Slots may be set to null after detachment for
     * zero-copy transfer.
     */
    final WritableChunk<Values>[][] addChunks;

    /**
     * Per-column chunk storage for modified rows. {@code modChunks[columnIndex]} is an array of {@link WritableChunk
     * WritableChunks}, each holding up to {@code DELTA_CHUNK_SIZE} rows of data. Null for columns not in
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
     * copied into fresh chunks, and the run's deltas are left intact.
     *
     * @param deltas the run, in recording order, at least two deltas; still owned by the caller
     * @param baseRowSet the propagated row set as of immediately before the run
     * @param chunkSources the producer's column sources, for each column's chunk type
     * @return a new delta, which the caller owns
     */
    static BarrageMessageDelta compact(final List<BarrageMessageDelta> deltas, final RowSet baseRowSet,
            final ChunkSource.WithPrev<Values>[] chunkSources) {
        final int numDeltas = deltas.size();
        if (numDeltas < 2) {
            throw new IllegalArgumentException(
                    "compact requires at least two deltas; a run of one is already compact");
        }
        final int numColumns = chunkSources.length;

        final UpdateCoalescer coalescer = new UpdateCoalescer(baseRowSet, deltas.get(0).update);
        for (int i = 1; i < numDeltas; ++i) {
            coalescer.update(deltas.get(i).update);
        }

        // We need to build our included additions and included modifications in addition to the coalesced update.
        final BitSet addColumnSet = new BitSet();
        final BitSet modColumnSet = new BitSet();

        final WritableRowSet localAdded = RowSetFactory.empty();
        for (int i = 0; i < numDeltas; ++i) {
            final BarrageMessageDelta delta = deltas.get(i);
            localAdded.remove(delta.update.removed());
            delta.update.shifted().apply(localAdded);

            // reset the add column set if we do not have any adds from previous updates
            if (localAdded.isEmpty()) {
                addColumnSet.clear();
            }

            if (delta.recordedAdds.isNonempty()) {
                if (addColumnSet.isEmpty()) {
                    addColumnSet.or(delta.subscribedColumns);
                } else {
                    // It pays to be certain that all of the data we look up was written down.
                    Assert.equals(delta.subscribedColumns, "delta.subscribedColumns", addColumnSet, "addColumnSet");
                }

                localAdded.insert(delta.recordedAdds);
            }

            if (delta.recordedMods.isNonempty()) {
                modColumnSet.or(delta.modifiedColumns);
            }
        }

        // One drawback of the ModifiedColumnSet, is that our adds must include data for all columns. However,
        // column specific data may be updated and we only write down that single changed column. So, the
        // computation of mapping output rows to input data may be different per Column. We can re-use calculations
        // where the set of deltas that modify column A are the same as column B.
        //
        // The mapping arrays store encoded source references (see below for details).
        // This allows reuse of the mapping across columns sharing the same modification pattern.
        final class ColumnInfo {
            final WritableRowSet recordedMods = RowSetFactory.empty();
            /** Where each surviving added / modified row's value comes from, as runs; see {@link Runs}. */
            final Runs addedRuns = new Runs();
            final Runs modifiedRuns = new Runs();
            long addedRows;
            long modifiedRows;
        }

        // Two columns may share a ColumnInfo only if every contributing delta recorded the same modified rows for
        // both. A recorded delta always did, so for it the pattern of which deltas modify the column is enough. A
        // compacted delta keeps one set per column (see perColumnRecordedMods) but hands the same set to every column
        // that shared a ColumnInfo when it was built, so the key also carries the identity of those sets: columns
        // that shared before go on sharing, and columns that diverged are kept apart.
        final HashMap<MappingKey, ColumnInfo> infoCache = new HashMap<>();
        final List<ColumnInfo> allColumnInfos = new ArrayList<>();
        final IntFunction<ColumnInfo> getColumnInfo = (columnIndex) -> {
            final BitSet deltasThatModifyThisColumn = new BitSet();
            final RowSet[] compactedMods = new RowSet[numDeltas];
            for (int i = 0; i < numDeltas; ++i) {
                final BarrageMessageDelta delta = deltas.get(i);
                if (delta.modifiedColumns.get(columnIndex)) {
                    deltasThatModifyThisColumn.set(i);
                    if (delta.perColumnRecordedMods != null) {
                        compactedMods[i] = delta.perColumnRecordedMods[columnIndex];
                    }
                }
            }

            final MappingKey key = new MappingKey(deltasThatModifyThisColumn, compactedMods);
            final ColumnInfo cached = infoCache.get(key);
            if (cached != null) {
                return cached;
            }

            final ColumnInfo retval = new ColumnInfo();
            allColumnInfos.add(retval);
            infoCache.put(key, retval);
            for (int i = 0; i < numDeltas; ++i) {
                final BarrageMessageDelta delta = deltas.get(i);
                retval.recordedMods.remove(delta.update.removed());
                delta.update.shifted().apply(retval.recordedMods);

                if (deltasThatModifyThisColumn.get(i)) {
                    retval.recordedMods.insert(delta.recordedMods(columnIndex));
                }
            }
            retval.recordedMods.remove(coalescer.added);

            retval.addedRows = localAdded.size();
            retval.modifiedRows = retval.recordedMods.size();

            final WritableRowSet unfilledAdds = localAdded.isEmpty() ? RowSetFactory.empty()
                    : RowSetFactory.flat(localAdded.size());
            final WritableRowSet unfilledMods = retval.recordedMods.isEmpty() ? RowSetFactory.empty()
                    : RowSetFactory.flat(retval.recordedMods.size());

            final WritableRowSet addedRemaining = localAdded.copy();
            final WritableRowSet modifiedRemaining = retval.recordedMods.copy();
            for (int i = numDeltas - 1; i >= 0; --i) {
                if (addedRemaining.isEmpty() && modifiedRemaining.isEmpty()) {
                    break;
                }

                final BarrageMessageDelta delta = deltas.get(i);

                // Encode (fromMods, deltaIndex) into the high bits of sourceRows values so that a run's source
                // identifies the delta, the side and the position in one long.
                final long encodedAddBase = ((long) i) << BarrageCopyKernel.DELTA_INDEX_SHIFT;
                final long encodedModBase = encodedAddBase | (1L << BarrageCopyKernel.DELTA_MOD_FLAG_BIT);

                final BiConsumer<Boolean, Boolean> applyMapping = (addedMapping, recordedAdds) -> {
                    final WritableRowSet remaining = addedMapping ? addedRemaining : modifiedRemaining;
                    final RowSet deltaRecorded =
                            recordedAdds ? delta.recordedAdds : delta.recordedMods(columnIndex);
                    try (final RowSet recorded = remaining.intersect(deltaRecorded);
                            final WritableRowSet sourceRows = deltaRecorded.invert(recorded);
                            final RowSet destinationsInPosSpace = remaining.invert(recorded);
                            final RowSet rowsToFill = (addedMapping ? unfilledAdds : unfilledMods)
                                    .subSetForPositions(destinationsInPosSpace)) {
                        // Shift sourceRows so each value encodes (deltaIndex | fromMods | srcPos).
                        sourceRows.shiftInPlace(recordedAdds ? encodedAddBase : encodedModBase);

                        remaining.remove(recorded);
                        if (addedMapping) {
                            unfilledAdds.remove(rowsToFill);
                        } else {
                            unfilledMods.remove(rowsToFill);
                        }

                        emitRuns(rowsToFill, sourceRows, addedMapping ? retval.addedRuns : retval.modifiedRuns);
                    }
                };

                applyMapping.accept(true, true); // map recorded adds
                applyMapping.accept(false, true); // map recorded mods that might have a scoped add

                if (deltasThatModifyThisColumn.get(i)) {
                    applyMapping.accept(true, false); // map recorded mods that propagate as adds
                    applyMapping.accept(false, false); // map recorded mods
                }

                delta.update.shifted().unapply(addedRemaining);
                delta.update.shifted().unapply(modifiedRemaining);
            }

            if (!unfilledAdds.isEmpty()) {
                Assert.assertion(false, "Error: added:" + coalescer.added + " unfilled:" + unfilledAdds
                        + " missing:" + coalescer.added.subSetForPositions(unfilledAdds));
            }
            Assert.eq(unfilledAdds.size(), "unfilledAdds.size()", 0);
            Assert.eq(unfilledMods.size(), "unfilledMods.size()", 0);
            return retval;
        };

        // Columns the run modified upstream are candidates; only those with surviving recorded rows keep data.
        if (coalescer.modifiedColumnSet == ModifiedColumnSet.ALL) {
            modColumnSet.set(0, numColumns);
        } else {
            modColumnSet.or(coalescer.modifiedColumnSet.extractAsBitSet());
        }

        // Kernel contexts, created only for columns whose runs are too short for range copies; see copyColumn.
        final BarrageCopyKernel.BarrageCopyKernelContext[] contexts =
                new BarrageCopyKernel.BarrageCopyKernelContext[numColumns];
        final IntFunction<BarrageCopyKernel.BarrageCopyKernelContext> contextFor = (columnIndex) -> {
            if (contexts[columnIndex] == null) {
                // noinspection unchecked
                final WritableChunk<Values>[][] colAddChunks = new WritableChunk[numDeltas][];
                // noinspection unchecked
                final WritableChunk<Values>[][] colModChunks = new WritableChunk[numDeltas][];
                for (int di = 0; di < numDeltas; ++di) {
                    final BarrageMessageDelta d = deltas.get(di);
                    colAddChunks[di] = d.addChunks[columnIndex];
                    colModChunks[di] = d.modChunks[columnIndex];
                }
                contexts[columnIndex] =
                        BarrageCopyKernel.makeBarrageCopyKernel(chunkSources[columnIndex].getChunkType())
                                .makeContext(colAddChunks, colModChunks, DELTA_CHUNK_SIZE);
            }
            return contexts[columnIndex];
        };

        // noinspection unchecked
        final WritableChunk<Values>[][] addChunks = new WritableChunk[numColumns][];
        for (int ci = addColumnSet.nextSetBit(0); ci >= 0; ci = addColumnSet.nextSetBit(ci + 1)) {
            final ColumnInfo info = getColumnInfo.apply(ci);
            final int columnIndex = ci;
            addChunks[ci] = copyColumn(info.addedRuns, info.addedRows, chunkSources[ci].getChunkType(), deltas, ci,
                    () -> contextFor.apply(columnIndex));
        }

        final RowSet[] perColumnRecordedMods = new RowSet[numColumns];
        final BitSet modifiedColumns = new BitSet();
        final WritableRowSet recordedMods = RowSetFactory.empty();
        final Set<ColumnInfo> handedOff = Collections.newSetFromMap(new IdentityHashMap<>());
        // noinspection unchecked
        final WritableChunk<Values>[][] modChunks = new WritableChunk[numColumns][];
        for (int ci = modColumnSet.nextSetBit(0); ci >= 0; ci = modColumnSet.nextSetBit(ci + 1)) {
            final ColumnInfo info = getColumnInfo.apply(ci);
            if (info.recordedMods.isEmpty()) {
                // modified upstream, but nothing we recorded survived the run
                continue;
            }
            final int columnIndex = ci;
            modChunks[ci] = copyColumn(info.modifiedRuns, info.modifiedRows, chunkSources[ci].getChunkType(), deltas,
                    ci, () -> contextFor.apply(columnIndex));
            // Columns that share a ColumnInfo share its row set; the delta closes each distinct set once.
            perColumnRecordedMods[ci] = info.recordedMods;
            modifiedColumns.set(ci);
            if (handedOff.add(info)) {
                recordedMods.insert(info.recordedMods);
            }
        }

        for (final ColumnInfo info : allColumnInfos) {
            if (!handedOff.contains(info)) {
                info.recordedMods.close();
            }
        }

        // Add data exists exactly for addColumnSet, so that is what this delta subscribes to; with no surviving
        // adds it inherits the run's column set so it still compacts with what follows it.
        final BitSet subscribedColumns =
                localAdded.isEmpty() ? (BitSet) deltas.get(0).subscribedColumns.clone() : addColumnSet;

        return new BarrageMessageDelta(deltas.get(0).generation, deltas.get(0).firstStep,
                deltas.get(numDeltas - 1).lastStep,
                coalescer.coalesce(), localAdded, recordedMods, perColumnRecordedMods,
                subscribedColumns, modifiedColumns, addChunks, modChunks);
    }

    /** Whether this delta only adds rows: nothing removed, modified or shifted, and no modified data recorded. */
    boolean supersedesNothing() {
        return update.removed().isEmpty() && update.modified().isEmpty() && update.shifted().empty()
                && recordedMods.isEmpty() && modifiedColumns.isEmpty();
    }

    /**
     * Whether nothing in the run supersedes anything recorded earlier in it: no removals, no modifications, no shifts.
     * Coalescing such a run cannot drop a single row, so compacting it would copy the whole run's data and save
     * nothing; the producer declines such runs.
     */
    static boolean nothingSuperseded(final List<BarrageMessageDelta> deltas) {
        for (final BarrageMessageDelta delta : deltas) {
            if (!delta.supersedesNothing()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Allocate storage for {@code size} rows of one column of a delta, from the pool. A full chunk of
     * {@link BarrageMessageProducer#DELTA_CHUNK_SIZE} rows is exactly a pool capacity. The last chunk of a column asks
     * for exactly the rows that remain and receives the next power of two at or above that, never more than
     * {@code DELTA_CHUNK_SIZE}; {@link #chunkBytes} counts that capacity, not the rows, so the rounding is visible in
     * the producer's pending-bytes figures.
     */
    static WritableChunk<Values> makeDeltaChunk(final ChunkType chunkType, final int size) {
        return chunkType.makeWritableChunk(size);
    }

    /**
     * The result of the mapping pass for one column pattern: for each stretch of surviving rows that is contiguous both
     * in the output and in the delta it comes from, one run of destination position, encoded source (delta index,
     * add-or-mod flag and source position, in the {@link BarrageCopyKernel} bit layout) and length. Row sets are
     * range-compressed and updates arrive in ranges, so a run usually covers many rows and the mapping pass costs
     * proportionally to ranges rather than rows.
     */
    static final class Runs {
        long[] dest = new long[16];
        long[] src = new long[16];
        long[] len = new long[16];
        int count;

        void add(final long destination, final long source, final long length) {
            if (count == dest.length) {
                dest = Arrays.copyOf(dest, count * 2);
                src = Arrays.copyOf(src, count * 2);
                len = Arrays.copyOf(len, count * 2);
            }
            dest[count] = destination;
            src[count] = source;
            len[count] = length;
            ++count;
        }
    }

    /**
     * Pair {@code destinations} with {@code sources}, both ascending and of equal size, in order, emitting one run for
     * each stretch over which both are contiguous.
     */
    static void emitRuns(final RowSet destinations, final RowSet sources, final Runs out) {
        Assert.eq(destinations.size(), "destinations.size()", sources.size(), "sources.size()");
        try (final RowSet.RangeIterator dit = destinations.rangeIterator();
                final RowSet.RangeIterator sit = sources.rangeIterator()) {
            long destPos = 0;
            long destEnd = -1;
            long srcPos = 0;
            long srcEnd = -1;
            while (true) {
                if (destPos > destEnd) {
                    if (!dit.hasNext()) {
                        break;
                    }
                    dit.next();
                    destPos = dit.currentRangeStart();
                    destEnd = dit.currentRangeEnd();
                }
                if (srcPos > srcEnd) {
                    Assert.assertion(sit.hasNext(), "sit.hasNext()");
                    sit.next();
                    srcPos = sit.currentRangeStart();
                    srcEnd = sit.currentRangeEnd();
                }
                final long length = Math.min(destEnd - destPos, srcEnd - srcPos) + 1;
                out.add(destPos, srcPos, length);
                destPos += length;
                srcPos += length;
            }
        }
    }

    /**
     * Build one column's output chunks for {@code totalRows} surviving rows from {@code runs}. Output chunks are cut at
     * {@link BarrageMessageProducer#DELTA_CHUNK_SIZE}, which is not a preference: the copy kernel and later compactions
     * locate a row by dividing its position by that constant, so every delta must keep that layout.
     *
     * <p>
     * When runs average at least {@link AbstractColumnSource#USE_RANGES_AVERAGE_RUN_LENGTH} rows, each run is copied
     * with an array copy, split where it crosses a chunk boundary on either side. Shorter runs would make a copy per
     * row through the range API, which is what the typed {@link BarrageCopyKernel} was built to avoid, so for them the
     * runs are expanded into the kernel's per-row mapping and the kernel gathers cell by cell.
     *
     * @return the output chunks, or null if there are no rows
     */
    private static WritableChunk<Values>[] copyColumn(final Runs runs, final long totalRows, final ChunkType chunkType,
            final List<BarrageMessageDelta> deltas, final int columnIndex,
            final Supplier<BarrageCopyKernel.BarrageCopyKernelContext> context) {
        final int numChunks = LongSizedDataStructure.intSize("BarrageMessageDelta",
                (totalRows + DELTA_CHUNK_SIZE - 1) / DELTA_CHUNK_SIZE);
        if (numChunks == 0) {
            return null;
        }
        // noinspection unchecked
        final WritableChunk<Values>[] dest = new WritableChunk[numChunks];
        for (int mi = 0; mi < numChunks; ++mi) {
            final int rows = (mi < numChunks - 1 || totalRows % DELTA_CHUNK_SIZE == 0)
                    ? DELTA_CHUNK_SIZE
                    : (int) (totalRows % DELTA_CHUNK_SIZE);
            dest[mi] = makeDeltaChunk(chunkType, rows);
        }

        if (totalRows / runs.count >= AbstractColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH) {
            copyRuns(runs, deltas, columnIndex, dest);
        } else {
            final long[][] mapping = mappingFromRuns(runs, numChunks, dest);
            final BarrageCopyKernel kernel = BarrageCopyKernel.makeBarrageCopyKernel(chunkType);
            final BarrageCopyKernel.BarrageCopyKernelContext ctx = context.get();
            for (int mi = 0; mi < numChunks; ++mi) {
                kernel.copyFromDeltaChunks(mapping[mi], dest[mi], ctx);
            }
        }
        return dest;
    }

    /** Copy every run with array copies, splitting a run wherever it crosses a source or destination chunk boundary. */
    private static void copyRuns(final Runs runs, final List<BarrageMessageDelta> deltas, final int columnIndex,
            final WritableChunk<Values>[] dest) {
        for (int ri = 0; ri < runs.count; ++ri) {
            final long encoded = runs.src[ri];
            final int deltaIdx =
                    (int) ((encoded >>> BarrageCopyKernel.DELTA_INDEX_SHIFT) & BarrageCopyKernel.DELTA_INDEX_MASK);
            final boolean fromMods = (encoded & (1L << BarrageCopyKernel.DELTA_MOD_FLAG_BIT)) != 0;
            final BarrageMessageDelta delta = deltas.get(deltaIdx);
            final WritableChunk<Values>[] srcChunks =
                    fromMods ? delta.modChunks[columnIndex] : delta.addChunks[columnIndex];

            long srcPos = encoded & BarrageCopyKernel.DELTA_POSITION_MASK;
            long destPos = runs.dest[ri];
            long remaining = runs.len[ri];
            while (remaining > 0) {
                final int srcOff = (int) (srcPos % DELTA_CHUNK_SIZE);
                final int destOff = (int) (destPos % DELTA_CHUNK_SIZE);
                final int length = (int) Math.min(remaining,
                        Math.min(DELTA_CHUNK_SIZE - srcOff, DELTA_CHUNK_SIZE - destOff));
                dest[(int) (destPos / DELTA_CHUNK_SIZE)].copyFromChunk(
                        srcChunks[(int) (srcPos / DELTA_CHUNK_SIZE)], srcOff, destOff, length);
                srcPos += length;
                destPos += length;
                remaining -= length;
            }
        }
    }

    /** Expand runs into the copy kernel's per-row mapping, one array per output chunk, sized like {@code dest}. */
    private static long[][] mappingFromRuns(final Runs runs, final int numChunks, final WritableChunk<Values>[] dest) {
        final long[][] mapping = new long[numChunks][];
        for (int mi = 0; mi < numChunks; ++mi) {
            mapping[mi] = new long[dest[mi].size()];
        }
        for (int ri = 0; ri < runs.count; ++ri) {
            long destPos = runs.dest[ri];
            long source = runs.src[ri];
            for (long remaining = runs.len[ri]; remaining > 0; --remaining) {
                mapping[(int) (destPos / DELTA_CHUNK_SIZE)][(int) (destPos % DELTA_CHUNK_SIZE)] = source;
                ++destPos;
                ++source;
            }
        }
        return mapping;
    }

    /**
     * The rows this delta recorded modified data for in {@code columnIndex}. Only meaningful for a column in
     * {@link #modifiedColumns}; every caller is already guarded by that test.
     */
    RowSet recordedMods(final int columnIndex) {
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
        if (perColumnRecordedMods != null) {
            // Columns that shared a ColumnInfo at compaction share one row set; close each distinct set once.
            final Set<RowSet> closed = Collections.newSetFromMap(new IdentityHashMap<>());
            for (final RowSet columnMods : perColumnRecordedMods) {
                if (columnMods != null && closed.add(columnMods)) {
                    columnMods.close();
                }
            }
        }
        closeChunkArrays(addChunks);
        closeChunkArrays(modChunks);
    }

    static void closeChunkArrays(final WritableChunk<Values>[][] chunkArrays) {
        if (chunkArrays == null) {
            return;
        }
        for (final WritableChunk<Values>[] chunks : chunkArrays) {
            if (chunks == null) {
                continue;
            }
            for (final WritableChunk<Values> chunk : chunks) {
                try (final SafeCloseable ignored = chunk) {
                }
            }
        }
    }

    /**
     * Cache key for the per-column mapping computation in {@link #compact}: which deltas of the run modify the column,
     * and for each of those that is itself compacted, the identity of the row set it recorded for the column.
     */
    private static final class MappingKey {
        private final BitSet deltasThatModify;
        private final RowSet[] compactedMods;
        private final int hash;

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
}
