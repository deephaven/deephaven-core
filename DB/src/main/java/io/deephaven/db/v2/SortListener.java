/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.LongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.v2.hashing.HashMapK4V4;
import io.deephaven.db.v2.sort.LongSortKernel;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.utils.*;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableList;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.internal.log.LoggerFactory;
import org.apache.commons.lang3.mutable.MutableInt;
import shaded.parquet.it.unimi.dsi.fastutil.longs.LongIterators;

import java.util.*;
import java.util.function.LongUnaryOperator;

public class SortListener extends BaseTable.ShiftAwareListenerImpl {
    // Do I get my own logger?
    private static final Logger log = LoggerFactory.getLogger(SortListener.class);

    // We like this key because it's in the middle of the (positive 32 bit signed integer) space.
    public static final long REBALANCE_MIDPOINT = Configuration.getInstance().getLongWithDefault("QueryTable.intradaySort.rebalance.midpoint", 1L << 30);
    public static final int REBALANCE_RANGE_SIZE = Configuration.getInstance().getIntegerWithDefault("QueryTable.intradaySort.rebalance.rangeSize", 64);
    public static final int REBALANCE_GAP_SIZE = Configuration.getInstance().getIntegerWithDefault("QueryTable.intradaySort.rebalance.gapSize", 64);
    public static final boolean REBALANCE_EFFORT_TRACKER_ENABLED = Configuration.getInstance().getBooleanWithDefault("QueryTable.intradaySort.rebalance.effortTracker.enabled", false);

    private final DynamicTable parent;
    private final QueryTable result;
    private final HashMapK4V4 reverseLookup;
    private final ColumnSource<Comparable<?>>[] columnsToSortBy;
    private final Index resultIndex;
    private final SortingOrder [] order;
    private final RedirectionIndex sortMapping;
    private final ColumnSource<Comparable<?>>[] sortedColumnsToSortBy;
    private final EffortTracker effortTracker;

    private final TargetComparator targetComparator;

    private final ModifiedColumnSet.Transformer mcsTransformer;
    private final ModifiedColumnSet sortColumnSet;

    public SortListener(DynamicTable parent, QueryTable result, HashMapK4V4 reverseLookup,
                        ColumnSource<Comparable<?>>[] columnsToSortBy, SortingOrder [] order,
                        RedirectionIndex sortMapping, ColumnSource<Comparable<?>>[] sortedColumnsToSortBy,
                        ModifiedColumnSet.Transformer mcsTransformer, ModifiedColumnSet sortColumnSet) {
        super("sortInternal", parent, result);
        this.parent = parent;
        this.result = result;
        this.reverseLookup = reverseLookup;
        this.columnsToSortBy = columnsToSortBy;
        this.resultIndex = result.getIndex();
        this.order = order;
        this.sortMapping = sortMapping;
        this.sortedColumnsToSortBy = sortedColumnsToSortBy;
        this.effortTracker = REBALANCE_EFFORT_TRACKER_ENABLED ? new EffortTracker(100) : null;

        // We create these comparators here so as to avoid building new ones on every call to doUpdate().
        this.targetComparator = new TargetComparator();

        this.mcsTransformer = mcsTransformer;
        this.sortColumnSet = sortColumnSet;
    }

    // The new "onUpdate" algorithm.
    //
    // First a note about terminology: we refer to the table we are mapping as the "input" table; our sorted
    // representation of that is called the "output" table.
    //
    // Next a note about the "modified" argument. Our algorithm computes which subset of modifies need to be reordered.
    // These reordered modifies propagate downstream as removes plus adds. Thus, the set of modifies that propagate are
    // the subset of upstream modifies that were not reordered (but may have been shifted).
    //
    // == Initialization for the removed set ==
    //
    // Allocate an array of size (removed.size() + modified.size()) and fill it with indexes (in the output
    // coordinate space) of the 'removed' and 'reordered-modified' sets. We obtain these indexes by doing a reverse
    // mapping lookup. Call this array 'removedOutputKeys'. Note that we must also maintain our redirection index states.
    //
    // == Initialization for the added set ==
    //
    // Allocate an array of size (added.size() + modified.size()) and fill it with the key indexes (in the input
    // coordinate space) of the 'added' and 'reordered-modified' sets.
    //
    // Sort this array by key value, "ascending" (but in the sense of what the comparator thinks is ascending),
    // breaking ties by comparing input key indices ascending (actual ascending, as in Long.compare). This secondary
    // comparison keeps key ordering stable.
    //
    // Make a parallel array to 'addedInputKeys'; call it 'addedOutputKeys'. The entries in this array indicate the
    // key index in the "output" space _at_ which we want to insert an element. The calculation used is sensitive to
    // whether we are operating in the forward or backward direction. The calculation used is:
    //
    // Scanning forward, find the rightmost key value in the table that is <= the key value being added. If we are
    // operating in the reverse direction, the index of the found key is the exact key index to use. On the other
    // hand, if we are moving in the forward direction, we adjust it by adding 1 to that index.
    //
    // For output indices >= the median, we want to operate in the forward direction. For output indices < median,
    // we want to operate in the reverse direction.
    //
    // Example of existing table:
    //  output indexes:     10    20    21    40    50    51    52
    //  input values:        C     E     I     I     I     O     U
    //
    // Note that the median of this table is 40.
    //
    // Values to add (note these have already been sorted thanks to the code above):
    // B: highest <= key doesn't exist (start of table is a special case), so at-key-index is 9 and direction is reverse (this will occupy an empty slot at 9)
    // C: highest <= key is C at 10, before the median, so at-key-index is 10 and dir is reverse (this will push the existing C to the left)
    // D: highest <= key is C at 10, before median, at-key-index 10, reverse, pushes C to the left
    // E: highest <= key is E at 20, before median, at-key-index 20, reverse, pushes E to the left
    // I: highest <= key is I at 50, after median, at-key-index 51 (recall the +1 rule), forward, pushes O to the right
    // J: highest <= key is I at 50, after median, at-key-index 51, forward, pushes O to the right
    // O: highest <= key is O at 51, after median, at-key-index 52, forward, pushes U to the right
    // Z: highest <= key is U at 52, after median, at-key-index 53, forward, occupies an empty slot at 53.
    //
    // (End example)
    //
    // == Split the work between the 'forward' and 'reverse' direction, by splitting at the median ==
    //
    // For the sake of efficiency we divide our work between some items we want to insert in the "forward" direction
    // (moving elements to the right), and other items we want to insert in the "reverse" direction (moving elements
    // to the left).
    //
    // Then we apply the below algorithm to each part. First, we process the "reverse" elements in the reverse
    // direction. Then we process the "forward" elements in the forward direction. After both sides are done, we
    // apply the changes to the output set and notify our downstream listeners.
    //
    // == Processing the elements (in a given direction) ===
    //
    // We work through the added queue. We take turns between writing as many added/modified rows as possible and then
    // removing as many things off of the backlog as possible. The backlog is "virtual", in that we use the resultIndex
    // to remember that we have a mapping already at a particular row.
    //
    // The destination for these merged queue items is 'destinationSlot', which starts at the configured start point
    // (probably the median) and marches "ahead" (in the direction we're operating in). Furthermore,
    // 'destinationSlot' is never "before" 'desiredSlot', so it skips "ahead" as needed (again, the notion of
    // "before" and "ahead" depend on the direction we are operating in).
    //
    // The operation repeats the following steps until all rows were inserted.
    //
    // There is one final piece to the logic. Threaded throughout the loop there is code that has to do with
    // "spreading" elements when they get overcrowded. The general approach is to watch for a run greater than
    // "maximumRunLength", a value defined below. (A run is a contiguous sequence in the index where we have had to
    // move every key. For example, if there are 300 contiguous keys and we inserted a single key at the beginning,
    // this would be a run of 300 even though the backlog never got larger than size 1). We compute up front whether or
    // not we will have a large run, and if so, we start spreading as soon as we start placing elements. Additionally,
    // we always spread when we append to either end of the table.
    @Override
    public void onUpdate(final Update upstream) {
        try (final SafeCloseableList closer = new SafeCloseableList()) {
            final Update downstream = new Update();
            final boolean modifiedNeedsSorting = upstream.modifiedColumnSet.containsAny(sortColumnSet) && upstream.modified.nonempty();
            final long REVERSE_LOOKUP_NO_ENTRY_VALUE = reverseLookup.getNoEntryValue();

            // We use these in enough places that we might as well just grab them (and check their sizes) here.
            upstream.added.intSize("validating added elements");
            final int removedSize = upstream.removed.intSize("allocating removed elements");
            final int modifiedSize = modifiedNeedsSorting ? upstream.modified.intSize("allocating modified elements") : 0;

            Assert.assertion((long) removedSize + (long) modifiedSize <= Integer.MAX_VALUE,
                    "(long)removedSize + (long)modifiedSize <= Integer.MAX_VALUE");
            int numRemovedKeys = removedSize;
            final long[] removedOutputKeys = new long[removedSize + modifiedSize];

            // handle upstream removes immediately (lest state gets trashed by upstream shifts)
            if (numRemovedKeys > 0) {
                fillArray(removedOutputKeys, upstream.removed, 0, reverseLookup::remove);
                Arrays.sort(removedOutputKeys, 0, numRemovedKeys);
                final LongChunk<OrderedKeyIndices> keyChunk = LongChunk.chunkWrap(removedOutputKeys, 0, numRemovedKeys);
                try (final OrderedKeys wrappedKeyChunk = OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(keyChunk)) {
                    sortMapping.removeAll(wrappedKeyChunk);
                }
                try (final Index rmKeyIndex = sortedArrayToIndex(removedOutputKeys, 0, numRemovedKeys)) {
                    resultIndex.remove(rmKeyIndex);
                }
            }

            // handle upstream shifts; note these never effect the sorted output keyspace
            final SortMappingAggregator mappingChanges = closer.add(new SortMappingAggregator());
            try (final Index prevIndex = parent.getIndex().getPrevIndex()) {
                upstream.shifted.forAllInIndex(prevIndex, (key, delta) -> {
                    final long dst = reverseLookup.remove(key);
                    if (dst != REVERSE_LOOKUP_NO_ENTRY_VALUE) {
                        mappingChanges.append(dst, key + delta);
                    }
                });
                mappingChanges.flush();
            }

            final long indexKeyForLeftmostInsert = resultIndex.empty() ? REBALANCE_MIDPOINT : resultIndex.firstKey() - 1;
            if (indexKeyForLeftmostInsert <= 0) {
                // Actually we "could", but we "don't" (yet).
                throw new IllegalStateException("Table has filled to key index 0; need to rebalance but cannot.");
            }

            // Identify the location where each key needs to be inserted.
            int numAddedKeys = 0;
            int numPropagatedModdedKeys = 0;
            final Index addedAndModified = modifiedNeedsSorting ? closer.add(upstream.added.union(upstream.modified)) : upstream.added;
            final long[] addedInputKeys = SortHelpers.getSortedKeys(order, columnsToSortBy, addedAndModified, false, false).getArrayMapping();
            final long[] addedOutputKeys = new long[addedInputKeys.length];
            final long[] propagatedModOutputKeys = modifiedNeedsSorting ? new long[upstream.modified.intSize()] : CollectionUtil.ZERO_LENGTH_LONG_ARRAY;

            final Index.SearchIterator ait = resultIndex.searchIterator();
            for (int ii = 0; ii < addedInputKeys.length; ++ii) {
                targetComparator.setTarget(addedInputKeys[ii]);
                final long after = ait.binarySearchValue(targetComparator, SortingOrder.Ascending.direction);
                final long outputKey = after == -1 ? indexKeyForLeftmostInsert : after;
                final long curr = modifiedNeedsSorting ? reverseLookup.get(addedInputKeys[ii]) : REVERSE_LOOKUP_NO_ENTRY_VALUE;

                // check if new location differs from current location or if the previous row needs to slot here
                if (curr != outputKey || (numAddedKeys > 0 && addedOutputKeys[numAddedKeys - 1] == curr)) {
                    // true for all adds and reordered mods
                    addedInputKeys[numAddedKeys] = addedInputKeys[ii];
                    addedOutputKeys[numAddedKeys] = outputKey;
                    ++numAddedKeys;

                    // check if we need to remove an existing mapping
                    if (curr != REVERSE_LOOKUP_NO_ENTRY_VALUE) {
                        removedOutputKeys[numRemovedKeys++] = curr;
                    }
                } else {
                    // thus this is a non-reordering mod
                    propagatedModOutputKeys[numPropagatedModdedKeys++] = outputKey;
                }
            }

            // Process downstream removed keys. Note that sortMapping cannot be modified until after the above loop completes
            // otherwise the algorithm will not be able to break ties by upstream keyspace.
            if (numRemovedKeys > removedSize) {
                Arrays.sort(removedOutputKeys, removedSize, numRemovedKeys);
                final LongChunk<OrderedKeyIndices> keyChunk = LongChunk.chunkWrap(removedOutputKeys, removedSize, numRemovedKeys - removedSize);
                try (final OrderedKeys wrappedKeyChunk = OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(keyChunk)) {
                    sortMapping.removeAll(wrappedKeyChunk);
                }
                try (final Index rmKeyIndex = sortedArrayToIndex(removedOutputKeys, removedSize, numRemovedKeys - removedSize)) {
                    resultIndex.remove(rmKeyIndex);
                }
                Arrays.sort(removedOutputKeys, 0, numRemovedKeys);
            }
            downstream.removed = sortedArrayToIndex(removedOutputKeys, 0, numRemovedKeys);

            final long medianOutputKey = resultIndex.empty() ? REBALANCE_MIDPOINT :
                    resultIndex.get(resultIndex.size() / 2);

            int addedStart = findKeyStart(addedOutputKeys, medianOutputKey, numAddedKeys);

            // The forward items in the add queue need to be adjusted by +1 for the logic to be right.
            for (int ii = addedStart; ii < numAddedKeys; ++ii) {
                addedOutputKeys[ii]++;
            }

            // Queues going in the reverse direction
            final QueueState rqs = new QueueState(-1, addedOutputKeys, addedInputKeys,
                    addedStart - 1, -1);
            // Queues going in the forward direction
            final QueueState fqs = new QueueState(1, addedOutputKeys, addedInputKeys,
                    addedStart, numAddedKeys);

            final IndexShiftData.Builder shiftBuilder = new IndexShiftData.Builder();
            final Index.SequentialBuilder addedBuilder = Index.FACTORY.getSequentialBuilder();

            performUpdatesInDirection(addedBuilder, shiftBuilder, medianOutputKey - 1, rqs, mappingChanges);
            performUpdatesInDirection(addedBuilder, shiftBuilder, medianOutputKey, fqs, mappingChanges);
            downstream.added = addedBuilder.getIndex();
            downstream.shifted = shiftBuilder.build();
            mappingChanges.flush();

            // Compute modified set in post-shift space.
            if (modifiedNeedsSorting && numPropagatedModdedKeys == 0 || upstream.modified.empty() || upstream.modifiedColumnSet.empty()) {
                downstream.modified = Index.FACTORY.getEmptyIndex();
            } else if (modifiedNeedsSorting) {
                Arrays.sort(propagatedModOutputKeys, 0, numPropagatedModdedKeys);

                int ii, si;
                final Index.SequentialBuilder modifiedBuilder = Index.FACTORY.getSequentialBuilder();
                for (ii = 0, si = 0; ii < numPropagatedModdedKeys && si < downstream.shifted.size(); ++si) {
                    final long beginRange = downstream.shifted.getBeginRange(si);
                    final long endRange = downstream.shifted.getEndRange(si);
                    final long shiftDelta = downstream.shifted.getShiftDelta(si);

                    // before the shifted range
                    for (; ii < numPropagatedModdedKeys && propagatedModOutputKeys[ii] < beginRange; ++ii) {
                        modifiedBuilder.appendKey(propagatedModOutputKeys[ii]);
                    }
                    // the shifted range
                    for (; ii < numPropagatedModdedKeys && propagatedModOutputKeys[ii] <= endRange; ++ii) {
                        modifiedBuilder.appendKey(propagatedModOutputKeys[ii] + shiftDelta);
                    }
                }

                // everything past the last shift
                for (; ii < numPropagatedModdedKeys; ++ii) {
                    modifiedBuilder.appendKey(propagatedModOutputKeys[ii]);
                }

                downstream.modified = modifiedBuilder.getIndex();
            } else {
                final long[] modifiedOutputKeys = new long[upstream.modified.intSize()];
                fillArray(modifiedOutputKeys, upstream.modified, 0, reverseLookup::get);
                Arrays.sort(modifiedOutputKeys);
                downstream.modified = sortedArrayToIndex(modifiedOutputKeys, 0, modifiedOutputKeys.length);
            }

            // Calculate downstream MCS.
            if (downstream.modified.empty()) {
                downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            } else {
                downstream.modifiedColumnSet = result.modifiedColumnSet;
                mcsTransformer.clearAndTransform(upstream.modifiedColumnSet, downstream.modifiedColumnSet);
            }

            // Update the final result index.
            resultIndex.insert(downstream.added);

            result.notifyListeners(downstream);
        }
    }

    private Index sortedArrayToIndex(long[] arr, int offset, int length) {
        final Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();
        builder.appendKeys(LongIterators.wrap(arr, offset, length));
        return builder.getIndex();
    }

    /**
     * @param added     The resulting added index
     * @param shifted   The resulting shift data
     * @param start     Start position
     * @param qs        Queue state -- containing the view on the various keys arrays, directions, etc.
     */
    private void performUpdatesInDirection(final Index.SequentialBuilder added, final IndexShiftData.Builder shifted, final long start,
                                           final QueueState qs, final SortMappingAggregator mappingChanges) {
        final long numRequestedAdds = (qs.addedEnd - qs.addedCurrent) * qs.direction;

        if (numRequestedAdds == 0) {
            return;
        }

        long numWrites = 0;
        final DirectionalResettableSequentialBuilder resultAdded = new DirectionalResettableSequentialBuilder(qs.direction);
        final DirectionalResettableIndexShiftDataBuilder resultShifted = new DirectionalResettableIndexShiftDataBuilder(qs.direction);

        final DirectionalResettableSequentialBuilder modRemoved = new DirectionalResettableSequentialBuilder(qs.direction);
        final DirectionalResettableSequentialBuilder modAdded = new DirectionalResettableSequentialBuilder(qs.direction);

        // When we notice that there is a long run length of contiguous mapping, we enter into spread mode which leaves
        // gaps for future incremental updates to use.

        // We use the sqrt of the resultIndex.size(), but we pick a reasonable minimum size. By the way,
        // integer overflow on cast-to-int wouldn't be a problem until
        // resultIndex.size() >= Integer.MAX_VALUE^2, which we won't reach because we'll run out of memory long
        // before.
        final int minimumRunLength = REBALANCE_RANGE_SIZE + REBALANCE_GAP_SIZE;
        final int maximumRunLength = Math.max(minimumRunLength, (int) Math.sqrt(resultIndex.size()));

        final Index.SearchIterator gapEvictionIter = (qs.direction == -1) ? resultIndex.reverseIterator() : resultIndex.searchIterator();
        final Index.SearchIterator backlogIter = (qs.direction == -1) ? resultIndex.reverseIterator() : resultIndex.searchIterator();

        long destKey = qs.addedOutputKeys[qs.addedCurrent];
        while (qs.hasMoreToAdd()) {
            // precondition: backlog starts empty
            long desiredOutputKey = qs.addedOutputKeys[qs.addedCurrent];
            boolean tableEmpty = !backlogIter.advance(desiredOutputKey);

            if (qs.isBefore(destKey, desiredOutputKey)) {
                destKey = desiredOutputKey;
            }

            if (tableEmpty) {
                // finish this update; backlog will forever be empty
                long writesUntilGap = Math.max(1, REBALANCE_RANGE_SIZE / 2);

                do {
                    // insert extra space at the end of the table; because it's the right thing to do
                    if (--writesUntilGap == 0) {
                        destKey = insertAGap(destKey, qs, modRemoved, mappingChanges, null);
                        writesUntilGap = REBALANCE_RANGE_SIZE;
                    }

                    resultAdded.appendKey(destKey);
                    mappingChanges.append(destKey, qs.addedInputKeys[qs.addedCurrent]);
                    destKey += qs.direction;
                    qs.addedCurrent += qs.direction;
                } while (qs.hasMoreToAdd());

                break;
            }

            // determine if we must be in spreading mode
            final long maxRunKey = desiredOutputKey + maximumRunLength * qs.direction;

            // note: this is an (over) approximation of cardinality since binarySearch will give any index if exists
            long addedMaxIdx;
            if (qs.direction == -1) {
                addedMaxIdx = qs.twiddleIfNegative(Arrays.binarySearch(qs.addedOutputKeys, 0, qs.addedCurrent, maxRunKey));
            } else {
                addedMaxIdx = qs.twiddleIfNegative(Arrays.binarySearch(qs.addedOutputKeys, qs.addedCurrent, qs.addedEnd, maxRunKey));
            }

            // note: if Index.SearchIterator had an O(1) method to get pos we should prefer that over Index#find,
            // turn maxRunKey into an advancing iterator (similar to gapEvictionIter), and also use that method to compute sizeToShift
            final long backMaxIdx = qs.twiddleIfNegative(resultIndex.find(maxRunKey));

            long sizeToAdd = qs.direction * (addedMaxIdx - qs.addedCurrent);
            long sizeToShift = qs.direction * (backMaxIdx - resultIndex.find(backlogIter.currentValue()));

            final boolean spreadMode = sizeToAdd + sizeToShift >= maximumRunLength;

            long writesUntilGap = REBALANCE_RANGE_SIZE;
            boolean backlogged = false;

            // stay in this loop until we might need to enable spreading; don't leave this loop while backlog is non-empty
            while (!tableEmpty && (backlogged || sizeToAdd > 0 && (spreadMode || (sizeToAdd + sizeToShift) <= qs.direction * (maxRunKey - destKey)))) {
                // Add anything prior to the next possible backlog item.
                while (qs.hasMoreToAdd() && qs.isBefore(desiredOutputKey, backlogIter.currentValue() + qs.direction)) {
                    if (spreadMode && --writesUntilGap == 0) {
                        destKey = insertAGap(destKey, qs, modRemoved, mappingChanges, gapEvictionIter);
                        writesUntilGap = REBALANCE_RANGE_SIZE;
                    }

                    checkDestinationSlotOk(destKey);
                    mappingChanges.append(destKey, qs.addedInputKeys[qs.addedCurrent]);
                    resultAdded.appendKey(destKey);

                    ++numWrites;
                    --sizeToAdd;
                    destKey += qs.direction;
                    qs.addedCurrent += qs.direction;

                    if (qs.hasMoreToAdd()) {
                        desiredOutputKey = qs.addedOutputKeys[qs.addedCurrent];
                    }
                }

                // Either, all items have been added, or next item comes after the backlog item(s) is(are) processed.
                long backlogKey = backlogIter.currentValue();
                final boolean writesPending = qs.hasMoreToAdd();
                while (((!writesPending || qs.isBefore(backlogKey, desiredOutputKey)) && qs.isBefore(backlogKey, destKey))) {
                    if (spreadMode && --writesUntilGap == 0) {
                        destKey = insertAGap(destKey, qs, modRemoved, mappingChanges, gapEvictionIter);
                        writesUntilGap = REBALANCE_RANGE_SIZE;
                    }

                    // insert this item
                    checkDestinationSlotOk(destKey);
                    modAdded.appendKey(destKey);
                    mappingChanges.append(destKey, sortMapping.get(backlogKey));
                    resultShifted.noteRequiredShift(backlogKey, destKey - backlogKey);

                    ++numWrites;
                    --sizeToShift;
                    destKey += qs.direction;

                    if (tableEmpty = !backlogIter.hasNext()) {
                        break; // done inserting from backlog
                    }
                    backlogKey = backlogIter.nextLong();
                }

                // must disable shift coalescing if any keys between last shift and next shift are not shifted
                backlogged = (writesPending && qs.isBefore(desiredOutputKey, destKey)) ||
                        (!tableEmpty && qs.isBefore(backlogIter.currentValue(), destKey));
                if (!backlogged) {
                    // note that we don't bother counting the number of shifted rows we're skipping
                    resultShifted.noteBacklogNowEmpty();
                    tableEmpty = !writesPending || !backlogIter.advance(desiredOutputKey);
                    if (qs.isBefore(destKey, desiredOutputKey)) {
                        destKey = desiredOutputKey;
                    }
                }
            }
        }

        if (effortTracker != null) {
            effortTracker.add(numWrites, numRequestedAdds);
            log.info().append(effortTracker.summarize()).endl();
        }

        resultAdded.appendToBuilder(added);
        resultShifted.appendToBuilder(shifted);

        // Note: modRemoved.intersect(modAdded) is not guaranteed to be empty
        resultIndex.remove(modRemoved.getIndex());
        resultIndex.insert(modAdded.getIndex());
    }

    private long insertAGap(final long destinationSlot, final QueueState qs,
                            final DirectionalResettableSequentialBuilder modRemoved,
                            final SortMappingAggregator mappingChanges,
                            final Index.SearchIterator gapEvictionIter) {
        final long gapEnd = destinationSlot + REBALANCE_GAP_SIZE * qs.direction;  // exclusive

        checkDestinationSlotOk(gapEnd);
        modRemoved.appendRange(destinationSlot, gapEnd - qs.direction);

        // evict any existing rows in the gap
        if (gapEvictionIter != null && gapEvictionIter.advance(destinationSlot)) {
            while (qs.isBefore(gapEvictionIter.currentValue(), gapEnd)) {
                mappingChanges.append(gapEvictionIter.currentValue(), Index.NULL_KEY);
                if (gapEvictionIter.hasNext()) {
                    gapEvictionIter.nextLong();
                } else {
                    break;
                }
            }
        }

        return gapEnd;
    }

    /**
     * The following may clarify what we are doing:
     * lKey (the "target") is in input coordinates
     * rKey (the "probe") is in output coordinates
     */
    private class TargetComparator implements Index.TargetComparator {
        private long lKey;
        private final ColumnComparatorFactory.IComparator[] comparators;

        TargetComparator() {
            Assert.eq(columnsToSortBy.length, "columnsToSortBy.length",
                    sortedColumnsToSortBy.length, "sortedColumnsToSortBy.length");
            this.comparators = new ColumnComparatorFactory.IComparator[columnsToSortBy.length];
            for (int ii = 0; ii < columnsToSortBy.length; ii++) {
                comparators[ii] = ColumnComparatorFactory.createComparatorLeftCurrRightPrev(columnsToSortBy[ii], sortedColumnsToSortBy[ii]);
            }
            setTarget(-1);
        }

        public void setTarget(final long lKey) {
            this.lKey = lKey;
        }

        @Override
        public int compareTargetTo(final long rKey, final int dir) {
            for (int ii = 0; ii < columnsToSortBy.length; ++ii) {
                final int difference = comparators[ii].compare(lKey, rKey);
                if (difference != 0) {
                    return difference * order[ii].direction * dir;
                }
            }
            final long inputKey = sortMapping.get(rKey);
            return Long.compare(lKey, inputKey);
        }
    }

    private static class ExposedTLongArrayList extends TLongArrayList {
        public ExposedTLongArrayList() {
        }

        public long[] peekDataArray() {
            return _data;
        }
    }

    private class SortMappingAggregator implements SafeCloseable {
        private final int chunkSize;
        private final ExposedTLongArrayList keys;
        private final ExposedTLongArrayList values;
        private final WritableLongChunk valuesChunk;
        private final WritableLongChunk<OrderedKeyIndices> keysChunk;
        private final WritableChunkSink.FillFromContext fillFromContext;
        private final LongSortKernel sortKernel;

        SortMappingAggregator() {
            keys = new ExposedTLongArrayList();
            values = new ExposedTLongArrayList();
            chunkSize = 4096;
            keysChunk = WritableLongChunk.makeWritableChunk(chunkSize);
            valuesChunk = WritableLongChunk.makeWritableChunk(chunkSize);
            fillFromContext = sortMapping.makeFillFromContext(chunkSize);
            sortKernel = LongSortKernel.makeContext(ChunkType.Long, SortingOrder.Ascending, chunkSize, true);
        }

        @Override
        public void close() {
            valuesChunk.close();
            keysChunk.close();
            fillFromContext.close();
            sortKernel.close();
        }

        public void flush() {
            if (keys.size() == 0) {
                return;
            }

            final int size = keys.size();
            for (int ii = 0; ii < size; ii += chunkSize) {
                final int thisSize = Math.min(chunkSize, size - ii);
                keysChunk.copyFromArray(keys.peekDataArray(), ii, 0, thisSize);
                valuesChunk.copyFromArray(values.peekDataArray(), ii, 0, thisSize);
                keysChunk.setSize(thisSize);
                valuesChunk.setSize(thisSize);

                //noinspection unchecked
                sortKernel.sort(valuesChunk, keysChunk);

                try (final OrderedKeys orderedKeys = OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(keysChunk)) {
                    //noinspection unchecked
                    sortMapping.fillFromChunk(fillFromContext, valuesChunk, orderedKeys);
                }

                for (int jj = 0; jj < thisSize; ++jj) {
                    final long index = valuesChunk.get(jj);
                    if (index != Index.NULL_KEY) {
                        reverseLookup.put(index, keysChunk.get(jj));
                    } else {
                        reverseLookup.remove(index);
                    }
                }
            }

            keys.clear();
            values.clear();
        }

        public void append(long key, long index) {
            keys.add(key);
            values.add(index);
        }

        public int checkpoint() {
            return keys.size();
        }
    }

    private static int findKeyStart(long[] array, long key, int arrayLen) {
        int index = Arrays.binarySearch(array, 0, arrayLen, key);
        if (index < 0) {
            // If the key was not found, then return the hypothetical insertion point (the first element > key)
            return -index - 1;
        }

        // If the key was found, then there might be multiple keys with the same value. If so, walk backwards to the
        // first such key.
        while (index > 0 && array[index - 1] == key) {
            --index;
        }
        return index;
    }

    private static class EffortTracker {
        private final LongRingBuffer writes;
        private final LongRingBuffer requestedAdds;
        private long totalNumWrites;
        private long totalNumRequestedAdds;

        EffortTracker(int windowSize) {
            this.writes = new LongRingBuffer(windowSize, false);
            this.requestedAdds = new LongRingBuffer(windowSize, false);
            this.totalNumWrites = 0;
            this.totalNumRequestedAdds = 0;
        }

        public void add(long numWrites, long numRequestedAdds) {
            if (writes.size() == writes.capacity()) {
                totalNumWrites -= writes.remove();
                totalNumRequestedAdds -= requestedAdds.remove();
            }

            totalNumWrites += numWrites;
            totalNumRequestedAdds += numRequestedAdds;
            writes.add(numWrites);
            requestedAdds.add(numRequestedAdds);
        }

        String summarize() {
            final double workRatio = totalNumRequestedAdds == 0 ? 0 :
                    (double) totalNumWrites / (double) totalNumRequestedAdds;
            return String.format("Sort Effort Summary: samples=%d, writes=%d, requested=%d, ratio=%g",
                    writes.size(), totalNumWrites, totalNumRequestedAdds, workRatio);
        }
    }

    private static class DirectionalResettableSequentialBuilder implements Index.SequentialBuilder {
        private final int direction;
        private final TLongArrayList firsts;
        private final TLongArrayList lasts;

        private DirectionalResettableSequentialBuilder(int direction, long[] initialItems) {
            this(direction, initialItems, direction > 0 ? 0 : initialItems.length - 1, direction > 0 ? initialItems.length : -1);
        }

        private DirectionalResettableSequentialBuilder(int direction, long[] initialItems, int begin, int end) {
            this(direction);
            while (begin != end) {
                appendKey(initialItems[begin]);
                begin += direction;
            }
        }

        private DirectionalResettableSequentialBuilder(int direction) {
            Assert.assertion(direction == -1 || direction == 1, "invalid direction");
            this.direction = direction;
            this.firsts = new TLongArrayList();
            this.lasts = new TLongArrayList();
        }

        @Override
        public void accept(final long first, final long last) {
            appendRange(first, last);
        }

        @Override
        public void appendKey(long key) {
            appendRange(key, key);
        }

        @Override
        public void appendRange(long firstKey, long lastKey) {
            // if direction == 1, then lastKey must be >= firstKey
            // if direction == -1, then lastKey must be <= firstKey
            final int rangeDirection = -Long.compare(firstKey, lastKey);
            if (rangeDirection * direction < 0) {
                Assert.assertion(rangeDirection * direction >= 0, "Range must be compatible with direction",
                        (Object) firstKey, "firstKey", (Object) lastKey, "lastKey", direction, "direction");
            }

            final int lSize = lasts.size();
            if (lSize > 0) {
                final long lastLast = lasts.get(lSize - 1);

                Assert.assertion(Long.compare(lastKey, lastLast) * direction > 0, "Long.compare(lastKey, lastLast) * direction > 0",
                        "New key not being added in the right direction");
                if (lastLast + direction == firstKey) {
                    lasts.set(lSize - 1, lastKey);
                    return;
                }
            }
            firsts.add(firstKey);
            lasts.add(lastKey);
        }

        @Override
        public Index getIndex() {
            Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();
            appendToBuilder(builder);
            return builder.getIndex();
        }

        private void appendToBuilder(Index.SequentialBuilder builder) {
            int nr = firsts.size();
            if (direction == -1) {
                for (int ii = nr - 1; ii >= 0; --ii) {
                    builder.appendRange(lasts.get(ii), firsts.get(ii));
                }
            } else {
                for (int ii = 0; ii < nr; ++ii) {
                    builder.appendRange(firsts.get(ii), lasts.get(ii));
                }
            }
        }
    }

    private static class DirectionalResettableIndexShiftDataBuilder {
        private final int direction;

        private boolean allowedToCoalesce = false;
        private final TLongArrayList firsts;
        private final TLongArrayList lasts;
        private final TLongArrayList deltas;

        private DirectionalResettableIndexShiftDataBuilder(int direction) {
            Assert.assertion(direction == -1 || direction == 1, "invalid direction");
            this.direction = direction;
            this.firsts = new TLongArrayList();
            this.lasts = new TLongArrayList();
            this.deltas = new TLongArrayList();
        }

        private void noteRequiredShift(final long key, final long delta) {
            if (delta * direction <= 0) {
                Assert.assertion(delta * direction > 0, "Shift delta must be compatible with direction",
                        (Object) key, "key", (Object) delta, "delta", direction, "direction");
            }

            final int lSize = lasts.size();
            if (lSize > 0 && allowedToCoalesce) {
                final long lastDelta = deltas.get(lSize - 1);
                if (lastDelta == delta) { // we can coalesce this shift
                    lasts.set(lSize - 1, key);
                    return;
                }
            } else {
                allowedToCoalesce = true;
            }

            firsts.add(key);
            lasts.add(key);
            deltas.add(delta);
        }

        private void noteBacklogNowEmpty() {
            // disable coalescing as there might be other keys that are not being shifted right now
            allowedToCoalesce = false;
        }

        private void appendToBuilder(final IndexShiftData.Builder builder) {
           int nr = firsts.size();
           if (direction < 0) {
               for (int ii = nr - 1; ii >= 0; --ii) {
                   builder.shiftRange(lasts.get(ii), firsts.get(ii), deltas.get(ii));
               }
           } else {
               for (int ii = 0; ii < nr; ++ii) {
                   builder.shiftRange(firsts.get(ii), lasts.get(ii), deltas.get(ii));
               }
           }
        }
    }

    private static class QueueState {
        // +1 or -1
        final int direction;
        // We provide a view on these various arrays
        final long[] addedOutputKeys;
        final long[] addedInputKeys;
        // The index for the current element of added(Input,Output)Keys
        int addedCurrent;
        // The exclusive end index for the added(Input,Output)Keys
        final int addedEnd;

        QueueState(int direction, long[] addedOutputKeys, long[] addedInputKeys, int addedCurrent, int addedEnd) {
            this.direction = direction;
            this.addedOutputKeys = addedOutputKeys;
            this.addedInputKeys = addedInputKeys;
            this.addedCurrent = addedCurrent;
            this.addedEnd = addedEnd;
        }

        private boolean hasMoreToAdd() {
            return addedCurrent != addedEnd;
        }

        private boolean isBefore(long a, long b) {
            if (direction == -1) {
                return b < a;
            }
            return a < b;
        }

        // This manipulates binarySearch results to yield the exclusive index.
        private long twiddleIfNegative(long a) {
            if (direction == -1) {
                return (a < 0) ? (~a) - 1 : a;
            }
            return (a < 0) ? ~a : a;
        }
    }

    private static void checkDestinationSlotOk(long destinationSlot) {
        if (destinationSlot <= 0 || destinationSlot == Long.MAX_VALUE) {
            throw new IllegalStateException(String.format("While updating index, the destination slot %d reached its limit",
                    destinationSlot));
        }
    }

    private static void fillArray(final long[] dest, final Index src, final int destIndex, final LongUnaryOperator transformer) {
        final MutableInt pos = new MutableInt(destIndex);
        src.forAllLongs((final long v) -> {
            dest[pos.intValue()] = transformer.applyAsLong(v);
            pos.increment();
        });
    }

    private static void showGaps(Index index) {
        long freeStart = 0;
        for (Index.RangeIterator i = index.rangeIterator(); i.hasNext(); ) {
            i.next();
            long freeEnd = i.currentRangeStart() - 1;
            long freeSize = freeEnd - freeStart + 1;

            long usedStart = i.currentRangeStart();
            long usedEnd = i.currentRangeEnd();

            long usedSize = usedEnd - usedStart + 1;
            System.out.printf("free %14d [%14d..%14d] [0x%10x..0x%10x]  used %14d [%14d..%14d] [0x%10x..0x%10x]%n",
                    freeSize, freeStart, freeEnd, freeStart, freeEnd,
                    usedSize, usedStart, usedEnd, usedStart, usedEnd);
            freeStart = usedEnd + 1;
        }
    }
}
