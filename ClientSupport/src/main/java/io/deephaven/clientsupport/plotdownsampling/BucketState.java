package io.deephaven.clientsupport.plotdownsampling;

import io.deephaven.base.verify.Assert;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.db.v2.utils.IndexUtilities;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Represents a given "pixel" in the downsampled output - the first and last value within that pixel, and the max/min
 * value of each column we're interested in within that pixel.
 *
 * The arrays of values for a given downsampled table are shared between all states, so each BucketState instance tracks
 * its own offset in those arrays.
 */
public class BucketState {
    private final Index index = Index.FACTORY.getEmptyIndex();

    private Index cachedIndex;

    /** the key used in the map */
    private final long key;

    /** the offset within the arraysources used to track values and their indexes */
    private final int offset;

    private final ValueTracker[] values;

    private final boolean trackNulls;
    private final Index[] nulls;

    public BucketState(final long key, final int offset, final ValueTracker[] valueTrackers, boolean trackNulls) {
        Assert.eqTrue(trackNulls || offset == 0 || offset == 1, "trackNulls || offset == 0 || offset == 1");
        this.key = key;
        this.offset = offset;
        this.values = valueTrackers;
        this.trackNulls = trackNulls;
        if (trackNulls) {
            this.nulls = IntStream.range(0, valueTrackers.length).mapToObj(ignore -> Index.FACTORY.getEmptyIndex())
                    .toArray(Index[]::new);
        } else {
            this.nulls = null;
        }
    }

    public Index getIndex() {
        return index;
    }

    public long getKey() {
        return key;
    }

    public long getOffset() {
        return offset;
    }

    public void append(final long rowIndex, final Chunk<? extends Attributes.Values>[] valueChunks,
            final int chunkIndex) {
        index.insert(rowIndex);
        for (int i = 0; i < values.length; i++) {
            values[i].append(offset, rowIndex, valueChunks[i], chunkIndex, trackNulls ? nulls[i] : null);
        }
        if (cachedIndex != null) {
            cachedIndex.close();
            cachedIndex = null;
        }
    }

    public void remove(final long rowIndex) {
        index.remove(rowIndex);
        for (int i = 0; i < values.length; i++) {
            if (trackNulls) {
                nulls[i].remove(rowIndex);
            }
            values[i].remove(offset, rowIndex);
        }
        if (cachedIndex != null) {
            cachedIndex.close();
            cachedIndex = null;
        }
    }

    public void update(final long rowIndex, final Chunk<? extends Attributes.Values>[] valueChunks,
            final int chunkIndex) {
        for (int i = 0; i < values.length; i++) {
            final Chunk<? extends Attributes.Values> valueChunk = valueChunks[i];
            if (valueChunk == null) {
                continue;// skip, already decided to be unnecessary
            }
            values[i].update(offset, rowIndex, valueChunk, chunkIndex, trackNulls ? nulls[i] : null);
        }
        if (cachedIndex != null) {
            cachedIndex.close();
            cachedIndex = null;
        }
    }

    public void shift(final IndexShiftData shiftData) {
        // update the bucket's index
        shiftData.apply(index);

        if (trackNulls) {
            // if we're tracking nulls, update those arrays
            for (final Index nullValues : nulls) {
                shiftData.apply(nullValues);
            }
        }

        // move the max and min indexes, if needed
        for (final ValueTracker tracker : values) {
            tracker.shiftMaxIndex(offset, shiftData);
            tracker.shiftMinIndex(offset, shiftData);
        }
    }

    public void rescanIfNeeded(final DownsampleChunkContext context) {
        final long indexSize = index.size();

        // this was already checked before this method was called, but let's make sure so that the null logic works
        Assert.gt(indexSize, "indexSize", 0);

        final int[] cols = IntStream.range(0, values.length)
                .filter(i -> {
                    if (trackNulls) {
                        // if all items are null, don't look for max/min - we can't be sure of this without null
                        // tracking
                        if (nulls[i].size() == indexSize) {
                            // all items are null, so we can mark this as valid
                            values[i].maxValueValid(offset, true);
                            values[i].minValueValid(offset, true);
                            // for sanity's sake, also mark the max and min index to be null
                            values[i].setMaxIndex(offset, QueryConstants.NULL_LONG);
                            values[i].setMinIndex(offset, QueryConstants.NULL_LONG);
                            return false;
                        }
                    }
                    return !values[i].maxValueValid(offset) || !values[i].minValueValid(offset);
                })
                .toArray();

        // This next line appears to be necessary, but is deliberately commented out, since it will have no effect.
        // Normally, any use of a ChunkContext to get Y values should first have a call to useYValues to ensure
        // that those contexts are ready. In this case, we already know that the contexts exists, so there is no
        // need to populate them - if they didn't exist, we wouldn't need to rescan that column, when that column
        // was first marked as needing a rescan, we already created the context.
        /* context.addYColumnsOfInterest(cols); */

        if (cols.length == 0) {
            return;
        }

        // As this is a complete rescan, we first pretend that this bucket has never seen any values, and mark all
        // positions as "null". We must have at least one value in the given context, so we know it will be marked
        // as valid again when we're done.
        for (final int columnIndex : cols) {
            if (trackNulls) {
                nulls[columnIndex].clear();
            }
            values[columnIndex].setMaxIndex(offset, QueryConstants.NULL_LONG);
            values[columnIndex].setMinIndex(offset, QueryConstants.NULL_LONG);
        }

        final OrderedKeys.Iterator it = index.getOrderedKeysIterator();
        while (it.hasMore()) {
            final OrderedKeys next = it.getNextOrderedKeysWithLength(RunChartDownsample.CHUNK_SIZE);
            // LongChunk<Attributes.Values> dateChunk = context.getXValues(next, false);
            final LongChunk<Attributes.OrderedKeyIndices> keyChunk = next.asKeyIndicesChunk();
            final Chunk<? extends Attributes.Values>[] valueChunks = context.getYValues(cols, next, false);

            // find the max in this chunk, compare with existing, loop.
            // this loop uses the prepared "which columns actually need testing" array
            for (int indexInChunk = 0; indexInChunk < keyChunk.size(); indexInChunk++) {
                for (final int columnIndex : cols) {
                    values[columnIndex].append(offset, keyChunk.get(indexInChunk), valueChunks[columnIndex],
                            indexInChunk, trackNulls ? nulls[columnIndex] : null);
                }
            }
        }
    }

    public Index makeIndex() {
        if (cachedIndex != null) {
            return cachedIndex;
        }
        final Index.RandomBuilder build = Index.FACTORY.getRandomBuilder();
        Assert.eqFalse(index.empty(), "index.empty()");
        build.addKey(index.firstKey());
        build.addKey(index.lastKey());
        if (trackNulls) {
            long indexSize = index.size();
            for (int i = 0; i < values.length; i++) {
                if (nulls[i].size() != indexSize) {
                    ValueTracker tracker = values[i];
                    // No need to null check these, since we already know at least one real value is in here, as we
                    // were tracking nulls
                    build.addKey(tracker.maxIndex(offset));
                    build.addKey(tracker.minIndex(offset));
                } // Else nothing to do, entire bucket is null, and we already included first+last, more than needed
            }

            for (Index nullsForCol : nulls) {
                if (nullsForCol.empty()) {
                    continue;
                }
                OrderedKeys.Iterator keysIterator = index.getOrderedKeysIterator();
                MutableLong position = new MutableLong(0);
                IndexUtilities.forAllInvertedLongRanges(index, nullsForCol, (first, last) -> {
                    if (first > 0) {
                        // Advance to (first - 1)
                        keysIterator.getNextOrderedKeysWithLength(first - 1 - position.longValue());
                        build.addKey(keysIterator.peekNextKey());
                        // Advance to first
                        keysIterator.getNextOrderedKeysWithLength(1);
                        build.addKey(keysIterator.peekNextKey());

                        position.setValue(first);
                    }

                    if (last < indexSize - 1) {
                        // Advance to last
                        keysIterator.getNextOrderedKeysWithLength(last - position.longValue());
                        build.addKey(keysIterator.peekNextKey());
                        // Advance to (last + 1)
                        keysIterator.getNextOrderedKeysWithLength(1);
                        build.addKey(keysIterator.peekNextKey());

                        position.setValue(last + 1);
                    }
                });
            }
        } else {
            for (final ValueTracker tracker : values) {
                // Nulls are not being tracked, so instead we will ask each column if it has only null values. If
                // so, skip max/min in the constructed index for this column, the first/last (and other column
                // values) are sufficient for this column. If either max or min index is null, the other must be as
                // well.

                final long max = tracker.maxIndex(offset);
                final long min = tracker.minIndex(offset);
                if (max != QueryConstants.NULL_LONG || min != QueryConstants.NULL_LONG) {
                    Assert.neq(max, "max", QueryConstants.NULL_LONG);
                    Assert.neq(min, "min", QueryConstants.NULL_LONG);
                    build.addKey(max);
                    build.addKey(min);
                } else {
                    // if one is null, both must be
                    Assert.eq(max, "max", QueryConstants.NULL_LONG);
                    Assert.eq(min, "min", QueryConstants.NULL_LONG);
                }
            }
        }

        cachedIndex = build.getIndex();
        return cachedIndex;
    }

    @Override
    public String toString() {
        return "BucketState{" +
                "key=" + key +
                ", offset=" + offset +
                ", values=" + Arrays.stream(values).map(vt -> vt.toString(offset)).collect(Collectors.joining(", ")) +
                '}';
    }

    public void validate(final boolean usePrev, final DownsampleChunkContext context, int[] allYColumnIndexes) {
        final OrderedKeys.Iterator it = index.getOrderedKeysIterator();
        while (it.hasMore()) {
            final OrderedKeys next = it.getNextOrderedKeysWithLength(RunChartDownsample.CHUNK_SIZE);
            final LongChunk<Attributes.OrderedKeyIndices> keyChunk = next.asKeyIndicesChunk();
            final Chunk<? extends Attributes.Values>[] valueChunks =
                    context.getYValues(allYColumnIndexes, next, usePrev);


            for (int indexInChunk = 0; indexInChunk < keyChunk.size(); indexInChunk++) {
                for (final int columnIndex : allYColumnIndexes) {
                    try {
                        if (trackNulls) {
                            if (nulls[columnIndex].size() == index.size()) {
                                // all entries are null
                                Assert.eq(values[columnIndex].maxIndex(offset),
                                        "values[" + columnIndex + "].maxIndex(" + offset + ")",
                                        QueryConstants.NULL_LONG);
                                Assert.eq(values[columnIndex].minIndex(offset),
                                        "values[" + columnIndex + "].minIndex(" + offset + ")",
                                        QueryConstants.NULL_LONG);
                            } else {
                                // must have non-null max and min
                                Assert.neq(values[columnIndex].maxIndex(offset),
                                        "values[" + columnIndex + "].maxIndex(" + offset + ")",
                                        QueryConstants.NULL_LONG);
                                Assert.neq(values[columnIndex].minIndex(offset),
                                        "values[" + columnIndex + "].minIndex(" + offset + ")",
                                        QueryConstants.NULL_LONG);
                            }
                        } // else we really can't assert anything specific
                        values[columnIndex].validate(offset, keyChunk.get(indexInChunk), valueChunks[columnIndex],
                                indexInChunk, trackNulls ? nulls[columnIndex] : null);
                    } catch (final RuntimeException e) {
                        System.out.println(index);
                        final String msg =
                                "Bad data! indexInChunk=" + indexInChunk + ", col=" + columnIndex + ", usePrev="
                                        + usePrev + ", offset=" + offset + ", index=" + keyChunk.get(indexInChunk);
                        throw new IllegalStateException(msg, e);
                    }
                }
            }
        }
        Assert.eqTrue(makeIndex().subsetOf(index), "makeIndex().subsetOf(index)");
    }

    public void close() {
        if (cachedIndex != null) {
            cachedIndex.close();
        }
        index.close();
    }
}
