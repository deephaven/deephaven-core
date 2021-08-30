package io.deephaven.clientsupport.plotdownsampling;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.BooleanArraySource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LongArraySource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * Wraps all values in a given Y column when downsampling and apply operations consistently to supported
 * column types. Each operation includes an offset, which is the BucketState.offset value - internally
 * this is transformed to the position in the array sources.
 *
 * Each tracker maintains 6 pieces of data/metadata on all possible buckets, spread across 3 array sources:
 *   o  the min and max value
 *   o  the indexes of those min and max values in the original table
 *   o  a flag indicating whether or not the each min and max are presently valid - these must always be true
 *      except when a shift aware update is currently being processed.
 *
 * It is possible that there are gaps in the data - this is not understood directly by the ValueTracker, but
 * instead by the fact that no BucketState exists with a corresponding offset.
 */
public abstract class ValueTracker {

    /**
     * Creates a set of value trackers to share across a given RunChartDownsample's BucketState instances.
     * @param valueColumnSources the Y columns being downsampled and tracked
     * @param bucketCount the initial size to allocate in each tracker, usually the number of pixels
     *                        to be displayed
     * @return an array of correctly typed and sizes value trackers for use with the given Y value column sources
     */
    public static ValueTracker[] of(final List<ColumnSource<?>> valueColumnSources, final int bucketCount) {
        final ValueTracker[] trackers = new ValueTracker[valueColumnSources.size()];
        for (int i = 0; i < trackers.length; i++) {
            switch (valueColumnSources.get(i).getChunkType()) {
                case Boolean:
                    throw new IllegalStateException("Cannot track boolean columns when downsampling");
                case Char:
                    trackers[i] = new CharValueTracker();
                    break;
                case Byte:
                    trackers[i] = new ByteValueTracker();
                    break;
                case Short:
                    trackers[i] = new ShortValueTracker();
                    break;
                case Int:
                    trackers[i] = new IntValueTracker();
                    break;
                case Long:
                    trackers[i] = new LongValueTracker();
                    break;
                case Float:
                    trackers[i] = new FloatValueTracker();
                    break;
                case Double:
                    trackers[i] = new DoubleValueTracker();
                    break;
                case Object:
                    trackers[i] = new ObjectValueTracker<>(valueColumnSources.get(i));
                    break;
            }
            trackers[i].ensureCapacity(bucketCount);
        }
        return trackers;
    }

    // May contain any non-negative value, or NULL_LONG if there is no value. We track NULL_LONG in cases where we
    // aren't tracking which indexes are null, so we know that the entire bucket contains nulls and we should only
    // include first/last index in the bucket's total index. This being null is not the same as the value being
    // invalid.
    private final LongArraySource indexes = new LongArraySource();

    // Must only be true/false, null is garbage and should cause errors. Something being marked as invalid indicates
    // that another pass will be required to find a more correct value for this entry.
    private final BooleanArraySource valid = new BooleanArraySource();

    protected void ensureCapacity(final int bucketCount) {
        indexes.ensureCapacity(bucketCount * 2);
        valid.ensureCapacity(bucketCount * 2);
    }

    /**
     * Indicates that a new value is being added to the original table being downsampled, and its value should be
     * considered as possibly interesting. Implementations should read the value from the chunk and specialize based on
     * that type of data.
     *
     * If it is the only value in the bucket (specified by the offset), include this value as both the max and the min.
     * If there are other values, check to see if the new value is either the new max or the new min. In any case where
     * this becomes the new max or the new min, mark that position as being valid, indicating that we are confident that
     * we have the largest or smallest value at that offset.
     *
     * Implementations must take care to check if the value is null. If so, if {@code nulls} is present, the current
     * index should be added to it. If the
     *
     * @param offset the offset of the bucket state to use - use this with minValuePosition/maxValuePosition to compute
     *               the actual position in the underlying array sources
     * @param rowIndex the index in the original table of the specified value. If the current given value is interesting
     *                 in some way, record this using setMinIndex/setMaxIndex so we can construct the full downsampled
     *                 table index later
     * @param valuesChunk the chunk that we're currently examining
     * @param indexInChunk the index in the chunk that we're currently examining
     */
    public abstract void append(int offset, long rowIndex, Chunk<? extends Attributes.Values> valuesChunk, int indexInChunk, @Nullable Index nulls);

    /**
     * Indicates that a row was removed from the original table being downsampled. If that index was previously
     * considered to be interesting, mark this offset as invalid, so that we can rescan later to find the next
     * interesting value.
     * @param offset the offset of the bucket state to use
     * @param rowIndex the index in the original table.
     */
    public final void remove(final int offset, final long rowIndex) {
        if (rowIndex == maxIndex(offset)) {
            maxValueValid(offset, false);
        }
        if (rowIndex == minIndex(offset)) {
            minValueValid(offset, false);
        }
    }

    /**
     * Indicates that a value has changed in the original table that is being downsampled, and we should consider if the
     * old value or the new value was interesting. Implementations should read the value from the chunk and specialize
     * based on that type of data.
     *
     * There are three cases to consider for each min and max, so six cases in total. Here is the summary for the three
     * "max" cases, the opposite must be likewise done for the min cases:
     *
     * If the updated row was the old max, then we cover two of the cases:
     *   o  if the new value is greater than the old value, record the new value, but we are still the max and still
     *      valid.
     *   o  if the new value is less than the old value, invalidate this row but keep the old max, we may need to
     *      rescan later
     *
     * Otherwise, if the new value is greater than the old max, then the current row is now the new max, and are now
     * valid.
     *
     * @param offset the offset of the bucket state to use - use this with minValuePosition/maxValuePosition to compute
     *               the actual position in the underlying array sources
     * @param rowIndex the index in the original table of the specified value. If the current given value is interesting
     *                 in some way, record this using setMinIndex/setMaxIndex so we can construct the full downsampled
     *                 table index later
     * @param valuesChunk the chunk that we're currently examining
     * @param chunkIndex the index in the chunk that we're currently examining
     */
    public abstract void update(int offset, long rowIndex, Chunk<? extends Attributes.Values> valuesChunk, int chunkIndex, @Nullable Index nulls);

    /**
     * Transforms the given BucketState.offset into the position in the array sources that represents the min value
     * of that bucket state.
     */
    protected final long minValuePosition(final int offset) {
        return 2 * offset;
    }
    /**
     * Transforms the given BucketState.offset into the position in the array sources that represents the max value
     * of that bucket state.
     */
    protected final long maxValuePosition(final int offset) {
        return 2 * offset + 1;
    }

    protected final long minIndex(final int offset) {
        return indexes.getUnsafe(minValuePosition(offset));
    }
    protected final long maxIndex(final int offset) {
        return indexes.getUnsafe(maxValuePosition(offset));
    }

    protected final void setMinIndex(final int offset, final long minIndex) {
        indexes.set(minValuePosition(offset), minIndex);
    }
    protected final void setMaxIndex(final int offset, final long maxIndex) {
        indexes.set(maxValuePosition(offset), maxIndex);
    }

    protected final void minValueValid(final int offset, final boolean isValid) {
        valid.set(minValuePosition(offset), isValid);
    }
    protected final void maxValueValid(final int offset, final boolean isValid) {
        valid.set(maxValuePosition(offset), isValid);
    }

    protected final boolean minValueValid(final int offset) {
        return valid.get(minValuePosition(offset));
    }
    protected final boolean maxValueValid(final int offset) {
        return valid.get(maxValuePosition(offset));
    }

    /**
     * Create a String representation of the tracked values and metadata at the given offset for logging/debugging
     * purposes.
     */
    public abstract String toString(int offset);

    /**
     * Scan the given chunk and confirm that whichever values are currently selected as max and min are correct, and
     * that the current data is now valid.
     */
    public abstract void validate(int offset, long rowIndex, Chunk<? extends Attributes.Values> valuesChunk, int indexInChunk, @Nullable Index nulls);

    public final void shiftMaxIndex(final int offset, final IndexShiftData shiftData) {
        final long maxIndex = maxIndex(offset);
        if (maxIndex == QueryConstants.NULL_LONG) {
            return;
        }

        int low = 0;
        int high = shiftData.size();
        while (low < high) {
            final int mid = (low + high) / 2;

            final long beginRange = shiftData.getBeginRange(mid);
            final long endRange = shiftData.getEndRange(mid);

            if (maxIndex < beginRange) {
                high = mid;
            } else if (maxIndex > endRange) {
                low = mid + 1;
            } else {
                setMaxIndex(offset, maxIndex + shiftData.getShiftDelta(mid));
                return;
            }
        }
    }

    public final void shiftMinIndex(final int offset, final IndexShiftData shiftData) {
        final long minIndex = minIndex(offset);
        if (minIndex == QueryConstants.NULL_LONG) {
            return;
        }

        int low = 0;
        int high = shiftData.size();
        while (low < high) {
            final int mid = (low + high) / 2;

            final long beginRange = shiftData.getBeginRange(mid);
            final long endRange = shiftData.getEndRange(mid);

            if (minIndex < beginRange) {
                high = mid;
            } else if (minIndex > endRange) {
                low = mid + 1;
            } else {
                setMinIndex(offset, minIndex + shiftData.getShiftDelta(mid));
                return;
            }
        }
    }

}
