//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharCompactModifications and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmcountdistinct.compactmodifications;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.IntComparisons;
import io.deephaven.util.mutable.MutableInt;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * Reduces a parallel pair of "removed" (pre-modify) and "added" (post-modify) value runs to the net change required of
 * a {@link io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet SegmentedSortedMultiSet}: values that were both
 * removed and added in equal quantity cancel out, leaving only the net removals in {@code removedValues} and the net
 * additions in {@code addedValues}.
 */
public class IntCompactModifications {
    /**
     * Diff the {@code removedLength} removed values beginning at {@code removedStart} against the {@code addedLength}
     * added values beginning at {@code addedStart}: values that were both removed and added in equal quantity cancel
     * out. After the call {@code removedValues}/{@code removedCounts} hold the net removals compacted to distinct
     * values beginning at {@code removedStart}, and {@code addedValues}/{@code addedCounts} hold the net additions
     * compacted to distinct values beginning at {@code addedStart}; the surviving lengths are returned via
     * {@code removedSize}/{@code addedSize}. The chunk sizes are left unchanged so the inputs may be sub-ranges of
     * larger shared chunks (the removed and added ranges may have different starts and lengths, e.g. when each side is
     * a separately-flattened run of child deltas).
     *
     * @param removedValues the removed values; receives the net removals
     * @param removedCounts receives the count removed for each surviving net removal
     * @param addedValues the added values; receives the net additions
     * @param addedCounts receives the count added for each surviving net addition
     * @param removedStart the first position to process in the removed chunk
     * @param removedLength the number of positions to process in the removed chunk
     * @param addedStart the first position to process in the added chunk
     * @param addedLength the number of positions to process in the added chunk
     * @param countNull whether null is a countable value
     * @param countNaN whether NaN is a countable value
     * @param removedSize set to the number of surviving net removals
     * @param addedSize set to the number of surviving net additions
     */
    public static void compactAndCountModifications(
            WritableIntChunk<? extends Values> removedValues, WritableIntChunk<ChunkLengths> removedCounts,
            WritableIntChunk<? extends Values> addedValues, WritableIntChunk<ChunkLengths> addedCounts,
            int removedStart, int removedLength, int addedStart, int addedLength, boolean countNull, boolean countNaN,
            MutableInt removedSize, MutableInt addedSize) {
        final int removedEnd = removedStart + removedLength;
        final int addedEnd = addedStart + addedLength;
        removedValues.sort(removedStart, removedLength);
        addedValues.sort(addedStart, addedLength);

        int rRead = removedStart;
        int aRead = addedStart;
        int rWrite = removedStart;
        int aWrite = addedStart;

        // walk both sorted ranges; for each distinct value emit only the net change, dropping ignored (null/NaN) runs
        while (rRead < removedEnd && aRead < addedEnd) {
            final int removedValue = removedValues.get(rRead);
            final int addedValue = addedValues.get(aRead);
            if (IntComparisons.eq(removedValue, addedValue)) {
                final int removedRun = countRun(removedValues, rRead, removedEnd);
                final int addedRun = countRun(addedValues, aRead, addedEnd);
                rRead += removedRun;
                aRead += addedRun;
                if (!ignore(removedValue, countNull, countNaN)) {
                    if (removedRun > addedRun) {
                        removedValues.set(rWrite, removedValue);
                        removedCounts.set(rWrite, removedRun - addedRun);
                        rWrite++;
                    } else if (addedRun > removedRun) {
                        addedValues.set(aWrite, addedValue);
                        addedCounts.set(aWrite, addedRun - removedRun);
                        aWrite++;
                    }
                }
            } else if (IntComparisons.lt(removedValue, addedValue)) {
                final int removedRun = countRun(removedValues, rRead, removedEnd);
                rRead += removedRun;
                if (!ignore(removedValue, countNull, countNaN)) {
                    removedValues.set(rWrite, removedValue);
                    removedCounts.set(rWrite, removedRun);
                    rWrite++;
                }
            } else {
                final int addedRun = countRun(addedValues, aRead, addedEnd);
                aRead += addedRun;
                if (!ignore(addedValue, countNull, countNaN)) {
                    addedValues.set(aWrite, addedValue);
                    addedCounts.set(aWrite, addedRun);
                    aWrite++;
                }
            }
        }

        // drain any remaining removals
        while (rRead < removedEnd) {
            final int removedValue = removedValues.get(rRead);
            final int removedRun = countRun(removedValues, rRead, removedEnd);
            rRead += removedRun;
            if (!ignore(removedValue, countNull, countNaN)) {
                removedValues.set(rWrite, removedValue);
                removedCounts.set(rWrite, removedRun);
                rWrite++;
            }
        }

        // drain any remaining additions
        while (aRead < addedEnd) {
            final int addedValue = addedValues.get(aRead);
            final int addedRun = countRun(addedValues, aRead, addedEnd);
            aRead += addedRun;
            if (!ignore(addedValue, countNull, countNaN)) {
                addedValues.set(aWrite, addedValue);
                addedCounts.set(aWrite, addedRun);
                aWrite++;
            }
        }

        removedSize.set(rWrite - removedStart);
        addedSize.set(aWrite - addedStart);
    }

    private static int countRun(WritableIntChunk<? extends Values> values, int pos, int end) {
        final int value = values.get(pos);
        int run = 1;
        while (pos + run < end && IntComparisons.eq(values.get(pos + run), value)) {
            run++;
        }
        return run;
    }

    private static boolean ignore(int value, boolean countNull, boolean countNaN) {
        if (!countNull && value == NULL_INT) {
            return true;
        }
        // region maybeIgnoreNaN
        // endregion maybeIgnoreNaN
        return false;
    }
}
