//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run GenerateMegaMergeKernels or ./gradlew generateMegaMergeKernels to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sort.megamerge;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Indices;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sort.LongMegaMergeKernel;
import io.deephaven.engine.table.impl.sort.timsort.TimsortUtils;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.util.compare.ObjectComparisons;
import java.lang.Object;
import java.lang.Override;
import java.lang.SuppressWarnings;
import java.lang.UnsupportedOperationException;

/**
 * Merges a sorted chunk of Objects into a sorted run of Objects in ascending order, moving the parallel long row keys
 * with their values.
 */
public class ObjectLongMegaMergeKernel {
    private ObjectLongMegaMergeKernel() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public static <ATTR extends Any, KEY_INDICES extends Indices> ObjectLongMegaMergeKernelContext<ATTR, KEY_INDICES> createContext(
            ) {
        return ObjectLongMegaMergeKernelContext.INSTANCE;
    }

    public static <ATTR extends Any, KEY_INDICES extends Indices> void merge(
            LongArraySource destinationKeys, ObjectArraySource<Object> destinationValues,
            long destinationOffset, long destinationSize, LongChunk<KEY_INDICES> keysChunk,
            ObjectChunk<Object, ATTR> valuesChunk) {
        destinationKeys.ensureCapacity(destinationOffset + destinationSize + keysChunk.size(), false);
        destinationValues.ensureCapacity(destinationOffset + destinationSize + valuesChunk.size(), false);
        // find the location of run2[0] in run1
        final Object run2lo = valuesChunk.get(0);
        final long mergeStartPosition = upperBound(destinationValues, destinationOffset, destinationOffset + destinationSize, run2lo);
        if (mergeStartPosition == destinationOffset + destinationSize) {
            copyChunkToDest(keysChunk, valuesChunk, destinationKeys, destinationValues, 0, destinationSize + destinationOffset, valuesChunk.size());
            return;
        }
        final long mergeLength = destinationSize + valuesChunk.size();
        long destCursor = destinationOffset + destinationSize - 1;
        int chunkCursor = keysChunk.size() - 1;
        Object val1 = destinationValues.getUnsafe(destCursor);
        Object val2 = valuesChunk.get(chunkCursor);
        final long mergeEnd = destinationOffset + mergeLength;
        long ii = mergeEnd - 1;
        int minGallop = TimsortUtils.INITIAL_GALLOP;
        no_data_left: while (ii >= mergeStartPosition) {
            int destWins = 0;
            int chunkWins = 0;
            if (minGallop < 2) {
                minGallop = 2;
            }
            while (destWins < minGallop && chunkWins < minGallop) {
                if (geq(val2, val1)) {
                    destinationValues.set(ii, val2);
                    destinationKeys.set(ii--, keysChunk.get(chunkCursor));
                    if (--chunkCursor < 0) {
                        break no_data_left;
                    }
                    val2 = valuesChunk.get(chunkCursor);
                    chunkWins++;
                    destWins = 0;
                } else {
                    destinationValues.set(ii, val1);
                    destinationKeys.set(ii--, destinationKeys.getLong(destCursor));
                    if (--destCursor < mergeStartPosition) {
                        break no_data_left;
                    }
                    val1 = destinationValues.getUnsafe(destCursor);
                    destWins++;
                    chunkWins = 0;
                }
            }
            // we are in galloping mode now, if we had run out of data then we should have already bailed out to
            // no_data_left
            while (ii >= mergeStartPosition) {
                // if we had a lot of things from run2, we take the next thing from run1 then find it in run2
                final int copyUntil2 = gallopBackward(valuesChunk, 0, chunkCursor, val1, true);
                final int gallopLength2 = chunkCursor - copyUntil2 + 1;
                if (gallopLength2 > 1) {
                    copyChunkToDest(keysChunk, valuesChunk, destinationKeys, destinationValues, copyUntil2, ii - gallopLength2 + 1, gallopLength2);
                    chunkCursor -= gallopLength2;
                    ii -= gallopLength2;
                    if (chunkCursor < 0) {
                        break no_data_left;
                    }
                    val2 = valuesChunk.get(chunkCursor);
                    minGallop--;
                }
                // if we had a lot of things from run1, we take the next thing from run2 and then find it in run1
                final long copyUntil1 = gallopBackward(destinationValues, mergeStartPosition, destCursor, val2, false);
                final long gallopLength1 = destCursor - copyUntil1 + 1;
                if (gallopLength1 > 1) {
                    moveInDest(destinationKeys, destinationValues, copyUntil1, ii - gallopLength1 + 1, gallopLength1);
                    destCursor -= gallopLength1;
                    ii -= gallopLength1;
                    if (destCursor < mergeStartPosition) {
                        break no_data_left;
                    }
                    val1 = destinationValues.getUnsafe(destCursor);
                    minGallop--;
                }
                if (gallopLength1 < TimsortUtils.INITIAL_GALLOP && gallopLength2 < TimsortUtils.INITIAL_GALLOP) {
                    // undo the possible subtraction from above
                    minGallop += 2;
                    break;
                }
            }
        }
        if (chunkCursor >= 0) {
            copyChunkToDest(keysChunk, valuesChunk, destinationKeys, destinationValues, 0, ii - chunkCursor, chunkCursor + 1);
        }
    }

    @SuppressWarnings("rawtypes")
    private static <ATTR extends Any, KEY_INDICES extends Indices> void copyChunkToDest(
            LongChunk<KEY_INDICES> keysChunk, ObjectChunk<Object, ATTR> valuesChunk,
            LongArraySource destinationKeys, ObjectArraySource<Object> destinationValues,
            int sourceStart, long destStart, int length) {
        destinationValues.copyFromChunk(destStart, length, (ObjectChunk) valuesChunk, sourceStart);
        destinationKeys.copyFromChunk(destStart, length, keysChunk, sourceStart);
    }

    private static void moveInDest(LongArraySource destinationKeys,
            ObjectArraySource<Object> destinationValues, long sourceStart, long destStart,
            long length) {
        destinationKeys.move(sourceStart, destStart, length);
        destinationValues.move(sourceStart, destStart, length);
    }

    private static int doComparison(Object lhs, Object rhs) {
        return ObjectComparisons.compare(lhs, rhs);
    }

    private static boolean geq(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) >= 0;
    }

    /**
     * When we binary search in run 1, we must identify a position for the search value that is <em>after</em> our test
     * values, because the values from run 2 may never be inserted before an equal value from run 1.
     *
     * @param lo the inclusive lower bound of the search
     * @param hi the exclusive upper bound of the search
     * @return the position of the first element that is > searchValue, or hi if there is no such element
     */
    private static long upperBound(ObjectArraySource<Object> values, long lo, long hi,
            Object searchValue) {
        return bound(values, lo, hi, searchValue, false);
    }

    private static long bound(ObjectArraySource<Object> valuesToSort, long lo, long hi,
            Object searchValue, @SuppressWarnings("SameParameterValue") final boolean lower) {
        // lt or leq
        final int compareLimit = lower ? -1 : 0;
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            final Object testValue = valuesToSort.getUnsafe(mid);
            final boolean moveLo = doComparison(testValue, searchValue) <= compareLimit;
            if (moveLo) {
                // For bound, (testValue OP searchValue) means that the result somewhere later than 'mid' [OP=lt or leq]
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * Returns {@code bound(valuesToSort, lo, hi, searchValue, lower)}, probing hi - 1, hi - 2, hi - 4, hi - 8, ...
     * before the binary search, so the number of comparisons is logarithmic in the distance from the result to hi.
     * <p>
     * The search in run 1 passes lower = false, as values from run 2 may never be inserted before an equal value from
     * run 1.
     */
    private static long gallopBackward(ObjectArraySource<Object> valuesToSort, long lo, long hi,
            Object searchValue, @SuppressWarnings("SameParameterValue") final boolean lower) {
        // lt or leq
        final int compareLimit = lower ? -1 : 0;
        final long maxDistance = hi - lo;
        // every element at or after searchHi is after the result
        long searchHi = hi;
        long distance = 0;
        while (distance < maxDistance && doComparison(valuesToSort.getUnsafe(hi - 1 - distance), searchValue) > compareLimit) {
            searchHi = hi - 1 - distance;
            distance = distance < (maxDistance >> 1) ? (distance << 1) + 1 : maxDistance;
        }
        return bound(valuesToSort, hi - distance, searchHi, searchValue, lower);
    }

    /**
     * Returns {@code bound(valuesToSort, lo, hi, searchValue, lower)}, probing hi - 1, hi - 2, hi - 4, hi - 8, ...
     * before the binary search, so the number of comparisons is logarithmic in the distance from the result to hi.
     * <p>
     * The search in run 2 passes lower = true, as values from run 1 may never be inserted after an equal value from run
     * 2.
     */
    private static int gallopBackward(ObjectChunk<Object, ?> valuesToSort,
            @SuppressWarnings("SameParameterValue") int lo, int hi, Object searchValue,
            @SuppressWarnings("SameParameterValue") final boolean lower) {
        // lt or leq
        final int compareLimit = lower ? -1 : 0;
        final int maxDistance = hi - lo;
        // every element at or after searchHi is after the result
        int searchHi = hi;
        int distance = 0;
        while (distance < maxDistance && doComparison(valuesToSort.get(hi - 1 - distance), searchValue) > compareLimit) {
            searchHi = hi - 1 - distance;
            distance = distance < (maxDistance >> 1) ? (distance << 1) + 1 : maxDistance;
        }
        return bound(valuesToSort, hi - distance, searchHi, searchValue, lower);
    }

    private static int bound(ObjectChunk<Object, ?> valuesToSort, int lo, int hi,
            Object searchValue, @SuppressWarnings("SameParameterValue") final boolean lower) {
        // lt or leq
        final int compareLimit = lower ? -1 : 0;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final Object testValue = valuesToSort.get(mid);
            final boolean moveLo = doComparison(testValue, searchValue) <= compareLimit;
            if (moveLo) {
                // For bound, (testValue OP searchValue) means that the result somewhere later than 'mid' [OP=lt or leq]
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    public static class ObjectLongMegaMergeKernelContext<ATTR extends Any, KEY_INDICES extends Indices> implements LongMegaMergeKernel<ATTR, KEY_INDICES> {
        @SuppressWarnings("rawtypes")
        private static final ObjectLongMegaMergeKernelContext INSTANCE = new ObjectLongMegaMergeKernelContext();

        @Override
        @SuppressWarnings("unchecked")
        public void merge(LongArraySource indexDestinationSource,
                WritableColumnSource<?> valuesDestinationSource, long destinationOffset,
                long destinationSize, LongChunk<KEY_INDICES> indexKeys, Chunk<ATTR> valuesToMerge) {
            ObjectLongMegaMergeKernel.merge(indexDestinationSource, (ObjectArraySource<Object>) valuesDestinationSource, destinationOffset, destinationSize, indexKeys, valuesToMerge.asObjectChunk());
        }
    }
}
