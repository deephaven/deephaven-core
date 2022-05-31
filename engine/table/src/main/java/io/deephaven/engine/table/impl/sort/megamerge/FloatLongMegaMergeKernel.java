/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharLongMegaMergeKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sort.megamerge;

import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.engine.table.impl.sort.LongMegaMergeKernel;
import io.deephaven.engine.table.impl.sort.timsort.TimsortUtils;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Indices;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;

public class FloatLongMegaMergeKernel {
    private FloatLongMegaMergeKernel() {
        throw new UnsupportedOperationException();
    }

    // region Context
    public static class FloatLongMegaMergeKernelContext<ATTR extends Any, KEY_INDICES extends Indices>
            implements LongMegaMergeKernel<ATTR, KEY_INDICES> {
        @SuppressWarnings("rawtypes")
        private static final FloatLongMegaMergeKernelContext INSTANCE = new FloatLongMegaMergeKernelContext();

        @Override
        public void merge(LongArraySource indexDestinationSource, ArrayBackedColumnSource<?> valuesDestinationSource,
                long destinationOffset, long destinationSize, LongChunk<KEY_INDICES> indexKeys,
                Chunk<ATTR> valuesToMerge) {
            FloatLongMegaMergeKernel.merge(indexDestinationSource, (FloatArraySource) valuesDestinationSource,
                    destinationOffset, destinationSize, indexKeys, valuesToMerge.asFloatChunk());
        }
    }
    // endregion Context

    public static <ATTR extends Any, KEY_INDICES extends Indices> FloatLongMegaMergeKernelContext<ATTR, KEY_INDICES> createContext() {
        // noinspection unchecked
        return FloatLongMegaMergeKernelContext.INSTANCE;
    }

    static public <ATTR extends Any, KEY_INDICES extends Indices> void merge(LongArraySource destinationKeys,
            FloatArraySource destinationValues,
            long destinationOffset,
            long destinationSize,
            LongChunk<KEY_INDICES> keysChunk,
            FloatChunk<ATTR> valuesChunk) {
        destinationKeys.ensureCapacity(destinationOffset + destinationSize + keysChunk.size(), false);
        destinationValues.ensureCapacity(destinationOffset + destinationSize + valuesChunk.size(), false);

        // find the location of run2[0] in run1
        final float run2lo = valuesChunk.get(0);
        final long mergeStartPosition =
                upperBound(destinationValues, destinationOffset, destinationOffset + destinationSize, run2lo);

        if (mergeStartPosition == destinationOffset + destinationSize) {
            copyChunkToDest(keysChunk, valuesChunk, destinationKeys, destinationValues, 0,
                    destinationSize + destinationOffset, valuesChunk.size());
            return;
        }

        final long mergeLength = destinationSize + valuesChunk.size();

        long destCursor = destinationOffset + destinationSize - 1;
        int chunkCursor = keysChunk.size() - 1;

        float val1 = destinationValues.getUnsafe(destCursor);
        float val2 = valuesChunk.get(chunkCursor);

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

                    if (--destCursor < (int) mergeStartPosition) {
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
                final int copyUntil2 = lowerBound(valuesChunk, 0, chunkCursor, val1);

                final int gallopLength2 = chunkCursor - copyUntil2 + 1;
                if (gallopLength2 > 1) {
                    copyChunkToDest(keysChunk, valuesChunk, destinationKeys, destinationValues, copyUntil2,
                            ii - gallopLength2 + 1, gallopLength2);
                    chunkCursor -= gallopLength2;
                    ii -= gallopLength2;

                    if (chunkCursor < 0) {
                        break no_data_left;
                    }

                    val2 = valuesChunk.get(chunkCursor);

                    minGallop--;
                }

                // if we had a lot of things from run1, we take the next thing from run2 and then find it in run1
                final long copyUntil1 = upperBound(destinationValues, mergeStartPosition, destCursor, val2);

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

                if (gallopLength1 < TimsortUtils.INITIAL_GALLOP
                        && gallopLength2 < TimsortUtils.INITIAL_GALLOP) {
                    minGallop += 2; // undo the possible subtraction from above
                    break;
                }
            }
        }

        if (chunkCursor >= 0) {
            copyChunkToDest(keysChunk, valuesChunk, destinationKeys, destinationValues, 0, ii - chunkCursor,
                    chunkCursor + 1);
        }
    }

    private static <ATTR extends Any, KEY_INDICES extends Indices> void copyChunkToDest(
            LongChunk<KEY_INDICES> keysChunk, FloatChunk<ATTR> valuesChunk, LongArraySource destinationKeys,
            FloatArraySource destinationValues, int sourceStart, long destStart, int length) {
        destinationValues.copyFromChunk(destStart, length, (FloatChunk) valuesChunk, sourceStart);
        destinationKeys.copyFromChunk(destStart, length, keysChunk, sourceStart);
    }

    private static void moveInDest(LongArraySource destinationKeys, FloatArraySource destinationValues,
            long sourceStart, long destStart, long length) {
        destinationKeys.move(sourceStart, destStart, length);
        destinationValues.move(sourceStart, destStart, length);
    }

    // region comparison functions
    private static int doComparison(float lhs, float rhs) {
        return FloatComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean geq(float lhs, float rhs) {
        return doComparison(lhs, rhs) >= 0;
    }

    // when we binary search in 1, we must identify a position for search value that is *after* our test values;
    // because the values from run 2 may never be inserted before an equal value from run 1
    //
    // lo is inclusive, hi is exclusive
    //
    // returns the position of the first element that is > searchValue or hi if there is no such element
    private static long upperBound(FloatArraySource values, long lo, long hi, float searchValue) {
        return bound(values, lo, hi, searchValue, false);
    }

    private static long bound(FloatArraySource valuesToSort, long lo, long hi, float searchValue,
            @SuppressWarnings("SameParameterValue") final boolean lower) {
        final int compareLimit = lower ? -1 : 0; // lt or leq

        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            final float testValue = valuesToSort.getUnsafe(mid);
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

    // when we binary search in 2, we must identify a position for search value that is *before* our test values;
    // because the values from run 1 may never be inserted after an equal value from run 2
    private static int lowerBound(FloatChunk<?> valuesToSort, @SuppressWarnings("SameParameterValue") int lo, int hi,
            float searchValue) {
        return bound(valuesToSort, lo, hi, searchValue, true);
    }

    private static int bound(FloatChunk<?> valuesToSort, int lo, int hi, float searchValue,
            @SuppressWarnings("SameParameterValue") final boolean lower) {
        final int compareLimit = lower ? -1 : 0; // lt or leq

        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final float testValue = valuesToSort.get(mid);
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
}
