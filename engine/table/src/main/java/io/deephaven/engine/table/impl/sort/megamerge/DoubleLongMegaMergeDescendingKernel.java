/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharLongMegaMergeKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sort.megamerge;

import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.engine.table.impl.sort.LongMegaMergeKernel;
import io.deephaven.engine.table.impl.sort.timsort.TimsortUtils;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Indices;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;

public class DoubleLongMegaMergeDescendingKernel {
    private DoubleLongMegaMergeDescendingKernel() {
        throw new UnsupportedOperationException();
    }

    // region Context
    public static class DoubleLongMegaMergeDescendingKernelContext<ATTR extends Any, KEY_INDICES extends Indices>
            implements LongMegaMergeKernel<ATTR, KEY_INDICES> {
        @SuppressWarnings("rawtypes")
        private static final DoubleLongMegaMergeDescendingKernelContext INSTANCE = new DoubleLongMegaMergeDescendingKernelContext();

        @Override
        public void merge(LongArraySource indexDestinationSource, ArrayBackedColumnSource<?> valuesDestinationSource,
                long destinationOffset, long destinationSize, LongChunk<KEY_INDICES> indexKeys,
                Chunk<ATTR> valuesToMerge) {
            DoubleLongMegaMergeDescendingKernel.merge(indexDestinationSource, (DoubleArraySource) valuesDestinationSource,
                    destinationOffset, destinationSize, indexKeys, valuesToMerge.asDoubleChunk());
        }
    }
    // endregion Context

    public static <ATTR extends Any, KEY_INDICES extends Indices> DoubleLongMegaMergeDescendingKernelContext<ATTR, KEY_INDICES> createContext() {
        // noinspection unchecked
        return DoubleLongMegaMergeDescendingKernelContext.INSTANCE;
    }

    static public <ATTR extends Any, KEY_INDICES extends Indices> void merge(LongArraySource destinationKeys,
            DoubleArraySource destinationValues,
            long destinationOffset,
            long destinationSize,
            LongChunk<KEY_INDICES> keysChunk,
            DoubleChunk<ATTR> valuesChunk) {
        destinationKeys.ensureCapacity(destinationOffset + destinationSize + keysChunk.size(), false);
        destinationValues.ensureCapacity(destinationOffset + destinationSize + valuesChunk.size(), false);

        // find the location of run2[0] in run1
        final double run2lo = valuesChunk.get(0);
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

        double val1 = destinationValues.getUnsafe(destCursor);
        double val2 = valuesChunk.get(chunkCursor);

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
            LongChunk<KEY_INDICES> keysChunk, DoubleChunk<ATTR> valuesChunk, LongArraySource destinationKeys,
            DoubleArraySource destinationValues, int sourceStart, long destStart, int length) {
        destinationValues.copyFromChunk(destStart, length, (DoubleChunk) valuesChunk, sourceStart);
        destinationKeys.copyFromChunk(destStart, length, keysChunk, sourceStart);
    }

    private static void moveInDest(LongArraySource destinationKeys, DoubleArraySource destinationValues,
            long sourceStart, long destStart, long length) {
        destinationKeys.move(sourceStart, destStart, length);
        destinationValues.move(sourceStart, destStart, length);
    }

    // region comparison functions
    // note that this is a descending kernel, thus the comparisons here are backwards (e.g., the lt function is in terms of the sort direction, so is implemented by gt)
    private static int doComparison(double lhs, double rhs) {
        return -1 * DoubleComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean geq(double lhs, double rhs) {
        return doComparison(lhs, rhs) >= 0;
    }

    // when we binary search in 1, we must identify a position for search value that is *after* our test values;
    // because the values from run 2 may never be inserted before an equal value from run 1
    //
    // lo is inclusive, hi is exclusive
    //
    // returns the position of the first element that is > searchValue or hi if there is no such element
    private static long upperBound(DoubleArraySource values, long lo, long hi, double searchValue) {
        return bound(values, lo, hi, searchValue, false);
    }

    private static long bound(DoubleArraySource valuesToSort, long lo, long hi, double searchValue,
            @SuppressWarnings("SameParameterValue") final boolean lower) {
        final int compareLimit = lower ? -1 : 0; // lt or leq

        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            final double testValue = valuesToSort.getUnsafe(mid);
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
    private static int lowerBound(DoubleChunk<?> valuesToSort, @SuppressWarnings("SameParameterValue") int lo, int hi,
            double searchValue) {
        return bound(valuesToSort, lo, hi, searchValue, true);
    }

    private static int bound(DoubleChunk<?> valuesToSort, int lo, int hi, double searchValue,
            @SuppressWarnings("SameParameterValue") final boolean lower) {
        final int compareLimit = lower ? -1 : 0; // lt or leq

        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final double testValue = valuesToSort.get(mid);
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
