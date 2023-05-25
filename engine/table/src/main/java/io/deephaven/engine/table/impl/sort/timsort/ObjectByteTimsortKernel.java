/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharByteTimsortKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ObjectLongTimsortKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sort.timsort;

import java.util.Objects;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.table.impl.sort.ByteSortKernel;
import io.deephaven.chunk.*;
import io.deephaven.util.annotations.VisibleForTesting;

/**
 * This implements a timsort kernel for Objects.
 * <p>
 * <a href="https://bugs.python.org/file4451/timsort.txt">bugs.python.org</a> and
 * <a href="https://en.wikipedia.org/wiki/Timsort">Wikipedia</a> do a decent job of describing the algorithm.
 */
public class ObjectByteTimsortKernel {
    private ObjectByteTimsortKernel() {
        throw new UnsupportedOperationException();
    }

    // region Context
    public static class ObjectByteSortKernelContext<SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any>
            implements ByteSortKernel<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> {

        int minGallop;
        int runCount = 0;
        private final int[] runStarts;
        private final int[] runLengths;
        private final WritableByteChunk<PERMUTE_VALUES_ATTR> temporaryKeys;
        private final WritableObjectChunk<Object, SORT_VALUES_ATTR> temporaryValues;

        private ObjectByteSortKernelContext(int size) {
            temporaryKeys = WritableByteChunk.makeWritableChunk((size + 2) / 2);
            temporaryValues = WritableObjectChunk.makeWritableChunk((size + 2) / 2);
            runStarts = new int[(size + 31) / 32];
            runLengths = new int[(size + 31) / 32];
            minGallop = TimsortUtils.INITIAL_GALLOP;
        }

        @Override
        public void sort(
                WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableChunk<SORT_VALUES_ATTR> valuesToSort) {
            ObjectByteTimsortKernel.sort(this, valuesToPermute, valuesToSort.asWritableObjectChunk());
        }

        @Override
        public void sort(
                WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableChunk<SORT_VALUES_ATTR> valuesToSort,
                IntChunk<? extends ChunkPositions> offsetsIn,
                IntChunk<? extends ChunkLengths> lengthsIn) {
            ObjectByteTimsortKernel.sort(this, valuesToPermute, valuesToSort.asWritableObjectChunk(), offsetsIn, lengthsIn);
        }

        @Override
        public void close() {
            temporaryKeys.close();
            temporaryValues.close();
        }
    }
    // endregion Context

    public static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any>
    ObjectByteSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> createContext(int size) {
        return new ObjectByteSortKernelContext<>(size);
    }

    /**
     * Sort the values in valuesToSort permuting the valuesToPermute chunk in the same way.
     * <p>
     * The offsetsIn chunk is contains the offset of runs to sort in valuesToPermute; and the lengthsIn contains the
     * length of the runs. This allows the kernel to be used for a secondary column sort, chaining it together with
     * fewer runs sorted on each pass.
     */
    static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void sort(
            ObjectByteSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableObjectChunk<Object, SORT_VALUES_ATTR> valuesToSort,
            IntChunk<? extends ChunkPositions> offsetsIn,
            IntChunk<? extends ChunkLengths> lengthsIn) {
        final int numberRuns = offsetsIn.size();
        for (int run = 0; run < numberRuns; ++run) {
            final int offset = offsetsIn.get(run);
            final int length = lengthsIn.get(run);

            timSort(context, valuesToPermute, valuesToSort, offset, length);
        }
    }

    /**
     * Sort the values in valuesToSort permuting the valuesToPermute chunk in the same way.
     * <p>
     * The offsetsIn chunk is contains the offset of runs to sort in valuesToPermute; and the lengthsIn contains the
     * length of the runs. This allows the kernel to be used for a secondary column sort, chaining it together with
     * fewer runs sorted on each pass.
     */
    public static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void sort(
            ObjectByteSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableObjectChunk<Object, SORT_VALUES_ATTR> valuesToSort) {
        timSort(context, valuesToPermute, valuesToSort, 0, valuesToPermute.size());
    }

    static private <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void timSort(
            ObjectByteSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableObjectChunk<Object, SORT_VALUES_ATTR> valuesToSort,
            int offset,
            int length) {
        if (length <= 1) {
            return;
        }

        final int minRun = TimsortUtils.getRunLength(length);

        if (length <= minRun) {
            insertionSort(valuesToPermute, valuesToSort, offset, length);
            return;
        }

        context.runCount = 0;

        int startRun = offset;
        while (startRun < offset + length) {
            Object current = valuesToSort.get(startRun);

            int endRun; // note that endrun is exclusive
            final boolean descending;

            if (startRun + 1 == offset + length) {
                endRun = offset + length;
                descending = false;
            } else {
                Object next = valuesToSort.get(startRun + 1);
                endRun = startRun + 2;
                descending = gt(current, next);

                if (!descending) {
                    // search for a non-descending run
                    current = next;
                    while (endRun < length && geq(next = valuesToSort.get(endRun), current)) {
                        current = next;
                        endRun++;
                    }
                } else {
                    // search for a strictly descending run; we can not have any equal values, or we will break the
                    // sort's stability guarantee
                    current = next;
                    while (endRun < length && lt(next = valuesToSort.get(endRun), current)) {
                        current = next;
                        endRun++;
                    }
                }
            }

            final int foundLength = endRun - startRun;
            context.runStarts[context.runCount] = startRun;
            if (foundLength < minRun) {
                // increase the size of the run to the minimum run
                final int actualLength = Math.min(minRun, length - (startRun - offset));
                insertionSort(valuesToPermute, valuesToSort, startRun, actualLength);
                context.runLengths[context.runCount] = actualLength;
                startRun += actualLength;
            } else {
                if (descending) {
                    // reverse the current run
                    for (int ii = 0; ii < foundLength / 2; ++ii) {
                        swap(valuesToPermute, valuesToSort, ii + startRun, endRun - ii - 1);
                    }
                }
                // now an ascending run
                context.runLengths[context.runCount] = foundLength;
                startRun = endRun;
            }

            context.runCount++;

            // check the invariants at the top of the stack
            ensureMergeInvariants(context, valuesToPermute, valuesToSort);
        }

        while (context.runCount > 1) {
            final int length2 = context.runLengths[context.runCount - 1];
            final int start1 = context.runStarts[context.runCount - 2];
            final int length1 = context.runLengths[context.runCount - 2];
            merge(context, valuesToPermute, valuesToSort, start1, length1, length2);
            context.runStarts[context.runCount - 2] = start1;
            context.runLengths[context.runCount - 2] = length1 + length2;
            context.runCount--;
        }
    }

    // region comparison functions
    // ascending comparison
    private static int doComparison(Object lhs, Object rhs) {
       if (lhs == rhs) {
            return 0;
        }
        if (lhs == null) {
            return -1;
        }
        if (rhs == null) {
            return 1;
        }
        //noinspection unchecked,rawtypes
        return ((Comparable)lhs).compareTo(rhs);
    }

    // endregion comparison functions

    @VisibleForTesting
    static boolean gt(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) > 0;
    }

    @VisibleForTesting
    static boolean lt(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) < 0;
    }

    @VisibleForTesting
    static boolean geq(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) >= 0;
    }

    @VisibleForTesting
    static boolean leq(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    /**
     * <p>
     * There are two merge invariants that we must preserve, quoting from Wikipedia:
     * <p>
     * Timsort is a stable sorting algorithm (order of elements with same key is kept) and strives to perform balanced
     * merges (a merge thus merges runs of similar sizes).
     * <p>
     * In order to achieve sorting stability, only consecutive runs are merged. Between two non-consecutive runs, there
     * can be an element with the same key inside the runs. Merging those two runs would change the order of equal keys.
     * Example of this situation ([] are ordered runs): [1 2 2] 1 4 2 [0 1 2]
     * <p>
     * In pursuit of balanced merges, Timsort considers three runs on the top of the stack, X, Y, Z, and maintains the
     * invariants:
     * <ul>
     * <li>|Z| > |Y| + |X|</li>
     * <li>|Y| > |X|</li>
     * </ul>
     * <p>
     * If any of these invariants is violated, Y is merged with the smaller of X or Z and the invariants are checked
     * again. Once the invariants hold, the search for a new run in the data can start. These invariants maintain merges
     * as being approximately balanced while maintaining a compromise between delaying merging for balance, exploiting
     * fresh occurrence of runs in cache memory and making merge decisions relatively simple.
     */
    private static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void ensureMergeInvariants(
            ObjectByteSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableObjectChunk<Object, SORT_VALUES_ATTR> valuesToSort) {
        while (context.runCount > 1) {
            final int xIndex = context.runCount - 1;
            final int yIndex = context.runCount - 2;
            final int zIndex = context.runCount - 3;

            final int xLen = context.runLengths[xIndex];
            final int yLen = context.runLengths[yIndex];
            final int zLen = zIndex >= 0 ? context.runLengths[zIndex] : -1;

            final boolean xMerge;

            if (zLen >= 0 && (zLen <= yLen + xLen)) {
                // we must merge the smaller of the two
                xMerge = xLen < zLen;
            } else if (yLen < xLen) {
                // we must merge Y into X
                xMerge = true;
            } else {
                break;
            }

            final int yStart = context.runStarts[yIndex];
            final int xStart = context.runStarts[xIndex];
            if (xMerge) {
                // merge y and x
                merge(context, valuesToPermute, valuesToSort, yStart, yLen, xLen);

                // unchanged: context.runStarts[yStart];
                context.runLengths[yIndex] += xLen;
            } else {
                // merge y and z
                final int zStart = context.runStarts[zIndex];
                merge(context, valuesToPermute, valuesToSort, zStart, zLen, yLen);

                // unchanged: context.runStarts[zIndex];
                context.runLengths[zIndex] += yLen;
                context.runStarts[yIndex] = xStart;
                context.runLengths[yIndex] = xLen;
            }
            context.runCount--;

        }
    }

    private static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void merge(
            ObjectByteSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableObjectChunk<Object, SORT_VALUES_ATTR> valuesToSort,
            int start1,
            int length1,
            int length2) {
        // we know that we can never have zero length runs, because there is a minimum run size enforced; and at the
        // end of an input, we won't create a zero-length run. When we merge runs, they only become bigger, thus
        // they'll never be empty. I'm being cheap about function calls and control flow here.
        // Assert.gtZero(length1, "length1");
        // Assert.gtZero(length2, "length2");

        final int start2 = start1 + length1;
        // find the location of run2[0] in run1
        final Object run2lo = valuesToSort.get(start2);
        final int mergeStartPosition = upperBound(valuesToSort, start1, start1 + length1, run2lo);

        if (mergeStartPosition == start1 + length1) {
            // these two runs are sorted already
            return;
        }

        // find the location of run1[length1 - 1] in run2
        final Object run1hi = valuesToSort.get(start1 + length1 - 1);
        final int mergeEndPosition = lowerBound(valuesToSort, start2, start2 + length2, run1hi);

        // figure out which of the two runs is now shorter
        final int remaining1 = start1 + length1 - mergeStartPosition;
        final int remaining2 = mergeEndPosition - start2;

        if (remaining1 < remaining2) {
            copyToTemporary(context, valuesToPermute, valuesToSort, mergeStartPosition, remaining1);
            // now we need to do the merge from temporary and remaining2 into remaining1 (so start at the front,
            // because we've preserved all the values of run1
            frontMerge(context, valuesToPermute, valuesToSort, mergeStartPosition, start2, remaining2);
        } else {
            copyToTemporary(context, valuesToPermute, valuesToSort, start2, remaining2);
            // now we need to do the merge from temporary and remaining1 into the remaining two area (so start at the
            // back, because we've preserved all the values of run2)
            backMerge(context, valuesToPermute, valuesToSort, mergeStartPosition, remaining1);
        }
    }

    /**
     * Merge context temporary and run2 between mergeStartPosition and length2 (which is not the full run length, but
     * the length of things we might need to merge.
     * <p>
     * We eventually need to do galloping here, but are skipping that for now
     */
    private static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void frontMerge(
            ObjectByteSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableObjectChunk<Object, SORT_VALUES_ATTR> valuesToSort,
            final int mergeStartPosition,
            final int start2,
            final int length2) {
        int tempCursor = 0;
        int run2Cursor = start2;

        final int run1size = context.temporaryValues.size();
        int ii;
        final int mergeEndExclusive = start2 + length2;

        Object val1 = context.temporaryValues.get(tempCursor);
        Object val2 = valuesToSort.get(run2Cursor);

        ii = mergeStartPosition;

        nodataleft:
        while (ii < mergeEndExclusive) {
            int run1wins = 0;
            int run2wins = 0;

            if (context.minGallop < 2) {
                context.minGallop = 2;
            }

            while (run1wins < context.minGallop && run2wins < context.minGallop) {
                if (leq(val1, val2)) {
                    valuesToSort.set(ii, val1);
                    valuesToPermute.set(ii++, context.temporaryKeys.get(tempCursor));

                    if (++tempCursor == run1size) {
                        break nodataleft;
                    }

                    val1 = context.temporaryValues.get(tempCursor);
                    run1wins++;
                    run2wins = 0;
                } else {
                    valuesToSort.set(ii, val2);
                    valuesToPermute.set(ii++, valuesToPermute.get(run2Cursor));

                    if (++run2Cursor == mergeEndExclusive) {
                        break nodataleft;
                    }
                    val2 = valuesToSort.get(run2Cursor);

                    run2wins++;
                    run1wins = 0;
                }
            }

            // we are in galloping mode now, if we had run out of data then we should have already bailed out to
            // nodataleft
            while (ii < mergeEndExclusive) {
                // if we had a lot of things from run1, we take the next thing from run2 then find it in run1
                final int copyUntil1 = upperBound(context.temporaryValues, tempCursor, run1size, val2);
                final int gallopLength1 = copyUntil1 - tempCursor;
                if (gallopLength1 > 0) {
                    copyToChunk(context.temporaryKeys, context.temporaryValues, valuesToPermute, valuesToSort,
                            tempCursor, ii, gallopLength1);
                    tempCursor += gallopLength1;
                    ii += gallopLength1;

                    if (tempCursor == run1size) {
                        break nodataleft;
                    }
                    val1 = context.temporaryValues.get(tempCursor);

                    context.minGallop--;
                }

                // if we had a lot of things from run2, we take the next thing from run1 and then find it in run2
                final int copyUntil2 = lowerBound(valuesToSort, run2Cursor, mergeEndExclusive, val1);
                final int gallopLength2 = copyUntil2 - run2Cursor;
                if (gallopLength2 > 0) {
                    copyToChunk(valuesToPermute, valuesToSort, valuesToPermute, valuesToSort, run2Cursor, ii,
                            gallopLength2);
                    run2Cursor += gallopLength2;
                    ii += gallopLength2;

                    if (run2Cursor == mergeEndExclusive) {
                        break nodataleft;
                    }
                    val2 = valuesToSort.get(run2Cursor);

                    context.minGallop--;
                }

                if (gallopLength1 < TimsortUtils.INITIAL_GALLOP && gallopLength2 < TimsortUtils.INITIAL_GALLOP) {
                    context.minGallop += 2; // undo the possible subtraction from above
                    break;
                }
            }
        }

        while (tempCursor < run1size) {
            valuesToSort.set(ii, context.temporaryValues.get(tempCursor));
            valuesToPermute.set(ii, context.temporaryKeys.get(tempCursor));
            tempCursor++;
            ii++;
        }
    }

    /**
     * Merge context temporary and run1 between mergeStartPosition + length1 + temporary.length
     * <p>
     * We eventually need to do galloping here, but are skipping that for now
     */
    private static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void backMerge(
            ObjectByteSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableObjectChunk<Object, SORT_VALUES_ATTR> valuesToSort,
            final int mergeStartPosition,
            final int length1) {
        final int run1End = mergeStartPosition + length1;
        int run1Cursor = run1End - 1;
        int tempCursor = context.temporaryValues.size() - 1;

        final int mergeLength = context.temporaryValues.size() + length1;
        int ii;


        Object val1 = valuesToSort.get(run1Cursor);
        Object val2 = context.temporaryValues.get(tempCursor);

        final int mergeEnd = mergeStartPosition + mergeLength;
        ii = mergeEnd - 1;

        nodataleft:
        while (ii >= mergeStartPosition) {
            int run1wins = 0;
            int run2wins = 0;

            if (context.minGallop < 2) {
                context.minGallop = 2;
            }

            while (run1wins < context.minGallop && run2wins < context.minGallop) {
                if (geq(val2, val1)) {
                    valuesToSort.set(ii, val2);
                    valuesToPermute.set(ii--, context.temporaryKeys.get(tempCursor));

                    if (--tempCursor < 0) {
                        break nodataleft;
                    }
                    val2 = context.temporaryValues.get(tempCursor);

                    run2wins++;
                    run1wins = 0;
                } else {
                    valuesToSort.set(ii, val1);
                    valuesToPermute.set(ii--, valuesToPermute.get(run1Cursor));

                    if (--run1Cursor < mergeStartPosition) {
                        break nodataleft;
                    }
                    val1 = valuesToSort.get(run1Cursor);

                    run1wins++;
                    run2wins = 0;
                }
            }

            // we are in galloping mode now, if we had run out of data then we should have already bailed out to
            // nodataleft
            while (ii >= mergeStartPosition) {
                // if we had a lot of things from run2, we take the next thing from run1 then find it in run2
                final int copyUntil2 = lowerBound(context.temporaryValues, 0, tempCursor, val1) + 1;

                final int gallopLength2 = tempCursor - copyUntil2 + 1;
                if (gallopLength2 > 0) {
                    copyToChunk(context.temporaryKeys, context.temporaryValues, valuesToPermute, valuesToSort,
                            copyUntil2, ii - gallopLength2 + 1, gallopLength2);
                    tempCursor -= gallopLength2;
                    ii -= gallopLength2;

                    if (tempCursor < 0) {
                        break nodataleft;
                    }
                    val2 = context.temporaryValues.get(tempCursor);

                    context.minGallop--;
                }

                // if we had a lot of things from run1, we take the next thing from run2 and then find it in run1
                final int copyUntil1 = upperBound(valuesToSort, mergeStartPosition, run1Cursor, val2);

                final int gallopLength1 = run1Cursor - copyUntil1;
                if (gallopLength1 > 0) {
                    copyToChunk(valuesToPermute, valuesToSort, valuesToPermute, valuesToSort,
                            copyUntil1, ii - gallopLength1, gallopLength1 + 1);
                    run1Cursor -= gallopLength1;
                    ii -= gallopLength1;

                    if (run1Cursor < mergeStartPosition) {
                        break nodataleft;
                    }
                    val1 = valuesToSort.get(run1Cursor);

                    context.minGallop--;
                }

                if (gallopLength1 < TimsortUtils.INITIAL_GALLOP && gallopLength2 < TimsortUtils.INITIAL_GALLOP) {
                    context.minGallop += 2; // undo the possible subtraction from above
                    break;
                }
            }
        }

        while (tempCursor >= 0) {
            valuesToSort.set(ii, context.temporaryValues.get(tempCursor));
            valuesToPermute.set(ii, context.temporaryKeys.get(tempCursor));
            tempCursor--;
            ii--;
        }
    }

    private static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void copyToTemporary(
            ObjectByteSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableObjectChunk<Object, SORT_VALUES_ATTR> valuesToSort,
            int mergeStartPosition,
            int remaining1) {
        context.temporaryValues.setSize(remaining1);
        context.temporaryKeys.setSize(remaining1);

        context.temporaryValues.copyFromChunk(valuesToSort, mergeStartPosition, 0, remaining1);
        context.temporaryKeys.copyFromChunk(valuesToPermute, mergeStartPosition, 0, remaining1);
    }

    private static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void copyToChunk(
            ByteChunk<PERMUTE_VALUES_ATTR> rowSetSource,
            ObjectChunk<Object, SORT_VALUES_ATTR> valuesSource,
            WritableByteChunk<PERMUTE_VALUES_ATTR> permuteValuesDest,
            WritableObjectChunk<Object, SORT_VALUES_ATTR> sortValuesDest,
            int sourceStart,
            int destStart,
            int length) {
        sortValuesDest.copyFromChunk(valuesSource, sourceStart, destStart, length);
        permuteValuesDest.copyFromChunk(rowSetSource, sourceStart, destStart, length);
    }

    // when we binary search in 1, we must identify a position for search value that is *after* our test values;
    // because the values from run 2 may never be inserted before an equal value from run 1
    //
    // lo is inclusive, hi is exclusive
    //
    // returns the position of the first element that is > searchValue or hi if there is no such element
    private static int upperBound(ObjectChunk<Object, ?> valuesToSort, int lo, int hi, Object searchValue) {
        return bound(valuesToSort, lo, hi, searchValue, false);
    }

    // when we binary search in 2, we must identify a position for search value that is *before* our test values;
    // because the values from run 1 may never be inserted after an equal value from run 2
    private static int lowerBound(ObjectChunk<Object, ?> valuesToSort, int lo, int hi, Object searchValue) {
        return bound(valuesToSort, lo, hi, searchValue, true);
    }

    private static int bound(ObjectChunk<Object, ?> valuesToSort, int lo, int hi, Object searchValue, final boolean lower) {
        final int compareLimit = lower ? -1 : 0; // lt or leq

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

    private static void insertionSort(
            WritableByteChunk<?> valuesToPermute,
            WritableObjectChunk<Object, ?> valuesToSort,
            int offset,
            int length) {
        // this could eventually be done with intrinsics (AVX 512/64 bits for byte keys == 16 elements, and can be
        // combined up to 256)
        for (int ii = offset + 1; ii < offset + length; ++ii) {
            for (int jj = ii; jj > offset && gt(valuesToSort.get(jj - 1), valuesToSort.get(jj)); jj--) {
                swap(valuesToPermute, valuesToSort, jj, jj - 1);
            }
        }
    }

    static private void swap(WritableByteChunk<?> valuesToPermute, WritableObjectChunk<Object, ?> valuesToSort, int a, int b) {
        final byte tempPermuteValue = valuesToPermute.get(a);
        final Object tempObject = valuesToSort.get(a);

        valuesToPermute.set(a, valuesToPermute.get(b));
        valuesToSort.set(a, valuesToSort.get(b));

        valuesToPermute.set(b, tempPermuteValue);
        valuesToSort.set(b, tempObject);
    }

//    private static void doCheck(Chunk.ByteChunk valuesToPermute, Chunk.ObjectChunk valuesToSort, int startCheck, int mergeEnd) {
//        Object lastCheck;
//        lastCheck = valuesToSort.get(startCheck);
//        for (int jj = startCheck + 1; jj < mergeEnd; ++jj) {
//            final Object newCheck = valuesToSort.get(jj);
//            if (newCheck < lastCheck) {
//                dumpValues(valuesToSort, startCheck, mergeEnd - startCheck, "Bad loop at " + jj);
//                throw new IllegalStateException();
//            }
//            else if (newCheck == lastCheck) {
//                if (valuesToPermute.get(jj) < valuesToPermute.get(jj - 1)) {
//                    dumpValues(valuesToSort, startCheck, mergeEnd - startCheck, "Bad index loop at " + jj);
//                    dumpKeys(valuesToPermute, startCheck, mergeEnd - startCheck, "Bad index loop at " + jj);
//                    throw new IllegalStateException();
//                }
//            }
//            lastCheck = newCheck;
//        }
//        final StackTraceElement [] calls = new Exception().getStackTrace();
//        System.out.println("CHECK OK at " + calls[1]);
//        System.out.println();
//    }

//    private static void dumpValues(Chunk.ObjectChunk valuesToSort, int start1, int length1, int start2, int length2, String msg) {
//        System.out.println(msg + " merge (" + start1 + ", " + length1 + ") -> (" + start2 + ", " + length2 + ")");
//        Object last = valuesToSort.get(start1);
//        System.out.print("[" + format(last));
//        for (int ii = start1 + 1; ii < start2 + length2; ++ii) {
//            final Object current = valuesToSort.get(ii);
//            if (current < last) {
//                System.out.println("****");
//            }
//            last = current;
//            System.out.print(", " + last);
//        }
//        System.out.println("]");
//    }

//    private static void dumpValues(Chunk.ObjectChunk valuesToSort, int start1, int length1, String msg) {
//        dumpValues(valuesToSort, start1, length1, msg, -1);
//    }
//
//    private static void dumpValues(Chunk.ObjectChunk valuesToSort, int start1, int length1, String msg, int highlight) {
//        System.out.println(msg + " (" + start1 + ", " + length1 + ")");
//        Object last = valuesToSort.get(start1);
//        System.out.print(String.format("%04d", start1) + "   ");
//
//        System.out.print(format(last, highlight == start1));
//        boolean doComma = true;
//        for (int ii = start1 + 1; ii < start1 + length1; ) {
//            final Object current = valuesToSort.get(ii);
//            if (current < last) {
//                System.out.println("****");
//            }
//            last = current;
//            System.out.print((doComma ? ", " : "") + format(last, highlight == ii));
//            doComma = true;
//            ++ii;
//            if ((ii - start1) % 20 == 0) {
//                System.out.println();
//                System.out.print(String.format("%04d", ii) + "   ");
//                doComma = false;
//            } else if ((ii - start1) % 10 == 0) {
//                System.out.print("   " + String.format("%04d", ii) + "   ");
//                doComma = false;
//            }
//        }
//        System.out.println();
//    }
//    private static void dumpKeys(Chunk.ByteChunk keysToSort, int start1, int length1, String msg) {
//        System.out.println(msg + " (" + start1 + ", " + length1 + ")");
//        byte last = keysToSort.get(start1);
//        System.out.print(String.format("%04d", start1) + "   ");
//
//        System.out.print(format(last));
//        boolean doComma = true;
//        for (int ii = start1 + 1; ii < start1 + length1; ) {
//            last = keysToSort.get(ii);
//            System.out.print((doComma ? ", " : "") + format(last));
//            doComma = true;
//            ++ii;
//            if ((ii - start1) % 20 == 0) {
//                System.out.println();
//                System.out.print(String.format("%04d", ii) + "   ");
//                doComma = false;
//            } else if ((ii - start1) % 10 == 0) {
//                System.out.print("   " + String.format("%04d", ii) + "   ");
//                doComma = false;
//            }
//        }
//        System.out.println();
//    }
//
//    private static String format(Object last) {
//        if (last >= 'A' && last <= 'Z') {
//            return Object.toString(last);
//        }
//        return String.format("0x%04x", (int) last);
//    }
//
//    private static String format(Object last, boolean highlight) {
//        return highlight ? "/" + format(last) + "/" : format(last);
//    }
//
//    private static String format(byte last) {
//        return String.format("0x%04d", last);
//    }
}
