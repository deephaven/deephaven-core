//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharTimsortKernel and run "./gradlew replicateSortKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sort.timsort;

import java.util.Objects;
import io.deephaven.util.compare.ObjectComparisons;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.Context;
import io.deephaven.util.annotations.VisibleForTesting;

/**
 * This implements a timsort kernel for Objects.
 * <p>
 * <a href="https://bugs.python.org/file4451/timsort.txt">Python</a> and
 * <a href="https://en.wikipedia.org/wiki/Timsort">Wikipedia</a> do a decent job of describing the algorithm.
 */
public class ObjectTimsortKernel {
    private ObjectTimsortKernel() {
        throw new UnsupportedOperationException();
    }

    // region Context
    public static class ObjectSortKernelContext<ATTR extends Any> implements Context {
        int minGallop;
        int runCount = 0;
        private final int[] runStarts;
        private final int[] runLengths;
        private final WritableObjectChunk<Object, ATTR> temporaryValues;

        private ObjectSortKernelContext(int size) {
            temporaryValues = WritableObjectChunk.makeWritableChunk((size + 2) / 2);
            runStarts = new int[(size + 31) / 32];
            runLengths = new int[(size + 31) / 32];
            minGallop = TimsortUtils.INITIAL_GALLOP;
        }

        public void sort(WritableChunk<ATTR> valuesToSort) {
            ObjectTimsortKernel.sort(this, valuesToSort.asWritableObjectChunk());
        }

        public void close() {
            temporaryValues.close();
        }
    }
    // endregion Context

    public static <ATTR extends Any> ObjectSortKernelContext<ATTR> createContext(int size) {
        return new ObjectSortKernelContext<>(size);
    }

    /**
     * Sort the values in valuesToSort permuting the indexKeys chunk in the same way.
     * <p>
     * The offsetsIn chunk is contains the offset of runs to sort in indexKeys; and the lengthsIn contains the length of
     * the runs. This allows the kernel to be used for a secondary column sort, chaining it together with fewer runs
     * sorted on each pass.
     */
    public static <ATTR extends Any> void sort(
            final ObjectSortKernelContext<ATTR> context,
            final WritableObjectChunk<Object, ATTR> valuesToSort) {
        timSort(context, valuesToSort, 0, valuesToSort.size());
    }

    static private <ATTR extends Any> void timSort(
            final ObjectSortKernelContext<ATTR> context,
            final WritableObjectChunk<Object, ATTR> valuesToSort,
            final int offset,
            final int length) {
        if (length <= 1) {
            return;
        }

        final int minRun = TimsortUtils.getRunLength(length);

        if (length <= minRun) {
            insertionSort(valuesToSort, offset, length);
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
                insertionSort(valuesToSort, startRun, actualLength);
                context.runLengths[context.runCount] = actualLength;
                startRun += actualLength;
            } else {
                if (descending) {
                    // reverse the current run
                    for (int ii = 0; ii < foundLength / 2; ++ii) {
                        swap(valuesToSort, ii + startRun, endRun - ii - 1);
                    }
                }
                // now an ascending run
                context.runLengths[context.runCount] = foundLength;
                startRun = endRun;
            }

            context.runCount++;

            // check the invariants at the top of the stack
            ensureMergeInvariants(context, valuesToSort);
        }

        while (context.runCount > 1) {
            final int length2 = context.runLengths[context.runCount - 1];
            final int start1 = context.runStarts[context.runCount - 2];
            final int length1 = context.runLengths[context.runCount - 2];
            merge(context, valuesToSort, start1, length1, length2);
            context.runStarts[context.runCount - 2] = start1;
            context.runLengths[context.runCount - 2] = length1 + length2;
            context.runCount--;
        }
    }

    // region comparison functions
    // ascending comparison
    private static int doComparison(Object lhs, Object rhs) {
        return ObjectComparisons.compare(lhs, rhs);
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
     * </p>
     *
     * <p>
     * Concurrently with the search for runs, the runs are merged with mergesort. Except where Timsort tries to optimise
     * for merging disjoint runs in galloping mode, runs are repeatedly merged two at a time, with the only concerns
     * being to maintain stability and merge balance.
     * </p>
     *
     * <p>
     * Stability requires non-consecutive runs are not merged, as elements could be transferred across equal elements in
     * the intervening run, violating stability. Further, it would be impossible to recover the order of the equal
     * elements at a later point.
     * </p>
     *
     * <p>
     * In pursuit of balanced merges, Timsort considers three runs on the top of the stack, X, Y, Z, and maintains the
     * invariants:
     *
     * <ul>
     * <li>|Z| > |Y| + |X|</li>
     * <li>|Y| > |X|</li>
     * </ul>
     *
     * If the invariants are violated, Y is merged with the smaller of X or Z and the invariants are checked again. Once
     * the invariants hold, the next run is formed.
     * </p>
     *
     * <p>
     * Somewhat inappreciably, the invariants maintain merges as being approximately balanced while maintaining a
     * compromise between delaying merging for balance, and exploiting fresh occurrence of runs in cache memory, and
     * also making merge decisions relatively simple.
     * </p>
     *
     * <p>
     * On reaching the end of the data, Timsort repeatedly merges the two runs on the top of the stack, until only one
     * run of the entire data remains.
     * </p>
     */
    private static <ATTR extends Any> void ensureMergeInvariants(
            final ObjectSortKernelContext<ATTR> context,
            final WritableObjectChunk<Object, ATTR> valuesToSort) {
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
                merge(context, valuesToSort, yStart, yLen, xLen);

                // unchanged: context.runStarts[yStart];
                context.runLengths[yIndex] += xLen;
            } else {
                // merge y and z
                final int zStart = context.runStarts[zIndex];
                merge(context, valuesToSort, zStart, zLen, yLen);

                // unchanged: context.runStarts[zIndex];
                context.runLengths[zIndex] += yLen;
                context.runStarts[yIndex] = xStart;
                context.runLengths[yIndex] = xLen;
            }
            context.runCount--;

        }
    }

    private static <ATTR extends Any> void merge(
            final ObjectSortKernelContext<ATTR> context,
            final WritableObjectChunk<Object, ATTR> valuesToSort,
            final int start1,
            final int length1,
            final int length2) {
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
            copyToTemporary(context, valuesToSort, mergeStartPosition, remaining1);
            // now we need to do the merge from temporary and remaining2 into remaining1 (so start at the front, because
            // we've preserved all the values of run1
            frontMerge(context, valuesToSort, mergeStartPosition, start2, remaining2);
        } else {
            copyToTemporary(context, valuesToSort, start2, remaining2);
            // now we need to do the merge from temporary and remaining1 into the remaining two area (so start at the
            // back, because we've preserved all the values of run2)
            backMerge(context, valuesToSort, mergeStartPosition, remaining1);
        }
    }

    /**
     * Merge context temporary and run2 between mergeStartPosition and length2 (which is not the full run length, but
     * the length of things we might need to merge.
     * <p>
     * We eventually need to do galloping here, but are skipping that for now
     */
    private static <ATTR extends Any> void frontMerge(
            final ObjectSortKernelContext<ATTR> context,
            final WritableObjectChunk<Object, ATTR> valuesToSort,
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

        nodataleft: while (ii < mergeEndExclusive) {
            int run1wins = 0;
            int run2wins = 0;

            if (context.minGallop < 2) {
                context.minGallop = 2;
            }

            while (run1wins < context.minGallop && run2wins < context.minGallop) {
                if (leq(val1, val2)) {
                    valuesToSort.set(ii++, val1);

                    if (++tempCursor == run1size) {
                        break nodataleft;
                    }

                    val1 = context.temporaryValues.get(tempCursor);
                    run1wins++;
                    run2wins = 0;
                } else {
                    valuesToSort.set(ii++, val2);

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
                    copyToChunk(context.temporaryValues, valuesToSort, tempCursor, ii, gallopLength1);
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
                    copyToChunk(valuesToSort, valuesToSort, run2Cursor, ii, gallopLength2);
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
            tempCursor++;
            ii++;
        }
    }

    /**
     * Merge context temporary and run1 between mergeStartPosition + length1 + temporary.length
     * <p>
     * We eventually need to do galloping here, but are skipping that for now
     */
    private static <ATTR extends Any> void backMerge(
            final ObjectSortKernelContext<ATTR> context,
            final WritableObjectChunk<Object, ATTR> valuesToSort,
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

        nodataleft: while (ii >= mergeStartPosition) {
            int run1wins = 0;
            int run2wins = 0;

            if (context.minGallop < 2) {
                context.minGallop = 2;
            }

            while (run1wins < context.minGallop && run2wins < context.minGallop) {
                if (geq(val2, val1)) {
                    valuesToSort.set(ii--, val2);

                    if (--tempCursor < 0) {
                        break nodataleft;
                    }
                    val2 = context.temporaryValues.get(tempCursor);

                    run2wins++;
                    run1wins = 0;
                } else {
                    valuesToSort.set(ii--, val1);

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
                    copyToChunk(context.temporaryValues, valuesToSort, copyUntil2, ii - gallopLength2 + 1,
                            gallopLength2);
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
                    copyToChunk(valuesToSort, valuesToSort, copyUntil1, ii - gallopLength1, gallopLength1 + 1);
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
            tempCursor--;
            ii--;
        }
    }

    private static <ATTR extends Any> void copyToTemporary(
            final ObjectSortKernelContext<ATTR> context,
            final WritableObjectChunk<Object, ATTR> valuesToSort,
            final int mergeStartPosition,
            final int remaining1) {
        context.temporaryValues.setSize(remaining1);
        context.temporaryValues.copyFromChunk(valuesToSort, mergeStartPosition, 0, remaining1);
    }

    private static <ATTR extends Any> void copyToChunk(
            final ObjectChunk<Object, ATTR> valuesSource,
            final WritableObjectChunk<Object, ATTR> valuesDest,
            final int sourceStart,
            final int destStart,
            final int length) {
        valuesDest.copyFromChunk(valuesSource, sourceStart, destStart, length);
    }

    // when we binary search in 1, we must identify a position for search value that is *after* our test values;
    // because the values from run 2 may never be inserted before an equal value from run 1
    //
    // lo is inclusive, hi is exclusive
    //
    // returns the position of the first element that is > searchValue or hi if there is no such element
    private static int upperBound(
            final ObjectChunk<Object, ?> valuesToSort,
            final int lo,
            final int hi,
            final Object searchValue) {
        return bound(valuesToSort, lo, hi, searchValue, false);
    }

    // when we binary search in 2, we must identify a position for search value that is *before* our test values;
    // because the values from run 1 may never be inserted after an equal value from run 2
    private static int lowerBound(
            final ObjectChunk<Object, ?> valuesToSort,
            final int lo,
            final int hi,
            final Object searchValue) {
        return bound(valuesToSort, lo, hi, searchValue, true);
    }

    private static int bound(
            final ObjectChunk<Object, ?> valuesToSort,
            int lo,
            int hi,
            final Object searchValue,
            final boolean lower) {
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
            final WritableObjectChunk<Object, ?> valuesToSort,
            final int offset,
            final int length) {
        // this could eventually be done with intrinsics (AVX 512/64 bits for byte keys == 16 elements, and can be
        // combined up to 256)
        for (int ii = offset + 1; ii < offset + length; ++ii) {
            for (int jj = ii; jj > offset && gt(valuesToSort.get(jj - 1), valuesToSort.get(jj)); jj--) {
                swap(valuesToSort, jj, jj - 1);
            }
        }
    }

    static private void swap(
            final WritableObjectChunk<Object, ?> valuesToSort,
            final int a,
            final int b) {
        final Object tempObject = valuesToSort.get(a);
        valuesToSort.set(a, valuesToSort.get(b));
        valuesToSort.set(b, tempObject);
    }
}
