//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run GenerateTimsortKernels or ./gradlew generateTimsortKernels to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sort.timsort2.multi;

import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.table.impl.sort.MultiColumnSortKernel;
import io.deephaven.engine.table.impl.sort.timsort.TimsortUtils;
import io.deephaven.util.compare.FloatComparisons;
import java.lang.Integer;
import java.lang.Override;
import java.lang.SuppressWarnings;
import java.lang.UnsupportedOperationException;

/**
 * This implements a timsort kernel that sorts a multi-column key (Float, Int), comparing each
 * column in turn, while permuting a parallel chunk of long row keys.
 * <p>
 * <a href="https://bugs.python.org/file4451/timsort.txt">bugs.python.org</a> and
 * <a href="https://en.wikipedia.org/wiki/Timsort">Wikipedia</a> do a decent job of describing the algorithm.
 */
public final class FloatIntMultiColumnTimsortKernel {
    private FloatIntMultiColumnTimsortKernel() {
        throw new UnsupportedOperationException();
    }

    public static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> FloatIntMultiColumnSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> createContext(
            final int size) {
        return new FloatIntMultiColumnSortKernelContext<>(size);
    }

    /**
     * Sort the values in valuesToSort permuting the valuesToPermute chunk in the same way.
     * <p>
     * The offsetsIn chunk is contains the offset of runs to sort in valuesToPermute; and the lengthsIn contains the
     * length of the runs. This allows the kernel to be used for a secondary column sort, chaining it together with
     * fewer runs sorted on each pass.
     */
    static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void sort(
            FloatIntMultiColumnSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableFloatChunk<SORT_VALUES_ATTR> valuesToSort0,
            WritableIntChunk<SORT_VALUES_ATTR> valuesToSort1,
            IntChunk<? extends ChunkPositions> offsetsIn,
            IntChunk<? extends ChunkLengths> lengthsIn) {
        final int numberRuns = offsetsIn.size();
        for (int run = 0; run < numberRuns; ++run) {
            final int offset = offsetsIn.get(run);
            final int length = lengthsIn.get(run);

            timSort(context, valuesToPermute, valuesToSort0, valuesToSort1, offset, length);
        }
    }

    /**
     * Sort the values in the valuesToSort chunks, comparing each column in turn, permuting the valuesToPermute
     * chunk in the same way.
     */
    public static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void sort(
            FloatIntMultiColumnSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableFloatChunk<SORT_VALUES_ATTR> valuesToSort0,
            WritableIntChunk<SORT_VALUES_ATTR> valuesToSort1) {
        timSort(context, valuesToPermute, valuesToSort0, valuesToSort1, 0, valuesToPermute.size());
    }

    private static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void timSort(
            FloatIntMultiColumnSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableFloatChunk<SORT_VALUES_ATTR> valuesToSort0,
            WritableIntChunk<SORT_VALUES_ATTR> valuesToSort1, int offset, int length) {
        if (length <= 1) {
            return;
        }

        final int minRun = TimsortUtils.getRunLength(length);

        if (length <= minRun) {
            insertionSort(valuesToPermute, valuesToSort0, valuesToSort1, offset, length);
            return;
        }

        context.runCount = 0;

        int startRun = offset;
        while (startRun < offset + length) {
            float current0 = valuesToSort0.get(startRun);

            int endRun; // note that endrun is exclusive
            final boolean descending;

            if (startRun + 1 == offset + length) {
                endRun = offset + length;
                descending = false;
            } else {
                float next0 = valuesToSort0.get(startRun + 1);
                endRun = startRun + 2;
                int startCmp = doComparison0(current0, next0);
                if (startCmp == 0) {
                    startCmp = doComparison1(valuesToSort1.get(startRun), valuesToSort1.get(startRun + 1));
                }
                descending = startCmp > 0;

                if (!descending) {
                    // search for a non-descending run
                    current0 = next0;
                    while (endRun < length) {
                        next0 = valuesToSort0.get(endRun);
                        int runCmp = doComparison0(next0, current0);
                        if (runCmp == 0) {
                            runCmp = doComparison1(valuesToSort1.get(endRun), valuesToSort1.get(endRun - 1));
                        }
                        if (runCmp < 0) {
                            break;
                        }
                        current0 = next0;
                        endRun++;
                    }
                } else {
                    // search for a strictly descending run; we can not have any equal values, or we will break the
                    // sort's stability guarantee
                    current0 = next0;
                    while (endRun < length) {
                        next0 = valuesToSort0.get(endRun);
                        int runCmp = doComparison0(next0, current0);
                        if (runCmp == 0) {
                            runCmp = doComparison1(valuesToSort1.get(endRun), valuesToSort1.get(endRun - 1));
                        }
                        if (runCmp >= 0) {
                            break;
                        }
                        current0 = next0;
                        endRun++;
                    }
                }
            }

            final int foundLength = endRun - startRun;
            context.runStarts[context.runCount] = startRun;
            if (foundLength < minRun) {
                // increase the size of the run to the minimum run
                final int actualLength = Math.min(minRun, length - (startRun - offset));
                insertionSort(valuesToPermute, valuesToSort0, valuesToSort1, startRun, actualLength);
                context.runLengths[context.runCount] = actualLength;
                startRun += actualLength;
            } else {
                if (descending) {
                    // reverse the current run
                    for (int ii = 0; ii < foundLength / 2; ++ii) {
                        swap(valuesToPermute, valuesToSort0, valuesToSort1, ii + startRun, endRun - ii - 1);
                    }
                }
                // now an ascending run
                context.runLengths[context.runCount] = foundLength;
                startRun = endRun;
            }

            context.runCount++;

            // check the invariants at the top of the stack
            ensureMergeInvariants(context, valuesToPermute, valuesToSort0, valuesToSort1);
        }

        while (context.runCount > 1) {
            final int length2 = context.runLengths[context.runCount - 1];
            final int start1 = context.runStarts[context.runCount - 2];
            final int length1 = context.runLengths[context.runCount - 2];
            merge(context, valuesToPermute, valuesToSort0, valuesToSort1, start1, length1, length2);
            context.runStarts[context.runCount - 2] = start1;
            context.runLengths[context.runCount - 2] = length1 + length2;
            context.runCount--;
        }
    }

    private static int doComparison0(float lhs, float rhs) {
        return FloatComparisons.compare(lhs, rhs);
    }

    private static int doComparison1(int lhs, int rhs) {
        return Integer.compare(lhs, rhs);
    }

    private static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void ensureMergeInvariants(
            FloatIntMultiColumnSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableFloatChunk<SORT_VALUES_ATTR> valuesToSort0,
            WritableIntChunk<SORT_VALUES_ATTR> valuesToSort1) {
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
                merge(context, valuesToPermute, valuesToSort0, valuesToSort1, yStart, yLen, xLen);

                // unchanged: context.runStarts[yStart];
                context.runLengths[yIndex] += xLen;
            } else {
                // merge y and z
                final int zStart = context.runStarts[zIndex];
                merge(context, valuesToPermute, valuesToSort0, valuesToSort1, zStart, zLen, yLen);

                // unchanged: context.runStarts[zIndex];
                context.runLengths[zIndex] += yLen;
                context.runStarts[yIndex] = xStart;
                context.runLengths[yIndex] = xLen;
            }
            context.runCount--;
        }
    }

    private static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void merge(
            FloatIntMultiColumnSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableFloatChunk<SORT_VALUES_ATTR> valuesToSort0,
            WritableIntChunk<SORT_VALUES_ATTR> valuesToSort1, int start1, int length1,
            int length2) {
        // we know that we can never have zero length runs, because there is a minimum run size enforced; and at the
        // end of an input, we won't create a zero-length run. When we merge runs, they only become bigger, thus
        // they'll never be empty. I'm being cheap about function calls and control flow here.

        final int start2 = start1 + length1;
        // find the location of run2[0] in run1
        final float run2lo0 = valuesToSort0.get(start2);
        final int run2lo1 = valuesToSort1.get(start2);
        final int mergeStartPosition = upperBound(valuesToSort0, valuesToSort1, start1, start1 + length1, run2lo0, run2lo1);

        if (mergeStartPosition == start1 + length1) {
            // these two runs are sorted already
            return;
        }

        // find the location of run1[length1 - 1] in run2
        final float run1hi0 = valuesToSort0.get(start1 + length1 - 1);
        final int run1hi1 = valuesToSort1.get(start1 + length1 - 1);
        final int mergeEndPosition = lowerBound(valuesToSort0, valuesToSort1, start2, start2 + length2, run1hi0, run1hi1);

        // figure out which of the two runs is now shorter
        final int remaining1 = start1 + length1 - mergeStartPosition;
        final int remaining2 = mergeEndPosition - start2;

        if (remaining1 < remaining2) {
            copyToTemporary(context, valuesToPermute, valuesToSort0, valuesToSort1, mergeStartPosition, remaining1);
            // now we need to do the merge from temporary and remaining2 into remaining1 (so start at the front,
            // because we've preserved all the values of run1
            frontMerge(context, valuesToPermute, valuesToSort0, valuesToSort1, mergeStartPosition, start2, remaining2);
        } else {
            copyToTemporary(context, valuesToPermute, valuesToSort0, valuesToSort1, start2, remaining2);
            // now we need to do the merge from temporary and remaining1 into the remaining two area (so start at the
            // back, because we've preserved all the values of run2)
            backMerge(context, valuesToPermute, valuesToSort0, valuesToSort1, mergeStartPosition, remaining1);
        }
    }

    /**
     * Merge context temporary and run2 between mergeStartPosition and length2 (which is not the full run length, but
     * the length of things we might need to merge.
     * <p>
     * We eventually need to do galloping here, but are skipping that for now
     */
    private static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void frontMerge(
            FloatIntMultiColumnSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableFloatChunk<SORT_VALUES_ATTR> valuesToSort0,
            WritableIntChunk<SORT_VALUES_ATTR> valuesToSort1, final int mergeStartPosition,
            final int start2, final int length2) {
        int tempCursor = 0;
        int run2Cursor = start2;

        final int run1size = context.temporaryValues0.size();
        int ii;
        final int mergeEndExclusive = start2 + length2;

        float val1_0 = context.temporaryValues0.get(tempCursor);
        float val2_0 = valuesToSort0.get(run2Cursor);

        ii = mergeStartPosition;

        nodataleft: while (ii < mergeEndExclusive) {
            int run1wins = 0;
            int run2wins = 0;

            if (context.minGallop < 2) {
                context.minGallop = 2;
            }

            while (run1wins < context.minGallop && run2wins < context.minGallop) {
                int cmp = doComparison0(val1_0, val2_0);
                if (cmp == 0) {
                    cmp = doComparison1(context.temporaryValues1.get(tempCursor), valuesToSort1.get(run2Cursor));
                }
                if (cmp <= 0) {
                    valuesToSort0.set(ii, val1_0);
                    valuesToSort1.set(ii, context.temporaryValues1.get(tempCursor));
                    valuesToPermute.set(ii++, context.temporaryKeys.get(tempCursor));

                    if (++tempCursor == run1size) {
                        break nodataleft;
                    }

                    val1_0 = context.temporaryValues0.get(tempCursor);
                    run1wins++;
                    run2wins = 0;
                } else {
                    valuesToSort0.set(ii, val2_0);
                    valuesToSort1.set(ii, valuesToSort1.get(run2Cursor));
                    valuesToPermute.set(ii++, valuesToPermute.get(run2Cursor));

                    if (++run2Cursor == mergeEndExclusive) {
                        break nodataleft;
                    }
                    val2_0 = valuesToSort0.get(run2Cursor);

                    run2wins++;
                    run1wins = 0;
                }
            }

            // we are in galloping mode now, if we had run out of data then we should have already bailed out to
            // nodataleft
            while (ii < mergeEndExclusive) {
                // if we had a lot of things from run1, we take the next thing from run2 then find it in run1
                final int copyUntil1 = upperBound(context.temporaryValues0, context.temporaryValues1, tempCursor, run1size, val2_0, valuesToSort1.get(run2Cursor));
                final int gallopLength1 = copyUntil1 - tempCursor;
                if (gallopLength1 > 0) {
                    copyToChunk(context.temporaryKeys, context.temporaryValues0, context.temporaryValues1, valuesToPermute, valuesToSort0, valuesToSort1, tempCursor, ii, gallopLength1);
                    tempCursor += gallopLength1;
                    ii += gallopLength1;

                    if (tempCursor == run1size) {
                        break nodataleft;
                    }
                    val1_0 = context.temporaryValues0.get(tempCursor);

                    context.minGallop--;
                }

                // if we had a lot of things from run2, we take the next thing from run1 and then find it in run2
                final int copyUntil2 = lowerBound(valuesToSort0, valuesToSort1, run2Cursor, mergeEndExclusive, val1_0, context.temporaryValues1.get(tempCursor));
                final int gallopLength2 = copyUntil2 - run2Cursor;
                if (gallopLength2 > 0) {
                    copyToChunk(valuesToPermute, valuesToSort0, valuesToSort1, valuesToPermute, valuesToSort0, valuesToSort1, run2Cursor, ii, gallopLength2);
                    run2Cursor += gallopLength2;
                    ii += gallopLength2;

                    if (run2Cursor == mergeEndExclusive) {
                        break nodataleft;
                    }
                    val2_0 = valuesToSort0.get(run2Cursor);

                    context.minGallop--;
                }

                if (gallopLength1 < TimsortUtils.INITIAL_GALLOP && gallopLength2 < TimsortUtils.INITIAL_GALLOP) {
                    context.minGallop += 2; // undo the possible subtraction from above
                    break;
                }
            }
        }

        while (tempCursor < run1size) {
            valuesToSort0.set(ii, context.temporaryValues0.get(tempCursor));
            valuesToSort1.set(ii, context.temporaryValues1.get(tempCursor));
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
            FloatIntMultiColumnSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableFloatChunk<SORT_VALUES_ATTR> valuesToSort0,
            WritableIntChunk<SORT_VALUES_ATTR> valuesToSort1, final int mergeStartPosition,
            final int length1) {
        final int run1End = mergeStartPosition + length1;
        int run1Cursor = run1End - 1;
        int tempCursor = context.temporaryValues0.size() - 1;

        final int mergeLength = context.temporaryValues0.size() + length1;
        int ii;

        float val1_0 = valuesToSort0.get(run1Cursor);
        float val2_0 = context.temporaryValues0.get(tempCursor);

        final int mergeEnd = mergeStartPosition + mergeLength;
        ii = mergeEnd - 1;

        nodataleft: while (ii >= mergeStartPosition) {
            int run1wins = 0;
            int run2wins = 0;

            if (context.minGallop < 2) {
                context.minGallop = 2;
            }

            while (run1wins < context.minGallop && run2wins < context.minGallop) {
                int cmp = doComparison0(val2_0, val1_0);
                if (cmp == 0) {
                    cmp = doComparison1(context.temporaryValues1.get(tempCursor), valuesToSort1.get(run1Cursor));
                }
                if (cmp >= 0) {
                    valuesToSort0.set(ii, val2_0);
                    valuesToSort1.set(ii, context.temporaryValues1.get(tempCursor));
                    valuesToPermute.set(ii--, context.temporaryKeys.get(tempCursor));

                    if (--tempCursor < 0) {
                        break nodataleft;
                    }
                    val2_0 = context.temporaryValues0.get(tempCursor);

                    run2wins++;
                    run1wins = 0;
                } else {
                    valuesToSort0.set(ii, val1_0);
                    valuesToSort1.set(ii, valuesToSort1.get(run1Cursor));
                    valuesToPermute.set(ii--, valuesToPermute.get(run1Cursor));

                    if (--run1Cursor < mergeStartPosition) {
                        break nodataleft;
                    }
                    val1_0 = valuesToSort0.get(run1Cursor);

                    run1wins++;
                    run2wins = 0;
                }
            }

            // we are in galloping mode now, if we had run out of data then we should have already bailed out to
            // nodataleft
            while (ii >= mergeStartPosition) {
                // if we had a lot of things from run2, we take the next thing from run1 then find it in run2
                final int copyUntil2 = lowerBound(context.temporaryValues0, context.temporaryValues1, 0, tempCursor, val1_0, valuesToSort1.get(run1Cursor)) + 1;

                final int gallopLength2 = tempCursor - copyUntil2 + 1;
                if (gallopLength2 > 0) {
                    copyToChunk(context.temporaryKeys, context.temporaryValues0, context.temporaryValues1, valuesToPermute, valuesToSort0, valuesToSort1, copyUntil2, ii - gallopLength2 + 1, gallopLength2);
                    tempCursor -= gallopLength2;
                    ii -= gallopLength2;

                    if (tempCursor < 0) {
                        break nodataleft;
                    }
                    val2_0 = context.temporaryValues0.get(tempCursor);

                    context.minGallop--;
                }

                // if we had a lot of things from run1, we take the next thing from run2 and then find it in run1
                final int copyUntil1 = upperBound(valuesToSort0, valuesToSort1, mergeStartPosition, run1Cursor, val2_0, context.temporaryValues1.get(tempCursor));

                final int gallopLength1 = run1Cursor - copyUntil1;
                if (gallopLength1 > 0) {
                    copyToChunk(valuesToPermute, valuesToSort0, valuesToSort1, valuesToPermute, valuesToSort0, valuesToSort1, copyUntil1, ii - gallopLength1, gallopLength1 + 1);
                    run1Cursor -= gallopLength1;
                    ii -= gallopLength1;

                    if (run1Cursor < mergeStartPosition) {
                        break nodataleft;
                    }
                    val1_0 = valuesToSort0.get(run1Cursor);

                    context.minGallop--;
                }

                if (gallopLength1 < TimsortUtils.INITIAL_GALLOP && gallopLength2 < TimsortUtils.INITIAL_GALLOP) {
                    context.minGallop += 2; // undo the possible subtraction from above
                    break;
                }
            }
        }

        while (tempCursor >= 0) {
            valuesToSort0.set(ii, context.temporaryValues0.get(tempCursor));
            valuesToSort1.set(ii, context.temporaryValues1.get(tempCursor));
            valuesToPermute.set(ii, context.temporaryKeys.get(tempCursor));
            tempCursor--;
            ii--;
        }
    }

    private static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void copyToTemporary(
            FloatIntMultiColumnSortKernelContext<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context,
            WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableFloatChunk<SORT_VALUES_ATTR> valuesToSort0,
            WritableIntChunk<SORT_VALUES_ATTR> valuesToSort1, int mergeStartPosition,
            int remaining1) {
        context.temporaryValues0.setSize(remaining1);
        context.temporaryValues1.setSize(remaining1);
        context.temporaryKeys.setSize(remaining1);
        context.temporaryValues0.copyFromChunk(valuesToSort0, mergeStartPosition, 0, remaining1);
        context.temporaryValues1.copyFromChunk(valuesToSort1, mergeStartPosition, 0, remaining1);
        context.temporaryKeys.copyFromChunk(valuesToPermute, mergeStartPosition, 0, remaining1);
    }

    private static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void copyToChunk(
            LongChunk<PERMUTE_VALUES_ATTR> rowSetSource, FloatChunk<SORT_VALUES_ATTR> valuesSource0,
            IntChunk<SORT_VALUES_ATTR> valuesSource1,
            WritableLongChunk<PERMUTE_VALUES_ATTR> permuteValuesDest,
            WritableFloatChunk<SORT_VALUES_ATTR> sortValuesDest0,
            WritableIntChunk<SORT_VALUES_ATTR> sortValuesDest1, int sourceStart, int destStart,
            int length) {
        sortValuesDest0.copyFromChunk(valuesSource0, sourceStart, destStart, length);
        sortValuesDest1.copyFromChunk(valuesSource1, sourceStart, destStart, length);
        permuteValuesDest.copyFromChunk(rowSetSource, sourceStart, destStart, length);
    }

    private static int upperBound(FloatChunk<?> valuesToSort0, IntChunk<?> valuesToSort1, int lo,
            int hi, float searchValue0, int searchValue1) {
        // when we binary search in 1, we must identify a position for search value that is *after* our test values;
        // because the values from run 2 may never be inserted before an equal value from run 1
        // 
        // lo is inclusive, hi is exclusive
        // 
        // returns the position of the first element that is > searchValue or hi if there is no such element
        return bound(valuesToSort0, valuesToSort1, lo, hi, searchValue0, searchValue1, false);
    }

    private static int lowerBound(FloatChunk<?> valuesToSort0, IntChunk<?> valuesToSort1, int lo,
            int hi, float searchValue0, int searchValue1) {
        // when we binary search in 2, we must identify a position for search value that is *before* our test values;
        // because the values from run 1 may never be inserted after an equal value from run 2
        return bound(valuesToSort0, valuesToSort1, lo, hi, searchValue0, searchValue1, true);
    }

    private static int bound(FloatChunk<?> valuesToSort0, IntChunk<?> valuesToSort1, int lo, int hi,
            float searchValue0, int searchValue1, final boolean lower) {
        final int compareLimit = lower ? -1 : 0; // lt or leq

        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            int cmp = doComparison0(valuesToSort0.get(mid), searchValue0);
            if (cmp == 0) {
                cmp = doComparison1(valuesToSort1.get(mid), searchValue1);
            }
            final boolean moveLo = cmp <= compareLimit;
            if (moveLo) {
                // For bound, (testValue OP searchValue) means that the result somewhere later than 'mid' [OP=lt or leq]
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        return lo;
    }

    private static void insertionSort(WritableLongChunk<?> valuesToPermute,
            WritableFloatChunk<?> valuesToSort0, WritableIntChunk<?> valuesToSort1, int offset,
            int length) {
        for (int ii = offset + 1; ii < offset + length; ++ii) {
            for (int jj = ii; jj > offset; jj--) {
                int cmp = doComparison0(valuesToSort0.get(jj - 1), valuesToSort0.get(jj));
                if (cmp == 0) {
                    cmp = doComparison1(valuesToSort1.get(jj - 1), valuesToSort1.get(jj));
                }
                if (cmp <= 0) {
                    break;
                }
                swap(valuesToPermute, valuesToSort0, valuesToSort1, jj, jj - 1);
            }
        }
    }

    private static void swap(WritableLongChunk<?> valuesToPermute,
            WritableFloatChunk<?> valuesToSort0, WritableIntChunk<?> valuesToSort1, int a, int b) {
        final long tempPermuteValue = valuesToPermute.get(a);
        final float temp0 = valuesToSort0.get(a);
        final int temp1 = valuesToSort1.get(a);

        valuesToPermute.set(a, valuesToPermute.get(b));
        valuesToSort0.set(a, valuesToSort0.get(b));
        valuesToSort1.set(a, valuesToSort1.get(b));

        valuesToPermute.set(b, tempPermuteValue);
        valuesToSort0.set(b, temp0);
        valuesToSort1.set(b, temp1);
    }

    public static class FloatIntMultiColumnSortKernelContext<SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> implements MultiColumnSortKernel<PERMUTE_VALUES_ATTR> {
        int minGallop;

        int runCount = 0;

        private final int[] runStarts;

        private final int[] runLengths;

        private final WritableLongChunk<PERMUTE_VALUES_ATTR> temporaryKeys;

        private final WritableFloatChunk<SORT_VALUES_ATTR> temporaryValues0;

        private final WritableIntChunk<SORT_VALUES_ATTR> temporaryValues1;

        private FloatIntMultiColumnSortKernelContext(int size) {
            temporaryKeys = WritableLongChunk.makeWritableChunk((size + 2) / 2);
            temporaryValues0 = WritableFloatChunk.makeWritableChunk((size + 2) / 2);
            temporaryValues1 = WritableIntChunk.makeWritableChunk((size + 2) / 2);
            runStarts = new int[(size + 31) / 32];
            runLengths = new int[(size + 31) / 32];
            minGallop = TimsortUtils.INITIAL_GALLOP;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void sort(WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableChunk<? extends Any>[] valuesToSort) {
            FloatIntMultiColumnTimsortKernel.sort(this, valuesToPermute,
                            (WritableFloatChunk<SORT_VALUES_ATTR>) valuesToSort[0].asWritableFloatChunk(),
                            (WritableIntChunk<SORT_VALUES_ATTR>) valuesToSort[1].asWritableIntChunk());
        }

        @Override
        public void close() {
            temporaryKeys.close();
            temporaryValues0.close();
            temporaryValues1.close();
        }
    }
}
