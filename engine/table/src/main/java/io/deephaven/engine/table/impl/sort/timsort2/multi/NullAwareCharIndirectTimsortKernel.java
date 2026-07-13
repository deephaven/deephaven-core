//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run GenerateTimsortKernels or ./gradlew generateTimsortKernels to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sort.timsort2.multi;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.table.impl.sort.MultiColumnSortKernel;
import io.deephaven.engine.table.impl.sort.timsort.TimsortUtils;
import io.deephaven.util.compare.CharComparisons;
import java.lang.Override;
import java.lang.UnsupportedOperationException;

/**
 * This implements a timsort kernel for a multi-column key (NullAwareChar) that never moves the column values:
 * it permutes a parallel chunk of int positions, reading values through the positions for each
 * comparison (comparing each column in turn, only reading later columns on ties). The row keys are
 * not permuted during the sort either; they are assembled in a single linear pass at the end.
 * <p>
 * <a href="https://bugs.python.org/file4451/timsort.txt">bugs.python.org</a> and
 * <a href="https://en.wikipedia.org/wiki/Timsort">Wikipedia</a> do a decent job of describing the algorithm.
 */
public final class NullAwareCharIndirectTimsortKernel {
    private NullAwareCharIndirectTimsortKernel() {
        throw new UnsupportedOperationException();
    }

    public static <PERMUTE_VALUES_ATTR extends Any> NullAwareCharIndirectSortKernelContext<PERMUTE_VALUES_ATTR> createContext(
            final int size) {
        return new NullAwareCharIndirectSortKernelContext<>(size);
    }

    /**
     * Sort the positions chunk such that the values it points to are lexicographically ordered, comparing
     * each column in turn; the value chunks themselves are not modified.
     */
    public static void sort(NullAwareCharIndirectSortKernelContext<?> context,
            WritableIntChunk<ChunkPositions> positions, CharChunk<?> valuesToSort0) {
        timSort(context, positions, valuesToSort0, 0, positions.size());
    }

    private static void timSort(NullAwareCharIndirectSortKernelContext<?> context,
            WritableIntChunk<ChunkPositions> positions, CharChunk<?> valuesToSort0, int offset,
            int length) {
        if (length <= 1) {
            return;
        }

        final int minRun = TimsortUtils.getRunLength(length);

        if (length <= minRun) {
            insertionSort(positions, valuesToSort0, offset, length);
            return;
        }

        context.runCount = 0;

        int startRun = offset;
        while (startRun < offset + length) {
            int currentPos = positions.get(startRun);

            int endRun; // note that endrun is exclusive
            final boolean descending;

            if (startRun + 1 == offset + length) {
                endRun = offset + length;
                descending = false;
            } else {
                int nextPos = positions.get(startRun + 1);
                endRun = startRun + 2;
                descending = compareColumns(valuesToSort0, currentPos, nextPos) > 0;

                if (!descending) {
                    // search for a non-descending run
                    currentPos = nextPos;
                    while (endRun < length) {
                        nextPos = positions.get(endRun);
                        if (compareColumns(valuesToSort0, nextPos, currentPos) < 0) {
                            break;
                        }
                        currentPos = nextPos;
                        endRun++;
                    }
                } else {
                    // search for a strictly descending run; we can not have any equal values, or we will break the
                    // sort's stability guarantee
                    currentPos = nextPos;
                    while (endRun < length) {
                        nextPos = positions.get(endRun);
                        if (compareColumns(valuesToSort0, nextPos, currentPos) >= 0) {
                            break;
                        }
                        currentPos = nextPos;
                        endRun++;
                    }
                }
            }

            final int foundLength = endRun - startRun;
            context.runStarts[context.runCount] = startRun;
            if (foundLength < minRun) {
                // increase the size of the run to the minimum run
                final int actualLength = Math.min(minRun, length - (startRun - offset));
                insertionSort(positions, valuesToSort0, startRun, actualLength);
                context.runLengths[context.runCount] = actualLength;
                startRun += actualLength;
            } else {
                if (descending) {
                    // reverse the current run
                    for (int ii = 0; ii < foundLength / 2; ++ii) {
                        swap(positions, ii + startRun, endRun - ii - 1);
                    }
                }
                // now an ascending run
                context.runLengths[context.runCount] = foundLength;
                startRun = endRun;
            }

            context.runCount++;

            // check the invariants at the top of the stack
            ensureMergeInvariants(context, positions, valuesToSort0);
        }

        while (context.runCount > 1) {
            final int length2 = context.runLengths[context.runCount - 1];
            final int start1 = context.runStarts[context.runCount - 2];
            final int length1 = context.runLengths[context.runCount - 2];
            merge(context, positions, valuesToSort0, start1, length1, length2);
            context.runStarts[context.runCount - 2] = start1;
            context.runLengths[context.runCount - 2] = length1 + length2;
            context.runCount--;
        }
    }

    private static int doComparison0(char lhs, char rhs) {
        return CharComparisons.compare(lhs, rhs);
    }

    /**
     * Compares the elements at two positions, column by column; later columns are only read when all
     * earlier columns compare equal.
     */
    private static int compareColumns(CharChunk<?> valuesToSort0, int lhsPos, int rhsPos) {
        final int cmp0 = doComparison0(valuesToSort0.get(lhsPos), valuesToSort0.get(rhsPos));
        return cmp0;
    }

    private static void ensureMergeInvariants(NullAwareCharIndirectSortKernelContext<?> context,
            WritableIntChunk<ChunkPositions> positions, CharChunk<?> valuesToSort0) {
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
                merge(context, positions, valuesToSort0, yStart, yLen, xLen);

                // unchanged: context.runStarts[yStart];
                context.runLengths[yIndex] += xLen;
            } else {
                // merge y and z
                final int zStart = context.runStarts[zIndex];
                merge(context, positions, valuesToSort0, zStart, zLen, yLen);

                // unchanged: context.runStarts[zIndex];
                context.runLengths[zIndex] += yLen;
                context.runStarts[yIndex] = xStart;
                context.runLengths[yIndex] = xLen;
            }
            context.runCount--;
        }
    }

    private static void merge(NullAwareCharIndirectSortKernelContext<?> context,
            WritableIntChunk<ChunkPositions> positions, CharChunk<?> valuesToSort0, int start1,
            int length1, int length2) {
        // we know that we can never have zero length runs, because there is a minimum run size enforced; and at the
        // end of an input, we won't create a zero-length run. When we merge runs, they only become bigger, thus
        // they'll never be empty. I'm being cheap about function calls and control flow here.

        final int start2 = start1 + length1;
        // find the location of run2[0] in run1
        final int run2loPos = positions.get(start2);
        final int mergeStartPosition = upperBound(positions, valuesToSort0, start1, start1 + length1, run2loPos);

        if (mergeStartPosition == start1 + length1) {
            // these two runs are sorted already
            return;
        }

        // find the location of run1[length1 - 1] in run2
        final int run1hiPos = positions.get(start1 + length1 - 1);
        final int mergeEndPosition = lowerBound(positions, valuesToSort0, start2, start2 + length2, run1hiPos);

        // figure out which of the two runs is now shorter
        final int remaining1 = start1 + length1 - mergeStartPosition;
        final int remaining2 = mergeEndPosition - start2;

        if (remaining1 < remaining2) {
            copyToTemporary(context, positions, mergeStartPosition, remaining1);
            // now we need to do the merge from temporary and remaining2 into remaining1 (so start at the front,
            // because we've preserved all the values of run1
            frontMerge(context, positions, valuesToSort0, mergeStartPosition, start2, remaining2);
        } else {
            copyToTemporary(context, positions, start2, remaining2);
            // now we need to do the merge from temporary and remaining1 into the remaining two area (so start at the
            // back, because we've preserved all the values of run2)
            backMerge(context, positions, valuesToSort0, mergeStartPosition, remaining1);
        }
    }

    /**
     * Merge context temporary and run2 between mergeStartPosition and length2 (which is not the full run length, but
     * the length of things we might need to merge.
     * <p>
     * We eventually need to do galloping here, but are skipping that for now
     */
    private static void frontMerge(NullAwareCharIndirectSortKernelContext<?> context,
            WritableIntChunk<ChunkPositions> positions, CharChunk<?> valuesToSort0,
            final int mergeStartPosition, final int start2, final int length2) {
        int tempCursor = 0;
        int run2Cursor = start2;

        final int run1size = context.temporaryPositions.size();
        int ii;
        final int mergeEndExclusive = start2 + length2;

        int val1Pos = context.temporaryPositions.get(tempCursor);
        int val2Pos = positions.get(run2Cursor);

        ii = mergeStartPosition;

        nodataleft: while (ii < mergeEndExclusive) {
            int run1wins = 0;
            int run2wins = 0;

            if (context.minGallop < 2) {
                context.minGallop = 2;
            }

            while (run1wins < context.minGallop && run2wins < context.minGallop) {
                if (compareColumns(valuesToSort0, val1Pos, val2Pos) <= 0) {
                    positions.set(ii++, val1Pos);

                    if (++tempCursor == run1size) {
                        break nodataleft;
                    }

                    val1Pos = context.temporaryPositions.get(tempCursor);
                    run1wins++;
                    run2wins = 0;
                } else {
                    positions.set(ii++, val2Pos);

                    if (++run2Cursor == mergeEndExclusive) {
                        break nodataleft;
                    }
                    val2Pos = positions.get(run2Cursor);

                    run2wins++;
                    run1wins = 0;
                }
            }

            // we are in galloping mode now, if we had run out of data then we should have already bailed out to
            // nodataleft
            while (ii < mergeEndExclusive) {
                // if we had a lot of things from run1, we take the next thing from run2 then find it in run1
                final int copyUntil1 = upperBound(context.temporaryPositions, valuesToSort0, tempCursor, run1size, val2Pos);
                final int gallopLength1 = copyUntil1 - tempCursor;
                if (gallopLength1 > 0) {
                    copyToChunk(context.temporaryPositions, positions, tempCursor, ii, gallopLength1);
                    tempCursor += gallopLength1;
                    ii += gallopLength1;

                    if (tempCursor == run1size) {
                        break nodataleft;
                    }
                    val1Pos = context.temporaryPositions.get(tempCursor);

                    context.minGallop--;
                }

                // if we had a lot of things from run2, we take the next thing from run1 and then find it in run2
                final int copyUntil2 = lowerBound(positions, valuesToSort0, run2Cursor, mergeEndExclusive, val1Pos);
                final int gallopLength2 = copyUntil2 - run2Cursor;
                if (gallopLength2 > 0) {
                    copyToChunk(positions, positions, run2Cursor, ii, gallopLength2);
                    run2Cursor += gallopLength2;
                    ii += gallopLength2;

                    if (run2Cursor == mergeEndExclusive) {
                        break nodataleft;
                    }
                    val2Pos = positions.get(run2Cursor);

                    context.minGallop--;
                }

                if (gallopLength1 < TimsortUtils.INITIAL_GALLOP && gallopLength2 < TimsortUtils.INITIAL_GALLOP) {
                    context.minGallop += 2; // undo the possible subtraction from above
                    break;
                }
            }
        }

        while (tempCursor < run1size) {
            positions.set(ii, context.temporaryPositions.get(tempCursor));
            tempCursor++;
            ii++;
        }
    }

    /**
     * Merge context temporary and run1 between mergeStartPosition + length1 + temporary.length
     * <p>
     * We eventually need to do galloping here, but are skipping that for now
     */
    private static void backMerge(NullAwareCharIndirectSortKernelContext<?> context,
            WritableIntChunk<ChunkPositions> positions, CharChunk<?> valuesToSort0,
            final int mergeStartPosition, final int length1) {
        final int run1End = mergeStartPosition + length1;
        int run1Cursor = run1End - 1;
        int tempCursor = context.temporaryPositions.size() - 1;

        final int mergeLength = context.temporaryPositions.size() + length1;
        int ii;

        int val1Pos = positions.get(run1Cursor);
        int val2Pos = context.temporaryPositions.get(tempCursor);

        final int mergeEnd = mergeStartPosition + mergeLength;
        ii = mergeEnd - 1;

        nodataleft: while (ii >= mergeStartPosition) {
            int run1wins = 0;
            int run2wins = 0;

            if (context.minGallop < 2) {
                context.minGallop = 2;
            }

            while (run1wins < context.minGallop && run2wins < context.minGallop) {
                if (compareColumns(valuesToSort0, val2Pos, val1Pos) >= 0) {
                    positions.set(ii--, val2Pos);

                    if (--tempCursor < 0) {
                        break nodataleft;
                    }
                    val2Pos = context.temporaryPositions.get(tempCursor);

                    run2wins++;
                    run1wins = 0;
                } else {
                    positions.set(ii--, val1Pos);

                    if (--run1Cursor < mergeStartPosition) {
                        break nodataleft;
                    }
                    val1Pos = positions.get(run1Cursor);

                    run1wins++;
                    run2wins = 0;
                }
            }

            // we are in galloping mode now, if we had run out of data then we should have already bailed out to
            // nodataleft
            while (ii >= mergeStartPosition) {
                // if we had a lot of things from run2, we take the next thing from run1 then find it in run2
                final int copyUntil2 = lowerBound(context.temporaryPositions, valuesToSort0, 0, tempCursor, val1Pos) + 1;

                final int gallopLength2 = tempCursor - copyUntil2 + 1;
                if (gallopLength2 > 0) {
                    copyToChunk(context.temporaryPositions, positions, copyUntil2, ii - gallopLength2 + 1, gallopLength2);
                    tempCursor -= gallopLength2;
                    ii -= gallopLength2;

                    if (tempCursor < 0) {
                        break nodataleft;
                    }
                    val2Pos = context.temporaryPositions.get(tempCursor);

                    context.minGallop--;
                }

                // if we had a lot of things from run1, we take the next thing from run2 and then find it in run1
                final int copyUntil1 = upperBound(positions, valuesToSort0, mergeStartPosition, run1Cursor, val2Pos);

                final int gallopLength1 = run1Cursor - copyUntil1;
                if (gallopLength1 > 0) {
                    copyToChunk(positions, positions, copyUntil1, ii - gallopLength1, gallopLength1 + 1);
                    run1Cursor -= gallopLength1;
                    ii -= gallopLength1;

                    if (run1Cursor < mergeStartPosition) {
                        break nodataleft;
                    }
                    val1Pos = positions.get(run1Cursor);

                    context.minGallop--;
                }

                if (gallopLength1 < TimsortUtils.INITIAL_GALLOP && gallopLength2 < TimsortUtils.INITIAL_GALLOP) {
                    context.minGallop += 2; // undo the possible subtraction from above
                    break;
                }
            }
        }

        while (tempCursor >= 0) {
            positions.set(ii, context.temporaryPositions.get(tempCursor));
            tempCursor--;
            ii--;
        }
    }

    private static void copyToTemporary(NullAwareCharIndirectSortKernelContext<?> context,
            IntChunk<ChunkPositions> positions, int mergeStartPosition, int remaining1) {
        context.temporaryPositions.setSize(remaining1);
        context.temporaryPositions.copyFromChunk(positions, mergeStartPosition, 0, remaining1);
    }

    private static void copyToChunk(IntChunk<ChunkPositions> positionsSource,
            WritableIntChunk<ChunkPositions> positionsDest, int sourceStart, int destStart,
            int length) {
        positionsDest.copyFromChunk(positionsSource, sourceStart, destStart, length);
    }

    private static int upperBound(IntChunk<ChunkPositions> positions, CharChunk<?> valuesToSort0,
            int lo, int hi, int searchPos) {
        // when we binary search in 1, we must identify a position for search value that is *after* our test values;
        // because the values from run 2 may never be inserted before an equal value from run 1
        // 
        // lo is inclusive, hi is exclusive
        // 
        // returns the position of the first element that is > searchValue or hi if there is no such element
        return bound(positions, valuesToSort0, lo, hi, searchPos, false);
    }

    private static int lowerBound(IntChunk<ChunkPositions> positions, CharChunk<?> valuesToSort0,
            int lo, int hi, int searchPos) {
        // when we binary search in 2, we must identify a position for search value that is *before* our test values;
        // because the values from run 1 may never be inserted after an equal value from run 2
        return bound(positions, valuesToSort0, lo, hi, searchPos, true);
    }

    private static int bound(IntChunk<ChunkPositions> positions, CharChunk<?> valuesToSort0, int lo,
            int hi, int searchPos, final boolean lower) {
        final int compareLimit = lower ? -1 : 0; // lt or leq

        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final boolean moveLo = compareColumns(valuesToSort0, positions.get(mid), searchPos) <= compareLimit;
            if (moveLo) {
                // For bound, (testValue OP searchValue) means that the result somewhere later than 'mid' [OP=lt or leq]
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        return lo;
    }

    private static void insertionSort(WritableIntChunk<ChunkPositions> positions,
            CharChunk<?> valuesToSort0, int offset, int length) {
        for (int ii = offset + 1; ii < offset + length; ++ii) {
            for (int jj = ii; jj > offset && compareColumns(valuesToSort0, positions.get(jj - 1), positions.get(jj)) > 0; jj--) {
                swap(positions, jj, jj - 1);
            }
        }
    }

    private static void swap(WritableIntChunk<ChunkPositions> positions, int a, int b) {
        final int tempPos = positions.get(a);
        positions.set(a, positions.get(b));
        positions.set(b, tempPos);
    }

    public static class NullAwareCharIndirectSortKernelContext<PERMUTE_VALUES_ATTR extends Any> implements MultiColumnSortKernel<PERMUTE_VALUES_ATTR> {
        int minGallop;

        int runCount = 0;

        private final int[] runStarts;

        private final int[] runLengths;

        private final WritableIntChunk<ChunkPositions> positions;

        private final WritableIntChunk<ChunkPositions> temporaryPositions;

        private final WritableLongChunk<PERMUTE_VALUES_ATTR> temporaryKeys;

        private NullAwareCharIndirectSortKernelContext(int size) {
            positions = WritableIntChunk.makeWritableChunk(size);
            temporaryPositions = WritableIntChunk.makeWritableChunk((size + 2) / 2);
            temporaryKeys = WritableLongChunk.makeWritableChunk(size);
            runStarts = new int[(size + 31) / 32];
            runLengths = new int[(size + 31) / 32];
            minGallop = TimsortUtils.INITIAL_GALLOP;
        }

        @Override
        public void sort(WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableChunk<? extends Any>[] valuesToSort) {
            final int size = valuesToPermute.size();
            positions.setSize(size);
            for (int ii = 0; ii < size; ++ii) {
                positions.set(ii, ii);
            }
            NullAwareCharIndirectTimsortKernel.sort(this, positions, valuesToSort[0].asCharChunk());
            // assemble the permuted row keys in a single linear pass rather than permuting them during the sort
            temporaryKeys.copyFromChunk(valuesToPermute, 0, 0, size);
            for (int ii = 0; ii < size; ++ii) {
                valuesToPermute.set(ii, temporaryKeys.get(positions.get(ii)));
            }
        }

        @Override
        public void close() {
            positions.close();
            temporaryPositions.close();
            temporaryKeys.close();
        }
    }
}
