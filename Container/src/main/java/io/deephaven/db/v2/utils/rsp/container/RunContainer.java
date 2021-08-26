/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 *
 * The code in this file is a heavily modified version of the original in the RoaringBitmap library;
 * please see https://roaringbitmap.org/
 *
 */
package io.deephaven.db.v2.utils.rsp.container;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import static io.deephaven.db.v2.utils.rsp.container.ContainerUtil.lowbits;
import static io.deephaven.db.v2.utils.rsp.container.ContainerUtil.toIntUnsigned;
import static io.deephaven.db.v2.utils.rsp.container.MutableInteger.setIfNotNull;
import static io.deephaven.db.v2.utils.rsp.container.PositionHint.resetIfNotNull;

/**
 * This container takes the form of runs of consecutive values (effectively, run-length encoding).
 * <p>
 * Adding and removing content from this container might make it wasteful so regular calls to
 * "runOptimize" might be warranted.
 */
public final class RunContainer extends Container {
    // Sizing of a short array object in a 64 bit JVM (Hotspot) uses
    // 12 bytes of object overhead (including array.length), plus
    // the space for the short elements payload, plus padding to round
    // objects to 8 bytes boundaries.
    // So an array of 4 elements uses: 12 + 2*4 + 4 padding = 24 bytes = 3*8 bytes.
    // The 4 bytes of padding are wasted and can be used for another 2 short elements.
    private static final int DEFAULT_INIT_SIZE_IN_RUNS = 3;
    private static final boolean ENABLE_GALLOPING_AND = false;

    private static int branchyUnsignedInterleavedBinarySearch(final short[] array, final int begin,
        final int end, final short k) {
        int ikey = toIntUnsigned(k);
        int low = begin;
        int high = end - 1;
        while (low <= high) {
            final int middleIndex = (low + high) >>> 1;
            final int middleValue = toIntUnsigned(array[2 * middleIndex]);
            if (middleValue < ikey) {
                low = middleIndex + 1;
            } else if (middleValue > ikey) {
                high = middleIndex - 1;
            } else {
                return middleIndex;
            }
        }
        return -(low + 1);
    }

    // starts with binary search and finishes with a sequential search
    private static int hybridUnsignedInterleavedBinarySearch(final short[] array, final int begin,
        final int end, final short k) {
        int ikey = toIntUnsigned(k);
        // next line accelerates the possibly common case where the value would
        // be inserted at the end
        if ((end > 0) && (toIntUnsigned(array[2 * (end - 1)]) < ikey)) {
            return -end - 1;
        }
        int low = begin;
        int high = end - 1;
        // 16 in the next line matches the size of a cache line
        while (low + 16 <= high) {
            final int middleIndex = (low + high) >>> 1;
            final int middleValue = toIntUnsigned(array[2 * middleIndex]);
            if (middleValue < ikey) {
                low = middleIndex + 1;
            } else if (middleValue > ikey) {
                high = middleIndex - 1;
            } else {
                return middleIndex;
            }
        }
        // we finish the job with a sequential search
        int x = low;
        for (; x <= high; ++x) {
            final int val = toIntUnsigned(array[2 * x]);
            if (val >= ikey) {
                if (val == ikey) {
                    return x;
                }
                break;
            }
        }
        return -(x + 1);
    }

    protected static int sizeInBytes(final int numberOfRuns) {
        return 2 * Short.BYTES * numberOfRuns;
    }

    private static int unsignedInterleavedBinarySearch(final short[] array, final int begin,
        final int end, final short k) {
        if (ContainerUtil.USE_HYBRID_BINSEARCH) {
            return hybridUnsignedInterleavedBinarySearch(array, begin, end, k);
        } else {
            return branchyUnsignedInterleavedBinarySearch(array, begin, end, k);
        }

    }

    private short[] valueslength;// we interleave values and lengths, so
    // that if you have the values 11,12,13,14,15, you store that as 11,4 where 4 means that beyond
    // 11
    // itself, there are
    // 4 contiguous values that follows.
    // Other example: e.g., 1, 10, 20,0, 31,2 would be a concise representation of 1, 2, ..., 11,
    // 20,
    // 31, 32, 33

    int nbrruns = 0;// how many runs, this number should fit in 16 bits.

    private boolean shared = false;

    int cardinality;

    /**
     * Create a container with default capacity
     */
    public RunContainer() {
        this(DEFAULT_INIT_SIZE_IN_RUNS);
    }

    private RunContainer(final short[] valueslength, final int nbrruns, final int cardinality) {
        this.valueslength = valueslength;
        this.nbrruns = nbrruns;
        this.cardinality = cardinality;
    }

    public RunContainer(final RunContainer rc) {
        this(Arrays.copyOf(rc.valueslength, rc.valueslength.length), rc.nbrruns, rc.cardinality);
    }

    protected RunContainer(final ArrayContainer arr, final int nbrRunsCapacity) {
        valueslength = new short[runsShortArraySizeRounding(nbrRunsCapacity)];
        final int arrCard = arr.getCardinality();
        if (arrCard == 0) {
            nbrruns = 0;
            return;
        }

        int prevVal = toIntUnsigned(arr.content[0]);
        int runLen = 0;
        int runIdx = 0;
        setValue(0, (short) prevVal);
        for (int i = 1; i < arrCard; i++) {
            final int curVal = toIntUnsigned(arr.content[i]);
            if (curVal == prevVal + 1) {
                ++runLen;
            } else {
                setLength(runIdx, (short) runLen);
                ++runIdx;
                setValue(runIdx, (short) curVal);
                runLen = 0;
            }
            prevVal = curVal;
        }
        setLength(runIdx, (short) runLen);
        nbrruns = runIdx + 1;
        cardinality = arrCard;
    }

    /**
     * Create an run container with a run of ones from start to end.
     *
     * @param start first index
     * @param end last index (range is exclusive)
     */
    public RunContainer(final int start, final int end) {
        nbrruns = 1;
        valueslength = new short[2 * DEFAULT_INIT_SIZE_IN_RUNS];
        valueslength[0] = (short) start;
        final int d = end - start;
        valueslength[1] = (short) (d - 1);
        cardinality = d;
    }

    public RunContainer(final int start1, final int end1, final int start2, final int end2) {
        nbrruns = 2;
        final int d1 = end1 - start1;
        final int d2 = end2 - start2;
        valueslength = new short[2 * DEFAULT_INIT_SIZE_IN_RUNS];
        valueslength[0] = lowbits(start1);
        valueslength[1] = lowbits(d1 - 1);
        valueslength[2] = lowbits(start2);
        valueslength[3] = lowbits(d2 - 1);
        cardinality = d1 + d2;
    }

    public static RunContainer select(final RunContainer src, final int startRank,
        final int endRank) {
        final RunContainer ans = new RunContainer(src.valueslength.length);
        int k = 0;
        int kStart = toIntUnsigned(src.getValue(0));
        int kLen = toIntUnsigned(src.getLength(0));
        int kEndPos = kLen;
        int ostart = -1;
        int oend = -1; // inclusive
        int run = 0;
        for (int pos = startRank; pos < endRank; ++pos) {
            while (kEndPos < pos) {
                ++k;
                kStart = toIntUnsigned(src.getValue(k));
                kLen = toIntUnsigned(src.getLength(k));
                kEndPos += 1 + kLen;
            }
            int key = kStart + kLen - (kEndPos - pos);
            if (ostart == -1) {
                ostart = oend = key;
            } else {
                if (key == oend + 1) {
                    oend = key;
                } else {
                    ans.setValue(run, (short) ostart);
                    final int len = oend - ostart;
                    ans.setLength(run, (short) len);
                    ans.cardinality += len + 1;
                    ++run;
                    ostart = oend = key;
                }
            }
        }
        ans.setValue(run, (short) ostart);
        final int len = oend - ostart;
        ans.setLength(run, (short) len);
        ans.cardinality += len + 1;
        ++run;
        ans.nbrruns = run;
        final int lenFromRuns = 2 * run;
        if (ans.valueslength.length - lenFromRuns > 64) {
            final short[] vs = new short[runsShortArraySizeRounding(ans.nbrruns)];
            System.arraycopy(ans.valueslength, 0, vs, 0, lenFromRuns);
            ans.valueslength = vs;
        }
        return ans;
    }

    // convert a bitmap container to a run container somewhat efficiently.
    protected RunContainer(final BitmapContainer bc) {
        nbrruns = bc.numberOfRuns();
        cardinality = bc.getCardinality();
        valueslength = new short[runsShortArraySizeRounding(nbrruns)];
        if (nbrruns == 0) {
            return;
        }

        int longCtr = 0; // index of current long in bitmap
        long curWord = bc.bitmap[0]; // its value
        int runCount = 0;
        while (true) {
            // potentially multiword advance to first 1 bit
            while (curWord == 0L && longCtr < bc.bitmap.length - 1) {
                curWord = bc.bitmap[++longCtr];
            }

            if (curWord == 0L) {
                // wrap up, no more runs
                return;
            }
            int localRunStart = Long.numberOfTrailingZeros(curWord);
            int runStart = localRunStart + 64 * longCtr;
            // stuff 1s into number's LSBs
            long curWordWith1s = curWord | (curWord - 1);

            // find the next 0, potentially in a later word
            int runEnd;
            while (curWordWith1s == -1L && longCtr < bc.bitmap.length - 1) {
                curWordWith1s = bc.bitmap[++longCtr];
            }

            if (curWordWith1s == -1L) {
                // a final unterminated run of 1s (32 of them)
                runEnd = 64 + longCtr * 64;
                setValue(runCount, (short) runStart);
                setLength(runCount, (short) (runEnd - runStart - 1));
                return;
            }
            int localRunEnd = Long.numberOfTrailingZeros(~curWordWith1s);
            runEnd = localRunEnd + longCtr * 64;
            setValue(runCount, (short) runStart);
            setLength(runCount, (short) (runEnd - runStart - 1));
            runCount++;
            // now, zero out everything right of runEnd.
            curWord = curWordWith1s & (curWordWith1s + 1);
            // We've lathered and rinsed, so repeat...
        }
    }

    /**
     * Create an array container with specified initial capacity
     *
     * @param runsCapacity The initial capacity of the container in number of runs.
     */
    public RunContainer(final int runsCapacity) {
        valueslength = new short[runsShortArraySizeRounding(runsCapacity)];
    }

    /**
     * Construct a new RunContainer backed by the provided array. Note that if you modify the
     * RunContainer a new array may be produced.
     *
     * @param array array where the data is stored
     * @param numRuns number of runs (each using 2 shorts in the buffer)
     */
    // @VisibleForTesting
    RunContainer(final short[] array, final int numRuns) {
        if (array.length < 2 * numRuns) {
            throw new RuntimeException("Mismatch between buffer and numRuns");
        }
        nbrruns = numRuns;
        valueslength = array;
        for (int i = 0; i < numRuns; ++i) {
            cardinality += getLengthAsInt(i) + 1;
        }
    }

    /**
     * Construct a new RunContainer using the provided array. The container takes ownership of the
     * array.
     *
     * @param valueslength array with valid runs, in increasing unsigned short order. The container
     *        takes ownership of this array.
     * @param nbrruns number of runs (the array should contain 2*n elements).
     * @param cardinality total cardinality in the runs.
     */
    public static RunContainer makeByWrapping(final short[] valueslength, final int nbrruns,
        final int cardinality) {
        return new RunContainer(valueslength, nbrruns, cardinality);
    }

    @Override
    public Container iadd(final int begin, final int end) {
        return iaddImpl(begin, end, () -> this, this::deepcopyIfShared);
    }

    @Override
    public Container add(final int begin, final int end) {
        return iaddImpl(begin, end, this::cowRef, this::deepCopy);
    }

    private Container iaddImpl(
        final int begin, final int end, final Supplier<RunContainer> self,
        final Supplier<RunContainer> copy) {
        if (end == begin) {
            return self.get();
        }
        if (begin > end || end > MAX_RANGE) {
            throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
        }

        final short k = (short) begin;
        final int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, k);
        if (begin == end - 1) {
            if (index >= 0) {
                return self.get();
            }
            return isetImpl(k, index, null, self, copy);
        }

        final RunContainer ans = copy.get();
        return ans.iaddUnsafe(begin, end, index < 0 ? ~index : index);
    }

    @Override
    public Container iset(final short k) {
        final int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, k);
        return iset(k, index);
    }

    private Container iset(final short k, final int index) {
        if (index >= 0) {
            return this;// already there
        }
        return isetImpl(k, index, null, () -> this, this::deepcopyIfShared);
    }

    @Override
    public Container set(final short k) {
        final int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, k);
        return setImpl(k, index);
    }

    private Container setImpl(final short k, final int index) {
        if (index >= 0) {
            return cowRef();// already there
        }
        return isetImpl(k, index, null, this::cowRef, this::deepCopy);
    }

    @Override
    Container iset(final short k, final PositionHint positionHint) {
        final int begin = Math.max(positionHint.value, 0);
        final int index = unsignedInterleavedBinarySearch(valueslength, begin, nbrruns, k);
        if (index >= 0) {
            positionHint.value = index;
            return this;
        }
        return isetImpl(k, index, positionHint, () -> this, this::deepcopyIfShared);
    }

    @Override
    Container set(final short k, final PositionHint positionHint) {
        final int begin = Math.max(positionHint.value, 0);
        final int index = unsignedInterleavedBinarySearch(valueslength, begin, nbrruns, k);
        if (index >= 0) {
            positionHint.value = index;
            return cowRef();
        }
        return isetImpl(k, index, positionHint, this::cowRef, this::deepCopy);
    }

    private Container isetImpl(final short k,
        int index,
        final PositionHint positionHint,
        final Supplier<RunContainer> self,
        final Supplier<RunContainer> copy) {
        index = -index - 2;// points to preceding value, possibly -1
        final int kAsInt = toIntUnsigned(k);
        if (index >= 0) {// possible match
            final int valueAtIndex = getValueAsInt(index);
            int offset = kAsInt - valueAtIndex;
            int le = getLengthAsInt(index);
            if (offset <= le) {
                setIfNotNull(positionHint, index);
                return self.get();
            }
            if (offset == le + 1) {
                // we may need to fuse
                if (index + 1 < nbrruns) {
                    final int valueAtIndexPlusOne = getValueAsInt(index + 1);
                    if (valueAtIndexPlusOne == kAsInt + 1) {
                        final int newEndInclusive = valueAtIndexPlusOne + getLengthAsInt(index + 1);
                        // indeed fusion is needed
                        if (nbrruns == 2) {
                            resetIfNotNull(positionHint);
                            return makeSingleRangeContainer(valueAtIndex, newEndInclusive + 1);
                        }
                        final RunContainer ans = copy.get();
                        final int newLen = newEndInclusive - valueAtIndex;
                        ans.setLength(index, (short) newLen);
                        ans.cardinality += newLen - le;
                        ans.recoverRoomAtIndex(index + 1);
                        setIfNotNull(positionHint, index);
                        return ans;
                    }
                }
                final RunContainer ans = copy.get();
                ans.incrementLength(index);
                setIfNotNull(positionHint, index);
                return ans;
            }
            if (index + 1 < nbrruns) {
                // we may need to fuse
                if (getValueAsInt(index + 1) == kAsInt + 1) {
                    // indeed fusion is needed
                    final RunContainer ans = copy.get();
                    ans.setValue(index + 1, k);
                    ans.incrementLength(index + 1);
                    setIfNotNull(positionHint, index + 1);
                    return ans;
                }
            }
        }
        if (index == -1) {
            // we may need to extend the first run
            if (nbrruns > 0) {
                if (getValueAsInt(0) == kAsInt + 1) {
                    final RunContainer ans = copy.get();
                    ans.incrementLength(0);
                    ans.decrementValue(0);
                    setIfNotNull(positionHint, 0);
                    return ans;
                }
            }
        }
        if (nbrruns >= ArrayContainer.DEFAULT_MAX_SIZE / 2) {
            if (positionHint != null) {
                positionHint.reset();
                return toBitmapContainer().iset(k, positionHint);
            }
            return toBitmapContainer().iset(k);
        }
        final RunContainer ans = copy.get();
        ans.makeRoomAtIndex(index + 1);
        ans.setValue(index + 1, k);
        ans.setLength(index + 1, (short) 0);
        ++ans.cardinality;
        setIfNotNull(positionHint, index + 1);
        return ans;
    }

    @Override
    public Container and(final ArrayContainer x) {
        if (x.isEmpty() || isEmpty()) {
            return Container.empty();
        }
        final ArrayContainer ac = new ArrayContainer(x.getCardinality());
        int rlepos = 0;
        int arraypos = 0;

        int rleval = getValueAsInt(rlepos);
        int rlelength = getLengthAsInt(rlepos);
        while (arraypos < x.getCardinality()) {
            int arrayval = toIntUnsigned(x.content[arraypos]);
            while (rleval + rlelength < arrayval) {// this will frequently be false
                ++rlepos;
                if (rlepos == nbrruns) {
                    return ac;// we are done
                }
                rleval = getValueAsInt(rlepos);
                rlelength = getLengthAsInt(rlepos);
            }
            if (rleval > arrayval) {
                arraypos = ContainerUtil.advanceUntil(x.content, arraypos, x.getCardinality(),
                    (short) rleval);
            } else {
                ac.content[ac.cardinality] = (short) arrayval;
                ++ac.cardinality;
                arraypos++;
            }
        }
        return ac;
    }


    @Override
    public Container and(final BitmapContainer x) {
        // could be implemented as return toBitmapOrArrayContainer().iand(x);
        if (x.isEmpty() || isEmpty()) {
            return Container.empty();
        }
        final int card = getCardinality();
        final int resultCardUpperBound = Math.min(card, x.getCardinality());
        if (resultCardUpperBound <= ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            final ArrayContainer answer = new ArrayContainer(resultCardUpperBound);
            final SearchRangeIterator xit = x.getShortRangeIterator(0);
            RUNS: for (int run = 0; run < nbrruns; ++run) {
                final int runStart = getValueAsInt(run);
                final int runLast = runStart + getLengthAsInt(run);
                final boolean valid = xit.advance(runStart);
                if (!valid) {
                    break;
                }
                while (xit.start() <= runLast) {
                    final int rBegin = Math.max(xit.start(), runStart);
                    final boolean inRun;
                    final int rEnd;
                    if (xit.end() < runLast + 1) {
                        inRun = true;
                        rEnd = xit.end();
                    } else {
                        inRun = false;
                        rEnd = runLast + 1;
                    }
                    for (int runValue = rBegin; runValue < rEnd; ++runValue) {
                        answer.content[answer.cardinality++] = (short) runValue;
                    }
                    if (!inRun) {
                        continue RUNS;
                    }
                    if (!xit.hasNext()) {
                        break RUNS;
                    }
                    xit.next();
                }
            }
            return answer.maybeSwitchContainer();
        }
        final BitmapContainer answer = x.deepCopy();
        int start = 0;
        for (int rlepos = 0; rlepos < nbrruns; ++rlepos) {
            int end = getValueAsInt(rlepos);
            int prevOnes = answer.cardinalityInRange(start, end);
            ContainerUtil.resetBitmapRange(answer.bitmap, start, end); // had been x.bitmap
            answer.updateCardinality(prevOnes, 0);
            start = end + getLengthAsInt(rlepos) + 1;
        }
        int ones = answer.cardinalityInRange(start, MAX_RANGE);
        ContainerUtil.resetBitmapRange(answer.bitmap, start, MAX_RANGE); // had been x.bitmap
        answer.updateCardinality(ones, 0);
        return answer.runOptimize();
    }

    @Override
    public Container and(final RunContainer x) {
        if (x.isEmpty() || isEmpty()) {
            return Container.empty();
        }
        final RunContainer answer = new RunContainer(nbrruns + x.nbrruns);
        int rlepos = 0;
        int xrlepos = 0;
        int start = getValueAsInt(rlepos);
        int end = start + getLengthAsInt(rlepos) + 1;
        int xstart = toIntUnsigned(x.getValue(xrlepos));
        int xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
        while ((rlepos < nbrruns) && (xrlepos < x.nbrruns)) {
            if (end <= xstart) {
                if (ENABLE_GALLOPING_AND) {
                    rlepos = skipAhead(this, rlepos, xstart); // skip over runs until we have end >
                                                              // xstart (or
                    // rlepos is advanced beyond end)
                } else {
                    ++rlepos;
                }

                if (rlepos < nbrruns) {
                    start = getValueAsInt(rlepos);
                    end = start + getLengthAsInt(rlepos) + 1;
                }
            } else if (xend <= start) {
                // exit the second run
                if (ENABLE_GALLOPING_AND) {
                    xrlepos = skipAhead(x, xrlepos, start);
                } else {
                    ++xrlepos;
                }

                if (xrlepos < x.nbrruns) {
                    xstart = toIntUnsigned(x.getValue(xrlepos));
                    xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
                }
            } else {// they overlap
                final int lateststart = Math.max(start, xstart);
                int earliestend;
                if (end == xend) {// improbable
                    earliestend = end;
                    rlepos++;
                    xrlepos++;
                    if (rlepos < nbrruns) {
                        start = getValueAsInt(rlepos);
                        end = start + getLengthAsInt(rlepos) + 1;
                    }
                    if (xrlepos < x.nbrruns) {
                        xstart = toIntUnsigned(x.getValue(xrlepos));
                        xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
                    }
                } else if (end < xend) {
                    earliestend = end;
                    rlepos++;
                    if (rlepos < nbrruns) {
                        start = getValueAsInt(rlepos);
                        end = start + getLengthAsInt(rlepos) + 1;
                    }

                } else {// end > xend
                    earliestend = xend;
                    xrlepos++;
                    if (xrlepos < x.nbrruns) {
                        xstart = toIntUnsigned(x.getValue(xrlepos));
                        xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
                    }
                }
                answer.valueslength[2 * answer.nbrruns] = (short) lateststart;
                final int d = earliestend - lateststart;
                answer.valueslength[2 * answer.nbrruns + 1] = (short) (d - 1);
                answer.cardinality += d;
                answer.nbrruns++;
            }
        }
        return answer.toEfficientContainer();
    }

    private Container andRangeImpl(final boolean inPlace, final int start, final int end) {
        if (end <= start || isEmpty()) {
            return Container.empty();
        }
        int srun = searchFrom(lowbits(start), 0);
        if (srun < 0) {
            srun = ~srun;
            if (srun >= nbrruns) {
                return Container.empty();
            }
        }
        final int rsval = getValueAsInt(srun);
        if (end <= rsval) {
            return Container.empty();
        }
        int erun = searchFrom(lowbits(end - 1), srun);
        if (erun < 0) {
            erun = ~erun;
            if (erun >= nbrruns) {
                erun = nbrruns - 1;
            } else if (end <= getValueAsInt(erun)) {
                --erun;
            }
        }
        if (srun == erun) {
            final int rend = Math.min(rsval + getLengthAsInt(srun) + 1, end);
            final int rstart = Math.max(start, rsval);
            return Container.singleRange(rstart, rend);
        }
        if (srun + 1 == erun) {
            final int r2start = getValueAsInt(erun);
            final int r2end = Math.min(r2start + getLengthAsInt(erun) + 1, end);
            final int r1start = Math.max(start, rsval);
            final int r1end = rsval + getLengthAsInt(srun) + 1;
            return Container.twoRanges(r1start, r1end, r2start, r2end);
        }
        final int lastVal = getValueAsInt(erun);
        final int erunLen = getLengthAsInt(erun);
        final int erunEnd = lastVal + erunLen + 1;
        if (srun == 0 && start <= rsval && erun == nbrruns - 1 && erunEnd <= end) {
            return inPlace ? this : cowRef();
        }
        final RunContainer ans;
        if (inPlace) {
            ans = this;
            ans.cardinality = 0; // we are going to building ourselves over on top of the same
                                 // array.
        } else {
            ans = new RunContainer(erun - srun + 1);
        }
        int n = 0;
        final int firstVal = Math.max(start, rsval);
        final int firstEnd = rsval + getLengthAsInt(srun) + 1;
        ans.valueslength[n++] = (short) firstVal;
        int d = firstEnd - firstVal;
        ans.valueslength[n++] = (short) (d - 1);
        ans.cardinality += d;
        for (int run = srun + 1; run < erun; ++run) {
            ans.valueslength[n++] = getValue(run);
            final short len = getLength(run);
            ans.valueslength[n++] = len;
            ans.cardinality += toIntUnsigned(len) + 1;
        }
        ans.valueslength[n++] = (short) lastVal;
        final int lastEnd = Math.min(erunEnd, end);
        d = lastEnd - lastVal;
        ans.valueslength[n++] = (short) (d - 1);
        ans.nbrruns = n / 2;
        ans.cardinality += d;
        return ans;
    }

    @Override
    public Container andRange(final int start, final int end) {
        return andRangeImpl(false, start, end);
    }

    @Override
    public Container iandRange(final int start, final int end) {
        return andRangeImpl(!shared, start, end);
    }

    @Override
    public Container andNot(final ArrayContainer x) {
        if (x.isEmpty()) {
            return cowRef();
        }
        // when x is small, we guess that the result will still be a run container
        final int arbitrary_threshold = 32; // this is arbitrary
        if (x.getCardinality() < arbitrary_threshold) {
            return andNotAsRun(x).toEfficientContainer();
        }
        // otherwise we generate either an array or bitmap container
        final int card = getCardinality();
        if (card <= ArrayContainer.DEFAULT_MAX_SIZE) {
            // if the cardinality is small, we construct the solution in place
            final ArrayContainer ac = new ArrayContainer(card);
            ac.cardinality =
                ContainerUtil.unsignedDifference(getShortIterator(), x.getShortIterator(),
                    ac.content);
            return ac;
        }
        // otherwise, we generate a bitmap
        return toBitmapOrArrayContainer(card).iandNot(x);
    }

    @Override
    public Container andNot(final BitmapContainer x) {
        if (x.isEmpty()) {
            return cowRef();
        }
        // could be implemented as toTemporaryBitmap().iandNot(x);
        int card = getCardinality();
        if (card <= ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            // result can only be an array (assuming that we never make a RunContainer)
            final ArrayContainer answer = new ArrayContainer(card);
            answer.cardinality = 0;
            for (int rlepos = 0; rlepos < nbrruns; ++rlepos) {
                int runStart = getValueAsInt(rlepos);
                int runEnd = runStart + getLengthAsInt(rlepos);
                for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                    if (!x.contains((short) runValue)) {// it looks like contains() should be cheap
                                                        // enough if
                        // accessed sequentially
                        answer.content[answer.cardinality++] = (short) runValue;
                    }
                }
            }
            return answer;
        }
        // we expect the answer to be a bitmap (if we are lucky)
        final BitmapContainer answer = x.deepCopy();
        int lastPos = 0;
        for (int rlepos = 0; rlepos < nbrruns; ++rlepos) {
            int start = getValueAsInt(rlepos);
            int end = start + getLengthAsInt(rlepos) + 1;
            int prevOnes = answer.cardinalityInRange(lastPos, start);
            int flippedOnes = answer.cardinalityInRange(start, end);
            ContainerUtil.resetBitmapRange(answer.bitmap, lastPos, start);
            ContainerUtil.flipBitmapRange(answer.bitmap, start, end);
            answer.updateCardinality(prevOnes + flippedOnes, end - start - flippedOnes);
            lastPos = end;
        }
        int ones = answer.cardinalityInRange(lastPos, MAX_RANGE);
        ContainerUtil.resetBitmapRange(answer.bitmap, lastPos, MAX_RANGE);
        answer.updateCardinality(ones, 0);
        return answer.maybeSwitchContainer();
    }

    @Override
    public Container andNot(final RunContainer x) {
        if (x.isEmpty()) {
            return cowRef();
        }
        final RunContainer ans = new RunContainer(nbrruns + x.nbrruns);
        int rlepos = 0;
        int xrlepos = 0;
        int start = getValueAsInt(rlepos);
        int end = start + getLengthAsInt(rlepos) + 1;
        int xstart = toIntUnsigned(x.getValue(xrlepos));
        int xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
        while ((rlepos < nbrruns) && (xrlepos < x.nbrruns)) {
            if (end <= xstart) {
                // output the first run
                ans.valueslength[2 * ans.nbrruns] = (short) start;
                final int d = end - start;
                ans.valueslength[2 * ans.nbrruns + 1] = (short) (d - 1);
                ++ans.nbrruns;
                ans.cardinality += d;
                ++rlepos;
                if (rlepos < nbrruns) {
                    start = getValueAsInt(rlepos);
                    end = start + getLengthAsInt(rlepos) + 1;
                }
            } else if (xend <= start) {
                // exit the second run
                ++xrlepos;
                if (xrlepos < x.nbrruns) {
                    xstart = toIntUnsigned(x.getValue(xrlepos));
                    xend = xstart + toIntUnsigned(x.getLength(xrlepos)) + 1;
                }
            } else {
                if (start < xstart) {
                    ans.valueslength[2 * ans.nbrruns] = (short) start;
                    final int d = xstart - start;
                    ans.valueslength[2 * ans.nbrruns + 1] = (short) (d - 1);
                    ++ans.nbrruns;
                    ans.cardinality += d;
                }
                if (xend < end) {
                    start = xend;
                } else {
                    rlepos++;
                    if (rlepos < nbrruns) {
                        start = getValueAsInt(rlepos);
                        end = start + getLengthAsInt(rlepos) + 1;
                    }
                }
            }
        }
        if (rlepos < nbrruns) {
            ans.valueslength[2 * ans.nbrruns] = (short) start;
            final int d = end - start;
            ans.valueslength[2 * ans.nbrruns + 1] = (short) (d - 1);
            ++ans.nbrruns;
            ans.cardinality += d;
            ++rlepos;
            if (rlepos < nbrruns) {
                System.arraycopy(valueslength, 2 * rlepos, ans.valueslength, 2 * ans.nbrruns,
                    2 * (nbrruns - rlepos));
                for (int run = rlepos; run < nbrruns; ++run) {
                    ans.cardinality += getLengthAsInt(run) + 1;
                }
                ans.nbrruns = ans.nbrruns + nbrruns - rlepos;
            }
        }
        return ans.toEfficientContainer();
    }

    // Append a value length with all values until a given value
    private void appendValueLength(final int value, final int index) {
        int previousValue = getValueAsInt(index);
        int length = getLengthAsInt(index);
        int offset = value - previousValue;
        if (offset > length) {
            setLength(index, (short) offset);
            cardinality += offset - length;
        }
    }

    // To check if a value length can be prepended with a given value
    private boolean canPrependValueLength(final int value, final int index) {
        if (index < nbrruns) {
            int nextValue = getValueAsInt(index);
            return nextValue == value + 1;
        }
        return false;
    }

    public void clear() {
        if (nbrruns != 0 && shared) {
            throw new IllegalStateException("Cannot clear a shared container.");
        }
        nbrruns = 0;
        cardinality = 0;
    }

    @Override
    public RunContainer deepCopy() {
        return new RunContainer(this);
    }

    @Override
    public RunContainer cowRef() {
        setCopyOnWrite();
        return this;
    }

    @Override
    public boolean isEmpty() {
        return nbrruns == 0;
    }

    @Override
    public boolean isAllOnes() {
        return nbrruns == 1 && valueslength[1] == (short) MAX_VALUE;
    }

    @Override
    public boolean isSingleElement() {
        return nbrruns == 1 && valueslength[1] == (short) 0;
    }

    // To set the last value of a value length
    private void closeValueLength(final int value, final int index) {
        final int initialValue = getValueAsInt(index);
        final int initialLen = getLengthAsInt(index);
        final int newLen = value - initialValue;
        setLength(index, (short) newLen);
        cardinality -= initialLen - newLen;
    }

    @Override
    public boolean contains(final short x) {
        return searchFrom(x, 0) >= 0;
    }

    /**
     * @param x a value to check for membership to this container
     * @param i a position in runs space where to begin a binary search
     * @return the index of the run that contains x, if x is contained in some existing run; if x is
     *         not part of an existing run, -(index+1) where index is the position where x would be
     *         "inserted". Note "inserting" x may mean either (a) adding x as the new ending for the
     *         run in the <em><previous/em> position, (b) inserting a new run with only x pushing
     *         the run in the position returned right, or (c) inserting x as the new start of the
     *         existing run in that position.
     */
    int searchFrom(final short x, final int i) {
        int index = unsignedInterleavedBinarySearch(valueslength, i, nbrruns, x);
        if (index >= 0) {
            return index;
        }
        return searchSecondHalf(x, index);
    }

    int searchSecondHalf(final short x, final int index) {
        int index2 = -index - 2; // points to preceding value, possibly -1
        if (index2 != -1) {// possible match
            int offset = toIntUnsigned(x) - getValueAsInt(index2);
            int le = getLengthAsInt(index2);
            if (offset <= le) {
                return index2;
            }
        }
        return index;
    }

    @Override
    public boolean contains(final int rangeStart, final int rangeEnd) {
        int ri = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, (short) rangeStart);
        if (ri >= 0) {
            final int len = getLengthAsInt(ri);
            return rangeEnd - rangeStart <= len + 1;
        }
        int ri2 = -ri - 2; // points to preceding value, possibly -1
        if (ri2 == -1) {
            return false;
        }
        final int v = getValueAsInt(ri2);
        final int lastOfThatRange = v + getLengthAsInt(ri2);
        return rangeEnd <= lastOfThatRange + 1;
    }

    @Override
    protected boolean contains(final RunContainer runContainer) {
        int i1 = 0, i2 = 0;
        while (i1 < numberOfRuns() && i2 < runContainer.numberOfRuns()) {
            int start1 = getValueAsInt(i1);
            int stop1 = start1 + getLengthAsInt(i1);
            int start2 = runContainer.getValueAsInt(i2);
            int stop2 = start2 + runContainer.getLengthAsInt(i2);
            if (start1 > start2) {
                return false;
            } else {
                if (stop1 > stop2) {
                    i2++;
                } else if (stop1 == stop2) {
                    i1++;
                    i2++;
                } else {
                    i1++;
                }
            }
        }
        return i2 == runContainer.numberOfRuns();
    }

    @Override
    protected boolean contains(final ArrayContainer arrayContainer) {
        final int cardinality = getCardinality();
        final int runCount = numberOfRuns();
        if (arrayContainer.getCardinality() > cardinality) {
            return false;
        }
        int ia = 0, ir = 0;
        while (ia < arrayContainer.getCardinality() && ir < runCount) {
            int start = getValueAsInt(ir);
            int stop = start + getLengthAsInt(ir);
            int ac = toIntUnsigned(arrayContainer.content[ia]);
            if (ac < start) {
                return false;
            } else if (ac > stop) {
                ++ir;
            } else {
                ++ia;
            }
        }
        return ia == arrayContainer.getCardinality();
    }

    @Override
    protected boolean contains(final BitmapContainer bitmapContainer) {
        final int cardinality = getCardinality();
        if (bitmapContainer.getCardinality() != -1
            && bitmapContainer.getCardinality() > cardinality) {
            return false;
        }
        final int runCount = numberOfRuns();
        short ib = 0, ir = 0;
        while (ib < bitmapContainer.bitmap.length && ir < runCount) {
            long w = bitmapContainer.bitmap[ib];
            while (w != 0 && ir < runCount) {
                int start = getValueAsInt(ir);
                int stop = start + getLengthAsInt(ir);
                long t = w & -w;
                long r = ib * 64 + Long.numberOfTrailingZeros(w);
                if (r < start) {
                    return false;
                } else if (r > stop) {
                    ++ir;
                } else {
                    w ^= t;
                }
            }
            if (w == 0) {
                ++ib;
            } else {
                return false;
            }
        }
        if (ib < bitmapContainer.bitmap.length) {
            for (; ib < bitmapContainer.bitmap.length; ++ib) {
                if (bitmapContainer.bitmap[ib] != 0) {
                    return false;
                }
            }
        }
        return true;
    }

    // Push all values length to the end of the array (resize array if needed)
    private void copyToOffset(final int offset) {
        final int newRuns = offset + nbrruns;
        final int minCapacity = 2 * newRuns;
        if (valueslength.length < minCapacity) {
            // expensive case where we need to reallocate
            final int newCapacity = runsShortArraySizeRounding(newRuns);
            final short[] newvalueslength = new short[newCapacity];
            copyValuesLength(valueslength, 0, newvalueslength, offset, nbrruns);
            valueslength = newvalueslength;
        } else {
            // efficient case where we just copy
            copyValuesLength(valueslength, 0, valueslength, offset, nbrruns);
        }
    }

    private void copyValuesLength(
        final short[] src, final int srcIndex, final short[] dst, final int dstIndex,
        final int length) {
        System.arraycopy(src, 2 * srcIndex, dst, 2 * dstIndex, 2 * length);
    }

    // caller is responsible to ensure that value is non-zero
    private void decrementLength(final int index) {
        --valueslength[2 * index + 1];
        --cardinality;
    }

    private void decrementValue(final int index) {
        --valueslength[2 * index];
    }

    private static int nextRunsCapacity(final int oldRuns) {
        return (oldRuns == 0) ? DEFAULT_INIT_SIZE_IN_RUNS
            : oldRuns < 32 ? runsSizeRounding(oldRuns * 2)
                : oldRuns < 512 ? runsSizeRounding(oldRuns * 3 / 2)
                    : runsSizeRounding(oldRuns * 5 / 4);

    }

    protected void ensureCapacity(final int minNbRuns) {
        final int minCapacity = 2 * minNbRuns;
        if (valueslength.length >= minCapacity) {
            return;
        }
        int newRunsCapacity = nbrruns;
        do {
            newRunsCapacity = nextRunsCapacity(newRunsCapacity);
        } while (newRunsCapacity < minNbRuns);
        short[] nv = new short[2 * newRunsCapacity];
        copyValuesLength(valueslength, 0, nv, 0, nbrruns);
        valueslength = nv;
    }

    @Override
    public Container iflip(final short x) {
        final boolean contains;
        final int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, x);
        if (index >= 0) {
            contains = true;
        } else {
            contains = searchSecondHalf(x, index) >= 0;
        }
        if (contains) {
            return iunset(x, index);
        } else {
            return iset(x, index);
        }
    }

    @Override
    public int getCardinality() {
        return cardinality;
    }

    /**
     * Gets the length of the run at the index.
     *
     * @param index the index of the run.
     * @return the length of the run at the index.
     * @throws ArrayIndexOutOfBoundsException if index is negative or larger than the index of the
     *         last run.
     */
    public short getLength(final int index) {
        return valueslength[2 * index + 1];
    }

    public int getLengthAsInt(final int index) {
        return toIntUnsigned(getLength(index));
    }

    static final class ReverseShortIterator implements ShortAdvanceIterator {
        private int nextRange;
        private int curr = -1;
        private int rangeStart = -1;
        private RunContainer parent;

        private int runStart(final int i) {
            return parent.getValueAsInt(i);
        }

        @SuppressWarnings("unused")
        private int runLast(final int i) {
            return runLast(runStart(i), i);
        }

        private int runLast(final int runStart, final int i) {
            return runStart + parent.getLengthAsInt(i);
        }

        ReverseShortIterator(final RunContainer p) {
            wrap(p);
        }

        @Override
        public boolean hasNext() {
            return curr > rangeStart || nextRange >= 0;
        }

        @Override
        public short next() {
            return (short) nextAsInt();
        }

        @Override
        public int nextAsInt() {
            if (--curr < rangeStart && nextRange >= 0) {
                updateFromNextRange();
                --nextRange;
            }
            return curr;
        }

        @Override
        public short curr() {
            return (short) currAsInt();
        }

        @Override
        public int currAsInt() {
            return curr;
        }

        @Override
        public boolean advance(final int v) {
            if (curr < 0) {
                if (nextRange == -1) {
                    return false;
                }
                next();
            }
            if (curr <= v) {
                return true;
            }
            if (rangeStart <= v) {
                curr = Math.max(rangeStart, v);
                return true;
            }
            if (nextRange < 0) {
                return false;
            }

            int left = 0;
            int runStartLeft = runStart(0);
            int runLastLeft = runLast(runStartLeft, 0);
            if (v <= runLastLeft) {
                nextRange = -1;
                if (v <= runStartLeft) {
                    curr = rangeStart = runStartLeft;
                    return v == runStartLeft;
                }
                curr = Math.min(v, runLastLeft);
                rangeStart = runStartLeft;
                return true;
            }
            // We know v is to the right of the run at position left.

            int right = nextRange;
            int runStartRight = runStart(right);
            if (runStartRight <= v) {
                rangeStart = runStartRight;
                curr = Math.min(v, runLast(runStartRight, nextRange));
                --nextRange;
                return true;
            }
            // We know v is to the left of the run at position right.

            // Binary search over start elements.
            // At this point neither left nor right contain v, and there is at least one other range
            // between them
            // thus we're guaranteed to advance.
            // Note as this loop iterates, it is always true neither left nor right contain v.
            while (true) {
                nextRange = (left + right) / 2;
                final int runStartPos = runStart(nextRange);
                if (v < runStartPos) {
                    right = nextRange;
                    continue;
                }
                runStartLeft = runStartPos;
                runLastLeft = runLast(runStartPos, nextRange);
                if (left == nextRange) {
                    curr = runLastLeft;
                    rangeStart = runStartLeft;
                    --nextRange;
                    return true;
                }
                if (v <= runLastLeft) {
                    curr = Math.min(v, runLastLeft);
                    rangeStart = runStartPos;
                    --nextRange;
                    return true;
                }
                left = nextRange;
            }
        }

        private void updateFromNextRange() {
            rangeStart = runStart(nextRange);
            curr = runLast(rangeStart, nextRange);
        }

        void wrap(RunContainer p) {
            parent = p;
            nextRange = parent.nbrruns - 1;
        }
    }

    @Override
    public ShortAdvanceIterator getReverseShortIterator() {
        return new ReverseShortIterator(this);
    }

    static class ForwardShortIterator implements ShortIterator {
        protected int pos;
        protected int le = 0;
        protected int maxlength;
        protected int base;
        protected RunContainer parent;

        ForwardShortIterator(final RunContainer p) {
            wrap(p);
        }

        @Override
        public boolean hasNext() {
            return pos < parent.nbrruns;
        }

        @Override
        public short next() {
            return lowbits(nextAsInt());
        }

        @Override
        public int nextAsInt() {
            int ans = base + le;
            le++;
            if (le > maxlength) {
                pos++;
                le = 0;
                if (pos < parent.nbrruns) {
                    maxlength = parent.getLengthAsInt(pos);
                    base = parent.getValueAsInt(pos);
                }
            }
            return ans;
        }

        void wrap(final RunContainer p) {
            parent = p;
            pos = 0;
            le = 0;
            if (pos < parent.nbrruns) {
                maxlength = parent.getLengthAsInt(pos);
                base = parent.getValueAsInt(pos);
            }
        }
    }

    @Override
    public ShortIterator getShortIterator() {
        return new ForwardShortIterator(this);
    }

    @Override
    public ContainerShortBatchIterator getShortBatchIterator(final int skipCount) {
        return new RunShortBatchIterator(this, skipCount);
    }

    @Override
    public SearchRangeIterator getShortRangeIterator(final int initialSeek) {
        return new RunContainerRangeIterator(this, initialSeek);
    }

    /**
     * Gets the value of the first element of the run at the index.
     *
     * @param index the index of the run.
     * @return the value of the first element of the run at the index.
     * @throws ArrayIndexOutOfBoundsException if index is negative or larger than the index of the
     *         last run.
     */
    public short getValue(final int index) {
        return valueslength[2 * index];
    }

    public int getValueAsInt(final int index) {
        return toIntUnsigned(getValue(index));
    }

    public RunContainer iaddUnsafe(final int begin, final int end, final int searchBeginRunIndex) {
        int bIndex = unsignedInterleavedBinarySearch(valueslength, searchBeginRunIndex, nbrruns,
            (short) begin);
        int eIndex;
        if (bIndex >= 0) {
            eIndex =
                unsignedInterleavedBinarySearch(valueslength, bIndex, nbrruns, (short) (end - 1));
        } else {
            final int effectiveBeginIndex = ~bIndex;
            if (effectiveBeginIndex >= nbrruns) {
                eIndex = bIndex;
            } else {
                eIndex = unsignedInterleavedBinarySearch(valueslength, effectiveBeginIndex, nbrruns,
                    (short) (end - 1));
            }
        }

        if (bIndex >= 0) {
            if (eIndex >= 0) {
                mergeValuesLength(bIndex, eIndex);
                return this;
            }
            // eIndex < 0.
            eIndex = -eIndex - 2;
            if (canPrependValueLength(end - 1, eIndex + 1)) {
                mergeValuesLength(bIndex, eIndex + 1);
                return this;
            }

            appendValueLength(end - 1, eIndex);
            mergeValuesLength(bIndex, eIndex);
            return this;
        }
        // bIndex < 0.
        if (eIndex >= 0) {
            bIndex = -bIndex - 2;

            if (bIndex >= 0) {
                if (valueLengthContains(begin - 1, bIndex)) {
                    mergeValuesLength(bIndex, eIndex);
                    return this;
                }
            }
            prependValueLength(begin, bIndex + 1);
            mergeValuesLength(bIndex + 1, eIndex);
            return this;
        }
        // eIndex < 0.
        bIndex = -bIndex - 2;
        eIndex = -eIndex - 2;

        if (eIndex >= 0) {
            if (bIndex >= 0) {
                if (!valueLengthContains(begin - 1, bIndex)) {
                    if (bIndex == eIndex) {
                        if (canPrependValueLength(end - 1, eIndex + 1)) {
                            prependValueLength(begin, eIndex + 1);
                            return this;
                        }
                        makeRoomAtIndex(eIndex + 1);
                        setValue(eIndex + 1, (short) begin);
                        final int d = end - begin;
                        setLength(eIndex + 1, (short) (d - 1));
                        cardinality += d;
                        return this;
                    } else {
                        bIndex++;
                        prependValueLength(begin, bIndex);
                    }
                }
            } else {
                bIndex = 0;
                prependValueLength(begin, bIndex);
            }

            if (canPrependValueLength(end - 1, eIndex + 1)) {
                mergeValuesLength(bIndex, eIndex + 1);
                return this;
            }

            appendValueLength(end - 1, eIndex);
            mergeValuesLength(bIndex, eIndex);
            return this;
        }

        if (canPrependValueLength(end - 1, 0)) {
            prependValueLength(begin, 0);
        } else {
            makeRoomAtIndex(0);
            setValue(0, (short) begin);
            final int d = end - begin;
            setLength(0, (short) (d - 1));
            cardinality += d;
        }
        return this;
    }

    @Override
    public Container iappend(final int begin, final int end) {
        // Note we don't validate inputs in the same way
        // as other methods; this method is part of the fast path for builders.
        if (end <= begin) {
            return this;
        }
        if (nbrruns == 0) {
            final RunContainer ans;
            if (shared) {
                ans = new RunContainer();
            } else {
                ans = this;
            }
            ans.valueslength[0] = (short) begin;
            final int d = end - begin;
            ans.valueslength[1] = (short) (d - 1);
            ans.nbrruns = 1;
            ans.cardinality = d;
            return ans;
        }
        final int lastValue = getValueAsInt(nbrruns - 1);
        final int lastLen = getLengthAsInt(nbrruns - 1);
        if (lastValue + lastLen + 1 == begin) {
            final RunContainer ans = deepcopyIfShared();
            final int d = end - begin;
            ans.valueslength[2 * nbrruns - 1] = (short) (lastLen + d);
            ans.cardinality += d;
            return ans;
        }
        final int capacityForAns;
        final int capacityNeeded = 2 * (nbrruns + 1);
        if (capacityNeeded > valueslength.length) {
            capacityForAns = nextCapacity();
            if (capacityForAns > ArrayContainer.DEFAULT_MAX_SIZE) {
                final Container bitmapContainer = toBitmapContainer();
                return bitmapContainer.iappend(begin, end);
            }
        } else {
            capacityForAns = valueslength.length;
        }
        final RunContainer ans;
        if (shared) {
            final short[] newValuesLength =
                getValuesLengthInBiggerArray(runsShortArraySizeRounding(capacityForAns / 2));
            ans = makeByWrapping(newValuesLength, nbrruns, cardinality);
        } else {
            ans = this;
            if (capacityForAns != valueslength.length) {
                increaseCapacityTo(runsShortArraySizeRounding(capacityForAns / 2));
            }
        }
        ans.valueslength[2 * nbrruns] = (short) begin;
        final int d = end - begin;
        ans.valueslength[2 * nbrruns + 1] = (short) (d - 1);
        ans.cardinality += d;
        ++ans.nbrruns;
        return ans;
    }

    @Override
    public Container iand(final ArrayContainer x) {
        return and(x);
    }

    @Override
    public Container iand(final BitmapContainer x) {
        return and(x);
    }

    @Override
    public Container iand(final RunContainer x) {
        return and(x);
    }


    @Override
    public Container iandNot(final ArrayContainer x) {
        if (x.isEmpty()) {
            return this;
        }
        return andNot(x);
    }

    @Override
    public Container iandNot(final BitmapContainer x) {
        if (x.isEmpty()) {
            return this;
        }
        return andNot(x);
    }

    @Override
    public Container iandNot(final RunContainer x) {
        if (x.isEmpty()) {
            return this;
        }
        return andNot(x);
    }

    private int nextCapacity() {
        int newCapacity = (valueslength.length == 0) ? DEFAULT_INIT_SIZE_IN_RUNS
            : valueslength.length < 64 ? valueslength.length * 2
                : valueslength.length < 1024 ? valueslength.length * 3 / 2
                    : valueslength.length * 5 / 4;
        return newCapacity;
    }

    private short[] getValuesLengthInBiggerArray(final int newCapacity) {
        final short[] nv = new short[newCapacity];
        System.arraycopy(valueslength, 0, nv, 0, 2 * nbrruns);
        return nv;
    }

    private void increaseCapacityTo(final int newCapacity) {
        valueslength = getValuesLengthInBiggerArray(newCapacity);
    }

    private void increaseCapacity() {
        increaseCapacityTo(nextCapacity());
    }

    private void incrementLength(final int index) {
        ++valueslength[2 * index + 1];
        ++cardinality;
    }

    private void incrementValue(final int index) {
        ++valueslength[2 * index];
    }

    // To remove part of a values length.
    private void chopValueLength(final int value, final int index) {
        int initialValue = getValueAsInt(index);
        int length = getLengthAsInt(index);
        setValue(index, (short) (value));
        final int d = value - initialValue;
        setLength(index, (short) (length - d));
        cardinality -= d;
    }

    @Override
    public Container inot(final int rangeStart, final int rangeEnd) {
        if (rangeEnd <= rangeStart) {
            return this;
        }

        // the +1 below is needed in case the valueslength.length is odd
        if (valueslength.length <= 2 * nbrruns + 1) {
            // no room for expansion
            // analyze whether this is a case that will require expansion (that we cannot do)
            // this is a bit costly now (4 "contains" checks)

            boolean lastValueBeforeRange = false;
            boolean firstValueInRange;
            boolean lastValueInRange;
            boolean firstValuePastRange = false;

            // contains is based on a binary search and is hopefully fairly fast.
            // however, one binary search could *usually* suffice to find both
            // lastValueBeforeRange AND firstValueInRange. ditto for
            // lastVaueInRange and firstValuePastRange

            // find the start of the range
            if (rangeStart > 0) {
                lastValueBeforeRange = contains((short) (rangeStart - 1));
            }
            firstValueInRange = contains((short) rangeStart);

            if (lastValueBeforeRange == firstValueInRange) {
                // expansion is required if also lastValueInRange==firstValuePastRange

                // tougher to optimize out, but possible.
                lastValueInRange = contains((short) (rangeEnd - 1));
                if (rangeEnd != MAX_RANGE) {
                    firstValuePastRange = contains((short) rangeEnd);
                }

                // there is definitely one more run after the operation.
                if (lastValueInRange == firstValuePastRange) {
                    return not(rangeStart, rangeEnd); // can't do in-place: true space limit
                }
            }
        }
        // either no expansion required, or we have room to handle any required expansion for it.

        // remaining code is just a minor variation on not()
        int myNbrRuns = nbrruns;

        // we can't use deepCopy/deepCopyIfShared because we already tested valueslentgh.lentgth
        // above,
        // and deepCopy may reduce valueslentgh.lentgth
        final RunContainer ans;
        if (shared) {
            ans = new RunContainer(Arrays.copyOf(valueslength, valueslength.length), 0, 0);
        } else {
            nbrruns = 0; // losing nbrruns, which is stashed in myNbrRuns.
            cardinality = 0;
            ans = this;
        }
        int k = 0;

        // could try using unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, rangeStart)
        // instead
        // of sequential scan
        // to find the starting location

        for (; (k < myNbrRuns) && (getValueAsInt(k) < rangeStart); ++k) {
            // since it is atop self, there is no copying needed
            // ans.valueslength[2 * k] = valueslength[2 * k];
            // ans.valueslength[2 * k + 1] = valueslength[2 * k + 1];
            ans.cardinality += getLengthAsInt(k) + 1;
            ++ans.nbrruns;
        }
        // We will work left to right, with a read pointer that always stays
        // left of the write pointer. However, we need to give the read pointer a head start.
        // use local variables so we are always reading 1 location ahead.

        short bufferedValue = 0, bufferedLength = 0; // MAX_VALUE start and MAX_VALUE length would
                                                     // be illegal,
        // could use as sentinel
        short nextValue = 0, nextLength = 0;
        if (k < myNbrRuns) { // prime the readahead variables
            bufferedValue = getValue(k);
            bufferedLength = getLength(k);
        }

        ans.smartAppendForXor((short) rangeStart, (short) (rangeEnd - rangeStart - 1));

        for (; k < myNbrRuns; ++k) {
            if (ans.nbrruns > k + 1) {
                throw new RuntimeException(
                    "internal error in inot, writer has overtaken reader!! " + k + " "
                        + ans.nbrruns);
            }
            if (k + 1 < myNbrRuns) {
                nextValue = getValue(k + 1); // readahead for next iteration
                nextLength = getLength(k + 1);
            }
            ans.smartAppendForXor(bufferedValue, bufferedLength);
            bufferedValue = nextValue;
            bufferedLength = nextLength;
        }
        // the number of runs can increase by one, meaning (rarely) a bitmap will become better
        // or the cardinality can decrease by a lot, making an array better
        return ans.toEfficientContainer();
    }

    @Override
    public Container ior(final ArrayContainer x) {
        if (shared) {
            return or(x);
        }
        if (isAllOnes() || x.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return x.cowRef();
        }
        final int offset = x.getCardinality();
        copyToOffset(offset);
        final int myRuns = nbrruns;
        nbrruns = 0;
        cardinality = 0;
        orImpl(this, this, myRuns, offset, x);
        return toEfficientContainer();
    }

    @Override
    public Container ior(final BitmapContainer x) {
        if (isAllOnes() || x.isEmpty()) {
            return this;
        }
        if (x.isAllOnes()) {
            return Container.full();
        }
        if (isEmpty()) {
            return x.cowRef();
        }
        return or(x);
    }

    @Override
    public Container ior(final RunContainer x) {
        if (shared) {
            return or(x);
        }
        if (isAllOnes() || x.isEmpty()) {
            return this;
        }
        if (isEmpty() || x.isAllOnes()) {
            return x.cowRef();
        }
        final int srcOffset = x.nbrruns;
        copyToOffset(srcOffset);
        final int srcRuns = nbrruns;
        nbrruns = 0;
        cardinality = 0;
        orImpl(this, this, srcRuns, srcOffset, x);
        return toEfficientContainer();
    }

    /**
     * Calculate the result of an or between two RunContainers storing the result in a RunContainer.
     *
     * @param dst A RunContainer where to store the result. Its nbrruns field member should be zero
     *        on entry.
     * @param src A RunContainer with source data for the RunContainer part of the or; if the same
     *        object as dst, the operation is done in place and the data in src is expected to
     *        shifted by srcOffset.
     * @param srcRuns The number of runs in src; as a separate argument since dst and src may be the
     *        same object, in which case nbrruns for src is also cero.
     * @param srcOffset An offset ot use while reading a particular position in src.
     * @param other A second RunContainer, to or against src.
     */
    private static void orImpl(
        final RunContainer dst,
        final RunContainer src,
        final int srcRuns,
        final int srcOffset,
        final RunContainer other) {
        final int otherRuns = other.nbrruns;
        int srcPosWithOffset = srcOffset;
        final int srcRunsWithOffset = srcRuns + srcOffset;
        int otherPos = 0;

        // Add values length (smaller first)
        while (srcPosWithOffset < srcRunsWithOffset && otherPos < otherRuns) {
            final short value = src.getValue(srcPosWithOffset);
            final short xvalue = other.getValue(otherPos);
            final short length = src.getLength(srcPosWithOffset);
            final short xlength = other.getLength(otherPos);

            if (ContainerUtil.compareUnsigned(value, xvalue) <= 0) {
                dst.smartAppend(value, length);
                ++srcPosWithOffset;
            } else {
                dst.smartAppend(xvalue, xlength);
                ++otherPos;
            }
        }

        if (srcPosWithOffset < srcRunsWithOffset) {
            if (src == dst && dst.nbrruns == srcPosWithOffset) {
                final int lastNext = dst.last() + 1;
                final int srcNextRunVal = src.getValueAsInt(srcPosWithOffset);
                if (lastNext < srcNextRunVal) {
                    // the values are already there.
                    dst.nbrruns += srcRunsWithOffset - srcPosWithOffset;
                    for (int run = srcPosWithOffset; run < srcRunsWithOffset; ++run) {
                        dst.cardinality += dst.getLengthAsInt(run) + 1;
                    }
                    return;
                }
            }
            do {
                dst.smartAppend(src.getValue(srcPosWithOffset), src.getLength(srcPosWithOffset));
            } while (++srcPosWithOffset < srcRunsWithOffset);
            return;
        }

        while (otherPos < otherRuns) {
            dst.smartAppend(other.getValue(otherPos), other.getLength(otherPos));
            ++otherPos;
        }
    }


    @Override
    public Container iremove(final int begin, final int end) {
        if (end == begin) {
            return this;
        }
        if (begin > end || end > MAX_RANGE) {
            throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
        }
        if (begin == end - 1) {
            return iunset((short) begin);
        }

        final RunContainer ans = deepcopyIfShared();
        return ans.iremoveImpl(begin, end);
    }

    private Container iremoveImpl(final int begin, final int end) {
        int bIndex = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, (short) begin);
        int eIndex =
            unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, (short) (end - 1));

        if (bIndex >= 0) { // beginning marks beginning of a run
            if (eIndex < 0) {
                eIndex = -eIndex - 2;
            }
            // eIndex could be a run that begins exactly at "end"
            // or it might be an earlier run

            // if the end is before the first run, we'd have eIndex==-1. But bIndex makes this
            // impossible.

            if (valueLengthContains(end, eIndex)) {
                chopValueLength(end, eIndex); // there is something left in the run
                recoverRoomsInRange(bIndex - 1, eIndex - 1);
            } else {
                recoverRoomsInRange(bIndex - 1, eIndex); // nothing left in the run
            }
        } else if (eIndex >= 0) {
            // start does not coincide to a run start, but end does.
            bIndex = -bIndex - 2;

            if (bIndex >= 0) {
                if (valueLengthContains(begin, bIndex)) {
                    closeValueLength(begin - 1, bIndex);
                }
            }

            // last run is one shorter
            if (getLength(eIndex) == 0) {// special case where we remove last run
                recoverRoomsInRange(eIndex - 1, eIndex);
            } else {
                incrementValue(eIndex);
                decrementLength(eIndex);
            }
            recoverRoomsInRange(bIndex, eIndex - 1);

        } else {
            bIndex = -bIndex - 2;
            eIndex = -eIndex - 2;

            if (eIndex >= 0) { // end-1 is not before first run.
                if (bIndex >= 0) { // nor is begin
                    if (bIndex == eIndex) { // all removal nested properly between
                        // one run start and the next
                        if (valueLengthContains(begin, bIndex)) {
                            if (valueLengthContains(end, eIndex)) {
                                // proper nesting within a run, generates 2 sub-runs
                                makeRoomAtIndex(bIndex);
                                cardinality += getLengthAsInt(bIndex) + 1;
                                closeValueLength(begin - 1, bIndex);
                                chopValueLength(end, bIndex + 1);
                                return this;
                            }
                            // removed area extends beyond run.
                            closeValueLength(begin - 1, bIndex);
                        }
                    } else { // begin in one run area, end in a later one.
                        if (valueLengthContains(begin, bIndex)) {
                            closeValueLength(begin - 1, bIndex);
                            // this cannot leave the bIndex run empty.
                        }
                        if (valueLengthContains(end, eIndex)) {
                            // there is additional stuff in the eIndex run
                            chopValueLength(end, eIndex);
                            eIndex--;
                        } // else { run ends at or before the range being removed, can delete it. }
                        recoverRoomsInRange(bIndex, eIndex);
                    }
                } else {
                    // removed range begins before the first run
                    if (valueLengthContains(end, eIndex)) { // had been end-1
                        chopValueLength(end, eIndex);
                        recoverRoomsInRange(bIndex, eIndex - 1);
                    } else { // removed range includes all the last run
                        recoverRoomsInRange(bIndex, eIndex);
                    }
                }

            } // else {eIndex == -1: whole range is before first run, nothing to delete... }
        }
        return this;
    }

    @Override
    public Container ixor(final ArrayContainer x) {
        if (shared) {
            return xor(x);
        }
        if (x.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return x.cowRef();
        }
        final int offset = x.getCardinality();
        copyToOffset(offset);
        final int myRuns = nbrruns;
        nbrruns = 0;
        cardinality = 0;
        xOrImpl(this, this, myRuns, offset, x);
        return toEfficientContainer();
    }

    @Override
    public Container ixor(final BitmapContainer x) {
        return xor(x);
    }

    @Override
    public Container ixor(final RunContainer x) {
        if (shared) {
            return xor(x);
        }
        if (x.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return x.cowRef();
        }
        final int offset = x.nbrruns;
        copyToOffset(offset);
        final int myRuns = nbrruns;
        nbrruns = 0;
        cardinality = 0;
        xOrImpl(this, this, myRuns, offset, x);
        return toEfficientContainer();
    }

    // x is not empty.
    private RunContainer andNotAsRun(final ArrayContainer x) {
        final RunContainer ans = new RunContainer(nbrruns + x.cardinality);
        int rlepos = 0;
        int xrlepos = 0;
        int start = getValueAsInt(rlepos);
        int end = start + getLengthAsInt(rlepos) + 1;
        int xstart = toIntUnsigned(x.content[xrlepos]);
        while ((rlepos < nbrruns) && (xrlepos < x.cardinality)) {
            if (end <= xstart) {
                // output the first run
                ans.valueslength[2 * ans.nbrruns] = (short) start;
                final int d = end - start;
                ans.valueslength[2 * ans.nbrruns + 1] = (short) (d - 1);
                ans.cardinality += d;
                ans.nbrruns++;
                rlepos++;
                if (rlepos < nbrruns) {
                    start = getValueAsInt(rlepos);
                    end = start + getLengthAsInt(rlepos) + 1;
                }
            } else if (xstart + 1 <= start) {
                // exit the second run
                xrlepos++;
                if (xrlepos < x.cardinality) {
                    xstart = toIntUnsigned(x.content[xrlepos]);
                }
            } else {
                if (start < xstart) {
                    ans.valueslength[2 * ans.nbrruns] = (short) start;
                    final int d = xstart - start;
                    ans.valueslength[2 * ans.nbrruns + 1] = (short) (d - 1);
                    ans.cardinality += d;
                    ans.nbrruns++;
                }
                if (xstart + 1 < end) {
                    start = xstart + 1;
                } else {
                    rlepos++;
                    if (rlepos < nbrruns) {
                        start = getValueAsInt(rlepos);
                        end = start + getLengthAsInt(rlepos) + 1;
                    }
                }
            }
        }
        if (rlepos < nbrruns) {
            ans.valueslength[2 * ans.nbrruns] = (short) start;
            final int d = end - start;
            ans.valueslength[2 * ans.nbrruns + 1] = (short) (d - 1);
            ans.cardinality += d;
            ++ans.nbrruns;
            rlepos++;
            if (rlepos < nbrruns) {
                final int nruns = nbrruns - rlepos;
                System.arraycopy(valueslength, 2 * rlepos, ans.valueslength, 2 * ans.nbrruns,
                    2 * nruns);
                for (int i = 0; i < nruns; ++i) {
                    ans.cardinality += ans.getLengthAsInt(ans.nbrruns + i) + 1;
                }
                ans.nbrruns += nruns;
            }
        }
        return ans;
    }

    /**
     * Calculate the result of an or between a RunContainer and an ArrayContainer storing the result
     * in a RunContainer.
     *
     * @param dst A RunContainer where to store the result. Its nbrruns field member should be zero
     *        on entry.
     * @param src A RunContainer with source data for the RunContainer part of the or, potentially
     *        shifted by srcOffset.
     * @param srcRuns The number of runs in the srcValueslength array.
     * @param srcOffset An offset ot use while reading a particular position in the srcValueslength
     *        array.
     * @param other An ArrayContainer to or against.
     */
    private static void orImpl(
        final RunContainer dst,
        final RunContainer src,
        final int srcRuns,
        final int srcOffset,
        final ArrayContainer other) {
        final int otherCardinality = other.getCardinality();
        int srcPosWithOffset = srcOffset;
        final int srcRunsWithOffset = srcRuns + srcOffset;
        int otherIdx = 0;

        if (otherIdx < otherCardinality && srcPosWithOffset < srcRunsWithOffset) {
            short otherValue = other.content[otherIdx];
            do {
                final short srcValue = src.getValue(srcPosWithOffset);
                if (ContainerUtil.compareUnsigned(srcValue, otherValue) <= 0) {
                    final short srcLen = src.getLength(srcPosWithOffset);
                    dst.smartAppend(srcValue, srcLen);
                    ++srcPosWithOffset;
                    continue;
                }
                dst.smartAppend(otherValue);
                ++otherIdx;
                if (otherIdx >= otherCardinality) {
                    break;
                }
                otherValue = other.content[otherIdx];
            } while (srcPosWithOffset < srcRunsWithOffset);
        }

        if (otherIdx < otherCardinality) {
            do {
                dst.smartAppend(other.content[otherIdx]);
            } while (++otherIdx < otherCardinality);
            return;
        }

        if (src == dst && dst.nbrruns == srcPosWithOffset) {
            final int nextRunValue = src.getValueAsInt(srcPosWithOffset);
            final int dstCurrentLastNext = dst.last() + 1;
            if (dstCurrentLastNext < nextRunValue) {
                // Any remaining values are already in place.
                dst.nbrruns += srcRunsWithOffset - srcPosWithOffset;
                for (int run = srcPosWithOffset; run < srcRunsWithOffset; ++run) {
                    dst.cardinality += dst.getLengthAsInt(run) + 1;
                }
                return;
            }
        }

        do {
            final short srcValue = src.getValue(srcPosWithOffset);
            final short srcLen = src.getLength(srcPosWithOffset);
            dst.smartAppend(srcValue, srcLen);
        } while (++srcPosWithOffset < srcRunsWithOffset);
    }

    private void makeRoomAtIndex(final int index) {
        if (2 * (nbrruns + 1) > valueslength.length) {
            increaseCapacity();
        }
        copyValuesLength(valueslength, index, valueslength, index + 1, nbrruns - index);
        ++nbrruns;
    }

    // To merge values length from begin(inclusive) to end(inclusive)
    private void mergeValuesLength(final int begin, final int end) {
        if (begin >= end) {
            return;
        }
        final int bValue = getValueAsInt(begin);
        final int bLength = getLengthAsInt(begin);
        final int eValue = getValueAsInt(end);
        final int eLength = getLengthAsInt(end);
        final int newLength = eValue - bValue + eLength;
        setLength(begin, (short) newLength);
        recoverRoomsInRange(begin, end);
        cardinality += newLength - bLength;
    }

    @Override
    public Container not(final int rangeStart, final int rangeEnd) {
        if (rangeEnd <= rangeStart) {
            return cowRef();
        }
        RunContainer ans = new RunContainer(nbrruns + 1);
        int k = 0;
        for (; (k < nbrruns) && (getValueAsInt(k) < rangeStart); ++k) {
            ans.valueslength[2 * k] = valueslength[2 * k];
            final short len = valueslength[2 * k + 1];
            ans.valueslength[2 * k + 1] = len;
            ++ans.nbrruns;
            ans.cardinality += len + 1;
        }
        ans.smartAppendForXor((short) rangeStart, (short) (rangeEnd - rangeStart - 1));
        for (; k < nbrruns; ++k) {
            ans.smartAppendForXor(getValue(k), getLength(k));
        }
        // the number of runs can increase by one, meaning (rarely) a bitmap will become better
        // or the cardinality can decrease by a lot, making an array better
        return ans.toEfficientContainer();
    }

    @Override
    public int numberOfRuns() {
        return nbrruns;
    }

    @Override
    public Container or(final ArrayContainer x) {
        if (isAllOnes() || x.isEmpty()) {
            return cowRef();
        }
        if (isEmpty()) {
            return x.cowRef();
        }
        final RunContainer answer = new RunContainer(nbrruns + x.getCardinality());
        orImpl(answer, this, nbrruns, 0, x);
        return answer.toEfficientContainer();
    }

    @Override
    public Container or(final BitmapContainer x) {
        if (isAllOnes() || x.isEmpty()) {
            return cowRef();
        }
        if (x.isAllOnes()) {
            return Container.full();
        }
        if (isEmpty()) {
            return x.cowRef();
        }
        // could be implemented as return toTemporaryBitmap().ior(x);
        final BitmapContainer answer = x.deepCopy();
        for (int rlepos = 0; rlepos < nbrruns; ++rlepos) {
            int start = getValueAsInt(rlepos);
            int end = start + getLengthAsInt(rlepos) + 1;
            int prevOnesInRange = answer.cardinalityInRange(start, end);
            ContainerUtil.setBitmapRange(answer.bitmap, start, end);
            answer.updateCardinality(prevOnesInRange, end - start);
        }
        if (answer.isAllOnes()) {
            return Container.full();
        }
        return answer;
    }

    @Override
    public Container or(final RunContainer x) {
        if (isAllOnes() || x.isEmpty()) {
            return cowRef();
        }
        if (isEmpty() || x.isAllOnes()) {
            return x.cowRef(); // cheap case that can save a lot of computation
        }
        final RunContainer answer = new RunContainer(nbrruns + x.nbrruns);
        orImpl(answer, this, nbrruns, 0, x);
        return answer.toEfficientContainer();
    }

    // Prepend a value length with all values starting from a given value
    private void prependValueLength(final int value, final int index) {
        int initialValue = getValueAsInt(index);
        int length = getLengthAsInt(index);
        setValue(index, (short) value);
        final int d = initialValue - value;
        setLength(index, (short) (d + length));
        cardinality += d;
    }

    @Override
    public int rank(final short lowbits) {
        int x = toIntUnsigned(lowbits);
        int answer = 0;
        for (int k = 0; k < nbrruns; ++k) {
            int value = getValueAsInt(k);
            int length = getLengthAsInt(k);
            if (x < value) {
                return answer;
            } else if (value + length + 1 > x) {
                return answer + x - value + 1;
            }
            answer += length + 1;
        }
        return answer;
    }

    private void recoverRoomAtIndex(final int index) {
        cardinality -= getLengthAsInt(index) + 1;
        copyValuesLength(valueslength, index + 1, valueslength, index, nbrruns - index - 1);
        --nbrruns;
    }

    // To recover rooms between begin(exclusive) and end(inclusive)
    private void recoverRoomsInRange(final int begin, final int end) {
        for (int run = begin + 1; run <= end; ++run) {
            cardinality -= getLengthAsInt(run) + 1;
        }
        if (end + 1 < nbrruns) {
            copyValuesLength(valueslength, end + 1, valueslength, begin + 1,
                nbrruns - 1 - end);
        }
        nbrruns -= end - begin;
    }

    @Override
    public Container remove(final int begin, final int end) {
        final RunContainer rc = deepCopyIfNotShared();
        return rc.iremove(begin, end);
    }

    @Override
    public Container unset(final short x) {
        final int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, x);
        return unsetImpl(x, index, false, null);
    }

    @Override
    public Container iunset(final short x) {
        final int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, x);
        return iunset(x, index);
    }

    private Container iunset(final short x, final int index) {
        return unsetImpl(x, index, true, null);
    }

    @Override
    Container iunset(final short x, final PositionHint positionHint) {
        final int index = unsignedInterleavedBinarySearch(valueslength,
            Math.max(positionHint.value, 0), nbrruns, x);
        return unsetImpl(x, index, true, positionHint);
    }

    @Override
    Container unset(final short x, final PositionHint positionHint) {
        final int index = unsignedInterleavedBinarySearch(valueslength,
            Math.max(positionHint.value, 0), nbrruns, x);
        return unsetImpl(x, index, false, positionHint);
    }

    private Container unsetImpl(final short x, int index, final boolean inPlace,
        final PositionHint positionHintOut) {
        if (index >= 0) {
            final RunContainer ans = inPlace ? deepcopyIfShared() : deepCopy();
            if (ans.getLength(index) == 0) {
                ans.recoverRoomAtIndex(index);
            } else {
                ans.incrementValue(index);
                ans.decrementLength(index);
            }
            setIfNotNull(positionHintOut, index);
            return ans;// already there
        }
        index = -index - 2;// points to preceding value, possibly -1
        if (index < 0) {
            // no match.
            setIfNotNull(positionHintOut, 0);
            return inPlace ? this : cowRef();
        }

        final int offset = toIntUnsigned(x) - getValueAsInt(index);
        final int le = getLengthAsInt(index);
        if (offset < le) {
            // need to break in two
            final RunContainer ans = inPlace ? deepcopyIfShared() : deepCopy();
            ans.setLength(index, (short) (offset - 1));
            ans.cardinality += offset - 1 - le;
            // need to insert
            final int newValue = toIntUnsigned(x) + 1;
            final int newLength = le - offset - 1;
            ans.makeRoomAtIndex(index + 1);
            ans.setValue(index + 1, (short) newValue);
            ans.setLength(index + 1, (short) newLength);
            ans.cardinality += newLength + 1;
            setIfNotNull(positionHintOut, index + 1);
            return ans;
        }
        if (offset != le) {
            setIfNotNull(positionHintOut, index);
            return inPlace ? this : cowRef();
        }
        final RunContainer ans = inPlace ? deepcopyIfShared() : deepCopy();
        ans.decrementLength(index);
        setIfNotNull(positionHintOut, index);
        return ans;
    }

    /**
     * Convert to Array or Bitmap container if the serialized form would be shorter. Exactly the
     * same functionality as toEfficientContainer.
     */

    @Override
    public Container runOptimize() {
        return toEfficientContainer();
    }

    @Override
    public short select(final int j) {
        int offset = 0;
        for (int k = 0; k < nbrruns; ++k) {
            int nextOffset = offset + getLengthAsInt(k) + 1;
            if (nextOffset > j) {
                return (short) (getValue(k) + (j - offset));
            }
            offset = nextOffset;
        }
        throw new IllegalArgumentException(
            "Cannot select " + j + " since cardinality is " + getCardinality());
    }

    @Override
    public RunContainer select(final int startRank, final int endRank) {
        return select(this, startRank, endRank);
    }

    @Override
    public int find(final short x) {
        int pos = 0;
        int target = toIntUnsigned(x);
        for (int k = 0; k < nbrruns; ++k) {
            int value = getValueAsInt(k);
            if (target < value) {
                break;
            }
            int length = getLengthAsInt(k);
            int end = value + length;
            if (target <= end) {
                return pos + target - value;
            }
            pos += length + 1;
        }
        return -pos - 1;
    }

    @Override
    public void selectRanges(final RangeConsumer outValues, final RangeIterator inPositions) {
        if (nbrruns == 0) {
            return;
        }
        int k = 0;
        int kStart = getValueAsInt(0);
        int kLen = getLengthAsInt(0);
        int kEndPos = kLen;
        int ostart = -1;
        int oend = -1; // inclusive
        while (inPositions.hasNext()) {
            inPositions.next();
            int istart = inPositions.start();
            int iend = inPositions.end();
            for (int pos = istart; pos < iend; ++pos) {
                while (kEndPos < pos) {
                    ++k;
                    if (k >= nbrruns) {
                        throw new IllegalArgumentException("selectRanges for invalid pos=" + pos);
                    }
                    kStart = getValueAsInt(k);
                    kLen = getLengthAsInt(k);
                    kEndPos += 1 + kLen;
                }
                int key = kStart + kLen - (kEndPos - pos);
                if (ostart == -1) {
                    ostart = oend = key;
                } else {
                    if (key == oend + 1) {
                        oend = key;
                    } else {
                        outValues.accept(ostart, oend + 1);
                        ostart = oend = key;
                    }
                }
            }
        }
        outValues.accept(ostart, oend + 1);
    }

    @Override
    public boolean findRanges(final RangeConsumer outPositions, final RangeIterator inValues,
        final int maxPos) {
        if (!inValues.hasNext()) {
            return false;
        }
        int k = 0;
        int offset = 0;
        int kStart = getValueAsInt(0);
        int kLen = getLengthAsInt(0);
        int kEnd = kStart + kLen;
        int ostart = -1;
        int oend = -1; // inclusive
        do {
            inValues.next();
            int istart = inValues.start();
            int iend = inValues.end();
            for (int key = istart; key < iend; ++key) {
                while (kEnd < key) {
                    ++k;
                    if (k >= nbrruns) {
                        throw new IllegalArgumentException("findRanges for invalid key=" + key);
                    }
                    offset += kLen + 1;
                    kStart = getValueAsInt(k);
                    kLen = getLengthAsInt(k);
                    kEnd = kStart + kLen;
                }
                if (key < kStart) {
                    throw new IllegalArgumentException("findRanges for invalid key=" + key);
                }
                int pos = offset + (key - kStart);
                if (ostart == -1) {
                    if (pos > maxPos) {
                        return true;
                    }
                    ostart = oend = pos;
                } else {
                    if (pos > maxPos) {
                        outPositions.accept(ostart, oend + 1);
                        return true;
                    }
                    if (pos == oend + 1) {
                        oend = pos;
                    } else {
                        outPositions.accept(ostart, oend + 1);
                        ostart = oend = pos;
                    }
                }
            }
        } while (inValues.hasNext());
        outPositions.accept(ostart, oend + 1);
        return false;
    }

    private void setLength(final int index, final short v) {
        setLength(valueslength, index, v);
    }

    private void setLength(final short[] valueslength, final int index, final short v) {
        valueslength[2 * index + 1] = v;
    }

    private void setValue(final int index, final short v) {
        setValue(valueslength, index, v);
    }

    private void setValue(final short[] valueslength, final int index, final short v) {
        valueslength[2 * index] = v;
    }


    // bootstrapping (aka "galloping") binary search. Always skips at least one.
    // On our "real data" benchmarks, enabling galloping is a minor loss
    // .."ifdef ENABLE_GALLOPING_AND" :)
    private int skipAhead(final RunContainer skippingOn, final int pos, final int targetToExceed) {
        int left = pos;
        int span = 1;
        int probePos;
        int end;
        // jump ahead to find a spot where end > targetToExceed (if it exists)
        do {
            probePos = left + span;
            if (probePos >= skippingOn.nbrruns - 1) {
                // expect it might be quite common to find the container cannot be advanced as far
                // as
                // requested. Optimize for it.
                probePos = skippingOn.nbrruns - 1;
                end = toIntUnsigned(skippingOn.getValue(probePos))
                    + toIntUnsigned(skippingOn.getLength(probePos)) + 1;
                if (end <= targetToExceed) {
                    return skippingOn.nbrruns;
                }
            }
            end = toIntUnsigned(skippingOn.getValue(probePos))
                + toIntUnsigned(skippingOn.getLength(probePos)) + 1;
            span *= 2;
        } while (end <= targetToExceed);
        int right = probePos;
        // left and right are both valid positions. Invariant: left <= targetToExceed && right >
        // targetToExceed
        // do a binary search to discover the spot where left and right are separated by 1, and
        // invariant is maintained.
        while (right - left > 1) {
            int mid = (right + left) / 2;
            int midVal = toIntUnsigned(skippingOn.getValue(mid))
                + toIntUnsigned(skippingOn.getLength(mid)) + 1;
            if (midVal > targetToExceed) {
                right = mid;
            } else {
                left = mid;
            }
        }
        return right;
    }

    private void simpleAppend(final short start, final short length) {
        valueslength[2 * nbrruns] = start;
        valueslength[2 * nbrruns + 1] = length;
        cardinality += toIntUnsigned(length) + 1;
        ++nbrruns;
    }

    /**
     * Append a value to a RunContainer. Callers must guarantee there is no need to grow the array
     * for appending.
     *
     * @param val a value to append.
     */
    private void smartAppend(final short val) {
        smartAppend(val, (short) 0);
    }

    /**
     * Append a run to a RunContainer. Callers must guarantee there is no need to grow the array for
     * appending.
     *
     * @param start the value for the start of the run.
     * @param length the length of the run.
     */
    void smartAppend(final short start, final short length) {
        if (nbrruns == 0) {
            simpleAppend(start, length);
            return;
        }
        final int oldStart = getValueAsInt(nbrruns - 1);
        final int oldLen = getLengthAsInt(nbrruns - 1);
        final int oldEnd = oldStart + oldLen + 1;
        if (toIntUnsigned(start) > oldEnd) {
            simpleAppend(start, length);
            return;
        }
        int newEnd = toIntUnsigned(start) + toIntUnsigned(length) + 1;
        if (newEnd > oldEnd) { // we merge
            setLength(nbrruns - 1, (short) (newEnd - 1 - oldStart));
            cardinality += newEnd - oldEnd;
        }
    }

    /**
     * Append a value to a RunContainer, or remove it if already there (as in an XOR operation).
     * Callers must guarantee there is no need to grow the array for appending.
     *
     * @param val a value to append.
     */
    private void smartAppendForXor(final short val) {
        smartAppendForXor(val, (short) 0);
    }

    /**
     * Append a run to a RunContainer, or remove from it if some values already there (as in an XOR
     * operation). Callers must guarantee there is no need to grow the array for appending.
     *
     * @param start the value for the start of the run.
     * @param length the length of the run.
     */
    private void smartAppendForXor(final short start, final short length) {
        if (nbrruns == 0) {
            simpleAppend(start, length);
            return;
        }
        final int oldStart = getValueAsInt(nbrruns - 1);
        final int oldLen = getLengthAsInt(nbrruns - 1);
        final int oldEnd = oldStart + oldLen + 1;
        final int newStart = toIntUnsigned(start);
        if (newStart > oldEnd) {
            simpleAppend(start, length);
            return;
        }
        if (newStart == oldEnd) {
            // we merge
            valueslength[2 * (nbrruns - 1) + 1] += length + 1;
            cardinality += toIntUnsigned((short) (length + 1));
            return;
        }
        // newStart < oldEnd.
        final int newEnd = newStart + toIntUnsigned(length) + 1;
        if (newStart == oldStart) {
            // we wipe out previous
            if (newEnd < oldEnd) {
                setValue(nbrruns - 1, (short) newEnd);
                setLength(nbrruns - 1, (short) (oldEnd - newEnd - 1));
                cardinality -= newEnd - oldStart;
                return;
            }
            if (newEnd > oldEnd) {
                setValue(nbrruns - 1, (short) oldEnd);
                setLength(nbrruns - 1, (short) (newEnd - oldEnd - 1));
                cardinality -= oldEnd - oldStart;
                cardinality += newEnd - oldEnd;
                return;
            }
            // they cancel out
            --nbrruns;
            cardinality -= oldEnd - oldStart;
            return;
        }
        final short newLen = (short) (start - oldStart - 1);
        setLength(nbrruns - 1, newLen);
        cardinality += toIntUnsigned(newLen) - oldLen;
        if (newEnd < oldEnd) {
            setValue(nbrruns, (short) newEnd);
            final int d = oldEnd - newEnd;
            setLength(nbrruns, (short) (d - 1));
            cardinality += d;
            ++nbrruns;
            return;
        }
        if (newEnd > oldEnd) {
            setValue(nbrruns, (short) oldEnd);
            final int d = newEnd - oldEnd;
            setLength(nbrruns, (short) (d - 1));
            cardinality += d;
            ++nbrruns;
        }
    }

    // convert to bitmap *if needed* (useful if you know it can't be an array)
    private Container toBitmapIfNeeded() {
        int sizeAsRunContainer = RunContainer.sizeInBytes(nbrruns);
        int sizeAsBitmapContainer = BitmapContainer.BITMAP_SIZE_IN_BYTES;
        if (sizeAsBitmapContainer > sizeAsRunContainer) {
            return this;
        }
        return toBitmapContainer();
    }



    /**
     * Convert the container to either a Bitmap or an Array Container, depending on the cardinality.
     *
     * @param card the current cardinality
     * @return new container
     */
    Container toBitmapOrArrayContainer(final int card) {
        // int card = getCardinality();
        if (card <= ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            ArrayContainer answer = new ArrayContainer(card);
            answer.cardinality = 0;
            for (int rlepos = 0; rlepos < nbrruns; ++rlepos) {
                int runStart = getValueAsInt(rlepos);
                int runEnd = runStart + getLengthAsInt(rlepos);

                for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                    answer.content[answer.cardinality++] = (short) runValue;
                }
            }
            return answer;
        }
        BitmapContainer answer = new BitmapContainer();
        for (int rlepos = 0; rlepos < nbrruns; ++rlepos) {
            int start = getValueAsInt(rlepos);
            int end = start + getLengthAsInt(rlepos) + 1;
            ContainerUtil.setBitmapRange(answer.bitmap, start, end);
        }
        answer.cardinality = card;
        return answer;
    }

    // convert to bitmap or array *if needed*
    Container toEfficientContainer() {
        if (nbrruns == 0) {
            return Container.empty();
        }
        if (nbrruns == 1) {
            final int start = ContainerUtil.toIntUnsigned(valueslength[0]);
            final int len = ContainerUtil.toIntUnsigned(valueslength[1]);
            if (len == 0) {
                return makeSingletonContainer(lowbits(start));
            }
            final int end = start + len + 1;
            return makeSingleRangeContainer(start, end);
        }
        if (nbrruns == 2 && 0 == valueslength[1] && 0 == valueslength[3]) {
            return makeTwoValuesContainer(valueslength[0], valueslength[2]);
        }
        final int sizeAsRunContainer = RunContainer.sizeInBytes(nbrruns);
        final int sizeAsBitmapContainer = BitmapContainer.BITMAP_SIZE_IN_BYTES;
        final int card = getCardinality();
        final int sizeAsArrayContainer = ArrayContainer.sizeInBytes(card);
        if (sizeAsRunContainer <= Math.min(sizeAsBitmapContainer, sizeAsArrayContainer)) {
            compact();
            return this;
        }
        return toBitmapOrArrayContainer(card);
    }

    private void compact() {
        final int used = runsShortArraySizeRounding(nbrruns);
        if (shared || 2 * valueslength.length <= used) {
            return;
        }
        final short[] newValueslength = new short[used];
        System.arraycopy(valueslength, 0, newValueslength, 0, 2 * nbrruns);
        valueslength = newValueslength;
    }

    @Override
    public void trim() {
        compact();
    }


    // To check if a value length contains a given value
    private boolean valueLengthContains(final int value, final int index) {
        int initialValue = getValueAsInt(index);
        int length = getLengthAsInt(index);

        return value <= initialValue + length;
    }

    // other.getCardinality() > 0.
    private static void xOrImpl(
        final RunContainer dst,
        final RunContainer src,
        final int srcRuns,
        final int srcOffset,
        final ArrayContainer other) {
        int srcPosWithOffset = srcOffset;
        final int srcRunsWithOffset = srcRuns + srcOffset;
        int otherIdx = 0;
        short otherValue = other.content[otherIdx];

        short srcValue = src.getValue(srcPosWithOffset);
        while (true) {
            if (ContainerUtil.compareUnsigned(srcValue, otherValue) < 0) {
                dst.smartAppendForXor(srcValue, src.getLength(srcPosWithOffset));
                ++srcPosWithOffset;
                if (srcPosWithOffset == srcRunsWithOffset) {
                    dst.smartAppendForXor(otherValue);
                    while (++otherIdx < other.cardinality) {
                        dst.smartAppendForXor(other.content[otherIdx]);
                    }
                    return;
                }
                srcValue = src.getValue(srcPosWithOffset);
                continue;
            }
            dst.smartAppendForXor(otherValue);
            if (++otherIdx >= other.cardinality) {
                if (srcPosWithOffset < srcRunsWithOffset) {
                    if (src == dst && dst.nbrruns == srcPosWithOffset) {
                        final int lastNext = dst.last() + 1;
                        final int srcNextRunVal = src.getValueAsInt(srcPosWithOffset);
                        if (lastNext < srcNextRunVal) {
                            dst.nbrruns += srcRunsWithOffset - srcPosWithOffset;
                            for (int run = srcPosWithOffset; run < srcRunsWithOffset; ++run) {
                                dst.cardinality += dst.getLengthAsInt(run) + 1;
                            }
                            return;
                        }
                    }
                    do {
                        dst.smartAppendForXor(src.getValue(srcPosWithOffset),
                            src.getLength(srcPosWithOffset));
                    } while (++srcPosWithOffset < srcRunsWithOffset);
                }
                return;
            }
            otherValue = other.content[otherIdx];
        }
    }

    @Override
    public Container xor(final ArrayContainer x) {
        if (x.isEmpty()) {
            return cowRef();
        }
        if (isEmpty()) {
            return x.cowRef();
        }
        // if the cardinality of the array is small, guess that the output will still be a run
        // container
        final int arbitrary_threshold = 32; // 32 is arbitrary here
        final int xCard = x.getCardinality();
        if (xCard < arbitrary_threshold) {
            if (xCard == 0) {
                return cowRef();
            }
            final RunContainer dst = new RunContainer(nbrruns + x.getCardinality());
            xOrImpl(dst, this, nbrruns, 0, x);
            return dst.toEfficientContainer();
        }
        // otherwise, we expect the output to be either an array or bitmap
        final int card = getCardinality();
        if (card <= ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            // if the cardinality is small, we construct the solution in place
            return x.xor(getShortIterator());
        }
        // otherwise, we generate a bitmap (even if runcontainer would be better)
        return toBitmapOrArrayContainer(card).ixor(x);
    }

    @Override
    public Container xor(final BitmapContainer x) {
        if (x.isEmpty()) {
            return cowRef();
        }
        if (isEmpty()) {
            return x.cowRef();
        }
        // could be implemented as return toTemporaryBitmap().ixor(x);
        final BitmapContainer answer = x.deepCopy();
        for (int rlepos = 0; rlepos < nbrruns; ++rlepos) {
            int start = getValueAsInt(rlepos);
            int end = start + getLengthAsInt(rlepos) + 1;
            int prevOnes = answer.cardinalityInRange(start, end);
            ContainerUtil.flipBitmapRange(answer.bitmap, start, end);
            answer.updateCardinality(prevOnes, end - start - prevOnes);
        }
        return answer.maybeSwitchContainer();
    }

    private static void xOrImpl(
        final RunContainer dst,
        final RunContainer src,
        final int srcRuns,
        final int srcOffset,
        final RunContainer other) {
        int srcPosWithOffset = srcOffset;
        final int srcRunsWithOffset = srcRuns + srcOffset;
        int otherPos = 0;

        short srcValue = src.getValue(srcPosWithOffset);
        short otherValue = other.getValue(otherPos);
        while (true) {
            if (ContainerUtil.compareUnsigned(srcValue, otherValue) < 0) {
                dst.smartAppendForXor(srcValue, src.getLength(srcPosWithOffset));
                ++srcPosWithOffset;

                if (srcPosWithOffset == srcRunsWithOffset) {
                    while (otherPos < other.nbrruns) {
                        dst.smartAppendForXor(other.getValue(otherPos), other.getLength(otherPos));
                        ++otherPos;
                    }
                    return;
                }
                srcValue = src.getValue(srcPosWithOffset);
                continue;
            }
            dst.smartAppendForXor(other.getValue(otherPos), other.getLength(otherPos));
            ++otherPos;
            if (otherPos == other.nbrruns) {
                if (srcPosWithOffset < srcRunsWithOffset) {
                    if (src == dst && dst.nbrruns == srcPosWithOffset) {
                        final int lastNext = dst.last() + 1;
                        final int srcNextRunVal = src.getValueAsInt(srcPosWithOffset);
                        if (lastNext < srcNextRunVal) {
                            dst.nbrruns += srcRunsWithOffset - srcPosWithOffset;
                            for (int run = srcPosWithOffset; run < srcRunsWithOffset; ++run) {
                                dst.cardinality += dst.getLengthAsInt(run) + 1;
                            }
                            return;
                        }
                    }
                    do {
                        dst.smartAppendForXor(src.getValue(srcPosWithOffset),
                            src.getLength(srcPosWithOffset));
                    } while (++srcPosWithOffset < srcRunsWithOffset);
                }
                return;
            }
            otherValue = other.getValue(otherPos);
        }
    }

    @Override
    public Container xor(final RunContainer x) {
        if (x.isEmpty()) {
            return cowRef();
        }
        if (isEmpty()) {
            return x.cowRef();
        }
        final RunContainer answer = new RunContainer(nbrruns + x.nbrruns);
        xOrImpl(answer, this, nbrruns, 0, x);
        return answer.toEfficientContainer();
    }

    @Override
    public boolean forEach(final ShortConsumer sc) {
        for (int k = 0; k < nbrruns; ++k) {
            int val = getValueAsInt(k);
            int len = getLengthAsInt(k);
            for (int v = val; v <= val + len; ++v) {
                final boolean wantMore = sc.accept((short) v);
                if (!wantMore) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean forEach(final int rankOffset, final ShortConsumer sc) {
        int rank = 0;
        int k = 0;
        while (rank < rankOffset) {
            final int val = getValueAsInt(k);
            final int len = getLengthAsInt(k);
            ++k;
            final int nextRank = rank + len + 1;
            if (nextRank > rankOffset) {
                final int n = nextRank - rankOffset;
                final int last = val + len;
                for (int v = last - n + 1; v <= last; ++v) {
                    final boolean wantMore = sc.accept((short) v);
                    if (!wantMore) {
                        return false;
                    }
                }
                break;
            }
            rank = nextRank;
        }
        for (; k < nbrruns; ++k) {
            final int val = getValueAsInt(k);
            final int len = getLengthAsInt(k);
            for (int v = val; v <= val + len; ++v) {
                final boolean wantMore = sc.accept((short) v);
                if (!wantMore) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean forEachRange(final int rankOffset, final ShortRangeConsumer sc) {
        int rank = 0;
        int k = 0;
        while (rank < rankOffset) {
            final int val = getValueAsInt(k);
            final int len = getLengthAsInt(k);
            ++k;
            final int nextRank = rank + len + 1;
            if (nextRank > rankOffset) {
                final int n = nextRank - rankOffset;
                final int last = val + len;
                final int start = last - n + 1;
                final boolean wantMore = sc.accept((short) start, (short) last);
                if (!wantMore) {
                    return false;
                }
                break;
            }
            rank = nextRank;
        }
        for (; k < nbrruns; ++k) {
            final int val = getValueAsInt(k);
            final int len = getLengthAsInt(k);
            final boolean wantMore = sc.accept((short) val, (short) (val + len));
            if (!wantMore) {
                return false;
            }
        }
        return true;
    }

    @Override
    public BitmapContainer toBitmapContainer() {
        int card = 0;
        BitmapContainer answer = new BitmapContainer();
        for (int rlepos = 0; rlepos < nbrruns; ++rlepos) {
            int start = getValueAsInt(rlepos);
            int len = getLengthAsInt(rlepos) + 1;
            int end = start + len;
            ContainerUtil.setBitmapRange(answer.bitmap, start, end);
            card += len;
        }
        answer.cardinality = card;
        return answer;
    }

    @Override
    public int nextValue(final short fromValue) {
        int index = unsignedInterleavedBinarySearch(valueslength, 0, nbrruns, fromValue);
        int effectiveIndex = index >= 0 ? index : -index - 2;
        if (effectiveIndex == -1) {
            return first();
        }
        int startValue = getValueAsInt(effectiveIndex);
        int value = toIntUnsigned(fromValue);
        int offset = value - startValue;
        int le = getLengthAsInt(effectiveIndex);
        if (offset <= le) {
            return value;
        }
        if (effectiveIndex + 1 < numberOfRuns()) {
            return getValueAsInt(effectiveIndex + 1);
        }
        return -1;
    }

    private void assertNonEmpty() {
        if (nbrruns == 0) {
            throw new NoSuchElementException("Empty " + getContainerName());
        }
    }

    @Override
    public int first() {
        assertNonEmpty();
        return toIntUnsigned(valueslength[0]);
    }

    @Override
    public int last() {
        assertNonEmpty();
        int index = numberOfRuns() - 1;
        int start = getValueAsInt(index);
        int length = getLengthAsInt(index);
        return start + length;
    }

    @Override
    public boolean subsetOf(final ArrayContainer c) {
        if (isEmpty()) {
            return true;
        }
        if (c.isEmpty()) {
            return false;
        }
        // don't do a cardinality based check since cardinality is not cached in run.
        if (first() < c.first() || last() > c.last()) {
            return false;
        }
        int ci = 0;
        for (int i = 0; i < nbrruns; ++i) {
            int ival = getValueAsInt(i);
            int ilen = getLengthAsInt(i);
            for (int j = ival; j <= ival + ilen; ++j) {
                if (ci >= c.getCardinality()) {
                    return false;
                }

                int s =
                    ContainerUtil.unsignedBinarySearch(c.content, ci, c.cardinality, lowbits(j));
                if (s < 0) {
                    return false;
                }
                ci = s + 1;
            }
        }
        return true;
    }

    @Override
    public boolean subsetOf(final BitmapContainer c) {
        if (isEmpty()) {
            return true;
        }
        if (c.isEmpty()) {
            return false;
        }
        // don't do a cardinality based check since cardinality is not cached in run.
        if (first() < c.first() || last() > c.last()) {
            return false;
        }
        for (int i = 0; i < nbrruns; ++i) {
            int ival = getValueAsInt(i);
            int ilen = getLengthAsInt(i);
            for (int j = ival; j <= ival + ilen; ++j) {
                if (!c.contains(lowbits(j))) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean subsetOf(final RunContainer c) {
        if (isEmpty()) {
            return true;
        }
        if (c.isEmpty()) {
            return false;
        }
        // don't do a cardinality based check since cardinality is not cached in run.
        if (first() < c.first() || last() > c.last()) {
            return false;
        }
        int ci = 0;
        for (int i = 0; i < nbrruns; ++i) {
            int ival = getValueAsInt(i);
            int ilen = getLengthAsInt(i);
            for (int j = ival; j <= ival + ilen; ++j) {
                if (ci >= c.nbrruns) {
                    return false;
                }

                int s = c.searchFrom(lowbits(j), ci);
                if (s < 0) {
                    return false;
                }
                ci = s;
            }
        }
        return true;
    }

    // rangeEnd is exclusive.
    @Override
    public boolean overlapsRange(final int rangeStart, final int rangeEnd) {
        if (isEmpty() || rangeEnd <= first() || rangeStart > last()) {
            return false;
        }
        int i = searchFrom(lowbits(rangeStart), 0);
        if (i >= 0) {
            return true;
        }
        i = ~i;
        // (i < numberOfRuns()), otherwise we bailed out on the rangeStart > last() check above.
        final int last = rangeEnd - 1;
        int j = searchFrom(lowbits(last), i);
        if (j >= 0) {
            return true;
        }
        j = ~j;
        // if i and j "sandwich" some range, we have an overlap.
        return i != j;
    }

    @Override
    public boolean overlaps(final ArrayContainer c) {
        return (getCardinality() < c.getCardinality()) ? ContainerUtil.overlaps(this, c)
            : ContainerUtil.overlaps(c, this);
    }

    @Override
    public boolean overlaps(final BitmapContainer c) {
        // BitmapContainers should not normally have more elements than a run container.
        for (int i = 0; i < nbrruns; ++i) {
            int ival = getValueAsInt(i);
            int ilen = getLengthAsInt(i);
            for (int j = ival; j <= ival + ilen; ++j) {
                if (c.contains(lowbits(j))) {
                    return true;
                }
            }
        }
        return false;
    }

    static boolean overlaps(final RunContainer c1, final RunContainer c2) {
        int c2i = 0;
        for (int c1i = 0; c1i < c1.nbrruns; ++c1i) {
            if (c2i >= c2.nbrruns) {
                return false;
            }
            int value = ContainerUtil.toIntUnsigned(c1.getValue(c1i));
            int len = ContainerUtil.toIntUnsigned(c1.getLength(c1i));
            for (int j = value; j <= value + len; ++j) {
                int s = c2.searchFrom(lowbits(j), c2i);
                if (s >= 0) {
                    return true;
                }
                c2i = -(s + 1);
            }
        }
        return false;
    }

    @Override
    public boolean overlaps(final RunContainer c) {
        return (getCardinality() < c.getCardinality()) ? overlaps(this, c) : overlaps(c, this);
    }

    @Override
    public void setCopyOnWrite() {
        shared = true;
    }

    private RunContainer deepcopyIfShared() {
        return shared ? deepCopy() : this;
    }

    private RunContainer deepCopyIfNotShared() {
        return shared ? this : deepCopy();
    }

    @Override
    public int bytesAllocated() {
        return Short.BYTES * valueslength.length;
    }

    @Override
    public int bytesUsed() {
        return 2 * Short.BYTES * nbrruns;
    }

    @Override
    public Container toLargeContainer() {
        return this;
    }

    @Override
    public void validate() {
        int prev = -2;
        int computedCard = 0;
        for (int i = 0; i < nbrruns; ++i) {
            final int val = getValueAsInt(i);
            final int len = getLengthAsInt(i);
            computedCard += len + 1;
            if (val - 1 <= prev || val + len > MAX_VALUE) {
                throw new IllegalStateException(
                    "i=" + i + ", prev=" + prev + ", val=" + val + ", len=" + len);
            }
            prev = val + len;
        }
        final int readCard = getCardinality();
        if (computedCard != readCard) {
            throw new IllegalStateException(
                "computedCard=" + computedCard + ", readCard=" + readCard);
        }
    }

    @Override
    public boolean isShared() {
        return shared;
    }
}
