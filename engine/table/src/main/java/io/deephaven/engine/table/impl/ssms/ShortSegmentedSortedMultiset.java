/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSegmentedSortedMultiset and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.ssms;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.vector.ShortVector;
import io.deephaven.vector.ShortVectorDirect;
import io.deephaven.vector.ObjectVector;
import io.deephaven.util.compare.ShortComparisons;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.engine.table.impl.by.SumIntChunk;
import io.deephaven.engine.table.impl.sort.timsort.TimsortUtils;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.annotations.VisibleForTesting;
import gnu.trove.set.hash.TShortHashSet;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Arrays;
import java.util.Objects;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

public final class ShortSegmentedSortedMultiset implements SegmentedSortedMultiSet<Short>, ShortVector {
    private final int leafSize;
    private int leafCount;
    private int size;
    private long totalSize;

    private int minGallop = TimsortUtils.INITIAL_GALLOP;

    /**
     * If we have only a single leaf, then we use the directory arrays for the leaf values, otherwise we use it to
     * track the largest value in a given leaf.  The values are valid for 0 ... leafCount - 2, because the last leaf
     * must accept any value that is greater than the second to last leave's maximum.
     */
    private short [] directoryValues;
    private long [] directoryCount;

    private int [] leafSizes;
    private short [][] leafValues;
    private long [][] leafCounts;

    // region Deltas
    private transient boolean accumulateDeltas = false;
    private transient TShortHashSet added;
    private transient TShortHashSet removed;
    private transient ShortVector prevValues;
    // endregion Deltas


    //region Constructor
    /**
     * Create a ShortSegmentedSortedArray with the given leafSize.
     *
     * @param leafSize the maximumSize for any leaf
     */
    public ShortSegmentedSortedMultiset(int leafSize) {
        this.leafSize = leafSize;
        leafCount = 0;
        size = 0;
    }
    //endregion Constructor

    //region Insertion
    @Override
    public boolean insert(WritableChunk<? extends Values> valuesToInsert, WritableIntChunk<ChunkLengths> counts) {
        final long beforeSize = size();
        insert(valuesToInsert.asWritableShortChunk(), counts);
        return beforeSize != size();
    }

    private int insertExistingIntoLeaf(WritableShortChunk<? extends Values> valuesToInsert, WritableIntChunk<ChunkLengths> counts, int ripos, MutableInt wipos, int leafSize, short [] leafValues, long [] leafCounts, short maxInsert, boolean lastLeaf) {
        int rlpos = 0;
        short nextValue;
        while (rlpos < leafSize && ripos < valuesToInsert.size() && (leq(nextValue = valuesToInsert.get(ripos), maxInsert) || lastLeaf)) {
            if (gt(leafValues[rlpos], nextValue)) {
                // we're not going to find nextValue in this leaf, so we skip over it
                valuesToInsert.set(wipos.intValue(), nextValue);
                counts.set(wipos.intValue(), counts.get(ripos));
                wipos.increment();
                ripos++;
            } else {
                rlpos = upperBound(leafValues, rlpos, leafSize, nextValue);
                if (rlpos < leafSize) {
                    if (eq(leafValues[rlpos], nextValue)) {
                        leafCounts[rlpos] += counts.get(ripos);
                        ripos++;
                    }
                } else if (rlpos == leafSize) {
                    // we have hit the end of the leaf, we can not insert any value that is less than maxvalue
                    final int lastInsert = lastLeaf ? valuesToInsert.size() : upperBound(valuesToInsert, ripos, valuesToInsert.size(), maxInsert);

                    // noinspection unchecked
                    valuesToInsert.copyFromTypedChunk((WritableShortChunk)valuesToInsert, ripos, wipos.intValue(), lastInsert - ripos);
                    counts.copyFromTypedChunk(counts, ripos, wipos.intValue(), lastInsert - ripos);
                    wipos.add(lastInsert - ripos);
                    ripos = lastInsert;
                }
            }
        }
        return ripos;
    }

    private void distributeNewIntoLeaves(ShortChunk<? extends Values> valuesToInsert, IntChunk<ChunkLengths> counts, final int insertStart, final int insertCount, int firstLeaf, int requiredLeaves, int newLeafSize) {
        Assert.gtZero(insertCount, "insertCount");

        final int valuesPerLeaf = valuesPerLeaf(newLeafSize, requiredLeaves);
        final int lastLeafSize = newLeafSize - valuesPerLeaf * (requiredLeaves - 1);
        // we start at the back of the arrays, writing into each leaf as needed

        int wleaf = firstLeaf + requiredLeaves - 1;
        int wpos = lastLeafSize - 1;
        int ripos = insertStart + insertCount - 1;
        int rlpos = leafSizes[firstLeaf] - 1;
        Assert.geq(leafValues[firstLeaf].length, "leafValues[firstLeaf].length", rlpos);
        Assert.geq(leafCounts[firstLeaf].length, "leafCounts[firstLeaf].length", rlpos);

        leafSizes[firstLeaf] = valuesPerLeaf;
        for (int li = firstLeaf + 1; li < firstLeaf + requiredLeaves; ++li) {
            leafValues[li] = new short[leafSize];
            leafCounts[li] = new long[leafSize];
            leafSizes[li] = valuesPerLeaf;
        }
        leafSizes[wleaf] = lastLeafSize;

        int remaining = newLeafSize;
        int lwins = 0;
        int iwins = 0;

        // starting at the last leaf, pull from either the chunk or the first leaf in the range as appropriate
        while (remaining-- > 0) {
            final short insertValue = valuesToInsert.get(ripos);
            final short leafValue = leafValues[firstLeaf][rlpos];
            final boolean useInsertValue = gt(insertValue, leafValue);

            if (useInsertValue) {
                leafValues[wleaf][wpos] = insertValue;
                leafCounts[wleaf][wpos] = counts.get(ripos);
                ripos--;
                wpos--;

                iwins++;
                lwins = 0;

                if (wpos >= 0 && iwins > minGallop) {
                    // find the smallest insert value greater than the leafValue, but do not go beyond the beginning of
                    // the leaf we are writing to
                    final int minInsert = gallopBound(valuesToInsert, Math.max(insertStart, ripos - wpos), ripos + 1, leafValue);

                    final int gallopLength = ripos - minInsert + 1;

                    if (gallopLength > 0) {
                        valuesToInsert.copyToTypedArray(minInsert, leafValues[wleaf], wpos - gallopLength + 1, gallopLength);
                        while (ripos >= minInsert) {
                            leafCounts[wleaf][wpos--] = counts.get(ripos--);
                        }
                        remaining -= gallopLength;
                    }

                    if (gallopLength < TimsortUtils.INITIAL_GALLOP) {
                        minGallop++;
                    } else {
                        minGallop = Math.max(2, minGallop - 1);
                    }
                    iwins = 0;
                }
            } else {
                leafValues[wleaf][wpos] = leafValue;
                leafCounts[wleaf][wpos] = leafCounts[firstLeaf][rlpos];
                rlpos--;
                wpos--;

                lwins++;
                iwins = 0;

                if (lwins > minGallop) {
                    // find the smallest leaf value greater than the insertValue, but do not go beyond the beginning of
                    // the leaf we are writing to
                    final int minInsert = gallopBound(leafValues[firstLeaf], Math.max(0, rlpos - wpos), rlpos + 1, insertValue);
                    final int gallopLength = rlpos - minInsert + 1;

                    if (gallopLength > 0) {
                        System.arraycopy(leafValues[firstLeaf], minInsert, leafValues[wleaf], wpos - gallopLength + 1, gallopLength);
                        System.arraycopy(leafCounts[firstLeaf], minInsert, leafCounts[wleaf], wpos - gallopLength + 1, gallopLength);
                        rlpos -= gallopLength;
                        wpos -= gallopLength;
                        remaining -= gallopLength;
                    }

                    if (gallopLength < TimsortUtils.INITIAL_GALLOP) {
                        minGallop++;
                    } else {
                        minGallop = Math.max(2, minGallop - 1);
                    }
                    lwins = 0;
                }
            }
            if (wpos < 0) {
                // allocate the next leaf
                if (wleaf < leafCount - 1) {
                    updateDirectory(wleaf);
                }
                wpos = valuesPerLeaf - 1;
                wleaf--;
            }
            if (ripos < insertStart) {
                // we have nothing left to insert, but still may need to distribute values between the leaves
                break;
            }
            if (rlpos < 0) {
                // we have no leaf values left, but still need to copy the insert values into leaves as appropriate
                break;
            }
        }

        if (ripos >= insertStart) {
            assert rlpos < 0;
            while (remaining > 0) {
                // we want to copy wpos + 1 values at a time
                final int copySize = wpos + 1;
                valuesToInsert.copyToTypedArray(ripos - wpos, leafValues[wleaf], 0, copySize);
                for (int ii = 0; ii < copySize; ++ii) {
                    leafCounts[wleaf][ii] = counts.get(ripos - (copySize - 1) + ii);
                }
                ripos -= copySize;
                remaining -= copySize;

                // allocate the next leaf
                if (wleaf < leafCount - 1) {
                    updateDirectory(wleaf);
                }
                wpos = valuesPerLeaf - 1;
                wleaf--;
            }
        }
        else {
            assert rlpos >= 0;
            // we need to copy the rest of the leaf values

            while (remaining > 0) {
                final int copySize = wpos + 1;
                System.arraycopy(leafValues[firstLeaf], rlpos - wpos, leafValues[wleaf], 0, copySize);
                System.arraycopy(leafCounts[firstLeaf], rlpos - wpos, leafCounts[wleaf], 0, copySize);
                rlpos -= copySize;
                remaining -= copySize;

                // allocate the next leaf
                if (wleaf < leafCount - 1) {
                    updateDirectory(wleaf);
                }
                wpos = valuesPerLeaf - 1;
                wleaf--;
            }
        }

        size += insertCount;
    }

    private void insertNewIntoLeaf(WritableShortChunk<? extends Values> valuesToInsert, WritableIntChunk<ChunkLengths> counts, int insertStart, int insertCount, int leafSize, short [] leafValues, long [] leafCounts) {
        assert insertCount > 0;

        // we start at the end of the leaf and insert values, picking off the correct value as we go
        int wpos = leafSize + insertCount - 1;
        int ripos = insertStart + insertCount - 1;
        int rlpos = leafSize - 1;

        int lwins = 0; // leaf wins
        int iwins = 0; // insert wins

        while (wpos >= 0) {
            final short insertValue = valuesToInsert.get(ripos);
            final short leafValue = leafValues[rlpos];

            if (gt(insertValue, leafValue)) {
                leafValues[wpos] = insertValue;
                leafCounts[wpos] = counts.get(ripos);
                if (ripos == 0) {
                    // all that is left is the leaf so we are completed
                    return;
                }
                ripos--;
                wpos--;

                iwins++;
                lwins = 0;
                if (iwins > minGallop) {
                    final int minInsert = gallopBound(valuesToInsert, 0, ripos + 1, leafValue);

                    final int gallopLength = ripos - minInsert + 1;

                    if (gallopLength > 0) {
                        valuesToInsert.copyToTypedArray(minInsert, leafValues, wpos - gallopLength + 1, gallopLength);
                        while (ripos >= minInsert) {
                            leafCounts[wpos--] = counts.get(ripos--);
                        }

                        if (ripos == -1) {
                            return;
                        }
                    }
                    if (gallopLength < TimsortUtils.INITIAL_GALLOP) {
                        minGallop++;
                    } else {
                        minGallop = Math.max(2, minGallop - 1);
                    }
                    iwins = 0;
                }
            } else {
                leafValues[wpos] = leafValue;
                leafCounts[wpos] = leafCounts[rlpos];
                if (rlpos == 0) {
                    // we just need to copy the remaining insert values to the leaf
                    copyRemainingValuesToLeaf(valuesToInsert, counts, insertStart, leafValues, leafCounts, ripos);
                    return;
                }
                rlpos--;
                wpos--;

                lwins++;
                iwins = 0;
                if (lwins > minGallop) {
                    final int minInsert = gallopBound(leafValues, 0, rlpos + 1, insertValue);
                    final int gallopLength = rlpos - minInsert + 1;

                    if (gallopLength > 0) {
                        System.arraycopy(leafValues, minInsert, leafValues, wpos - gallopLength + 1, gallopLength);
                        System.arraycopy(leafCounts, minInsert, leafCounts, wpos - gallopLength + 1, gallopLength);
                        rlpos -= gallopLength;
                        wpos -= gallopLength;
                        if (rlpos == -1) {
                            copyRemainingValuesToLeaf(valuesToInsert, counts, insertStart, leafValues, leafCounts, ripos);
                            return;
                        }
                    }

                    if (gallopLength < TimsortUtils.INITIAL_GALLOP) {
                        minGallop++;
                    } else {
                        minGallop = Math.max(2, minGallop - 1);
                    }
                }
            }
        }
    }

    private void copyRemainingValuesToLeaf(WritableShortChunk<? extends Values> valuesToInsert, WritableIntChunk<ChunkLengths> counts, int insertStart, short[] leafValues, long[] leafCounts, int ripos) {
        valuesToInsert.copyToTypedArray(insertStart, leafValues, 0, ripos - insertStart + 1);
        for (int ii = 0; ii < ripos - insertStart + 1; ++ii) {
            leafCounts[ii] = counts.get(ii + insertStart);
        }
    }

    private void maybeCompact(WritableShortChunk<? extends Values> valuesToInsert, WritableIntChunk<ChunkLengths> counts, int ripos, int wipos) {
        if (wipos == ripos) {
            return;
        }
        // we've found something to compact away
        final int originalSize = valuesToInsert.size();
        final int toCopy = originalSize - ripos;
        //noinspection unchecked - how the heck does this type not actuall work?
        valuesToInsert.copyFromTypedChunk((ShortChunk)valuesToInsert, ripos, wipos, toCopy);
        counts.copyFromChunk(counts, ripos, wipos, toCopy);
        valuesToInsert.setSize(wipos + toCopy);
        counts.setSize(wipos + toCopy);
    }

    private void insertExisting(WritableShortChunk<? extends Values> valuesToInsert, WritableIntChunk<ChunkLengths> counts) {
        if (leafCount == 0) {
            return;
        }
        if (leafCount == 1) {
            final MutableInt wipos = new MutableInt(0);
            final int ripos = insertExistingIntoLeaf(valuesToInsert, counts, 0, wipos, size, directoryValues, directoryCount, NULL_SHORT, true);
            maybeCompact(valuesToInsert, counts, ripos, wipos.intValue());
            return;
        }

        // we have multiple leaves that we should insert into
        final MutableInt wipos = new MutableInt(0);
        int ripos = 0;
        int nextLeaf = 0;
        while (ripos < valuesToInsert.size()) {
            final short startValue = valuesToInsert.get(ripos);
            nextLeaf = lowerBoundExclusive(directoryValues, nextLeaf, leafCount - 1, startValue);
            // find the thing in directoryValues
            final boolean lastLeaf = nextLeaf == leafCount - 1;
            final short maxValue = lastLeaf ? NULL_SHORT : directoryValues[nextLeaf];
            ripos = insertExistingIntoLeaf(valuesToInsert, counts, ripos, wipos, leafSizes[nextLeaf], leafValues[nextLeaf], leafCounts[nextLeaf], maxValue, lastLeaf);
            if (lastLeaf) {
                break;
            }
        }
        maybeCompact(valuesToInsert, counts, ripos, wipos.intValue());
    }

    private void insert(WritableShortChunk<? extends Values> valuesToInsert, WritableIntChunk<ChunkLengths> counts) {
        validate();
        validateInputs(valuesToInsert, counts);
        if (valuesToInsert.size() == 0) {
            return;
        }

        totalSize += SumIntChunk.sumIntChunk(counts, 0, counts.size());

        if (leafCount == 0) {
            // we are creating something brand new
            makeLeavesInitial(valuesToInsert, counts);
            maybeAccumulateAdditions(valuesToInsert);
            validate();
            return;
        }
        insertExisting(valuesToInsert, counts);

        if (valuesToInsert.size() == 0) {
            validate();
            return;
        }

        maybeAccumulateAdditions(valuesToInsert);

        if (leafCount > 1 && gt(valuesToInsert.get(0), getMaxShort())) {
            doAppend(valuesToInsert, counts);
            return;
        }

        final int newSize = valuesToInsert.size() + size;
        final int desiredLeafCount = getDesiredLeafCount(newSize);

        // now we are inserting things, which we know to be new
        if (leafCount == 1) {
            // if we are too small to fit the excess, increase our size
            final int freeLocations = directoryValues.length - size;
            if (freeLocations < valuesToInsert.size()) {
                if (size + valuesToInsert.size() > leafSize) {
                    // we must move the directory into the first leaf
                    moveDirectoryToLeaf(desiredLeafCount);
                } else {
                    directoryValues = Arrays.copyOf(directoryValues, newSize);
                    directoryCount = Arrays.copyOf(directoryCount, newSize);
                }
            }
            if (desiredLeafCount == 1) {
                // we should fit into the existing leaf
                insertNewIntoLeaf(valuesToInsert, counts, 0, valuesToInsert.size(), size, directoryValues, directoryCount);
                size = newSize;
                validate();
                return;
            }
        }

        // this might not be enough, but we should at least start out with enough room for what we will insert
        reallocateLeafArrays(desiredLeafCount);

        int rpos = 0;
        int nextLeaf = 0;

        do {
            final short insertValue = valuesToInsert.get(rpos);
            // find out what leaf this belongs in
            nextLeaf = leafCount > 1 ? upperBound(directoryValues, nextLeaf, leafCount - 1, insertValue) : 0;

            // now figure out the last insert value that is suitable for this leaf
            final int lastInsertValue;
            if (nextLeaf == leafCount - 1) {
                // we should insert all of the remaining values in this leaf
                lastInsertValue = valuesToInsert.size();
            } else {
                final short lastLeafValue = directoryValues[nextLeaf];
                lastInsertValue = upperBound(valuesToInsert, rpos, valuesToInsert.size(), lastLeafValue);
            }
            final int originalLeafSize = leafSizes[nextLeaf];
            final int insertIntoLeaf = lastInsertValue - rpos;
            final int newLeafSize = originalLeafSize + insertIntoLeaf;

            final int requiredLeaves = getDesiredLeafCount(newLeafSize);
            if (requiredLeaves > 1) {
                // we need to make a hole for the new things
                makeLeafHole(nextLeaf + 1, requiredLeaves - 1);
                leafCount += (requiredLeaves - 1);
            }
            distributeNewIntoLeaves(valuesToInsert, counts, rpos, insertIntoLeaf, nextLeaf, requiredLeaves, newLeafSize);

            rpos += insertIntoLeaf;
        }
        while (rpos < valuesToInsert.size());

        validate();
    }

    private void moveDirectoryToLeaf(int desiredLeafCount) {
        moveDirectoryToLeaf(desiredLeafCount, 0);
    }

    private void moveDirectoryToLeaf(int desiredLeafCount, int directoryLocation) {
        leafValues = new short[desiredLeafCount][];
        leafCounts = new long[desiredLeafCount][];
        leafSizes = new int[desiredLeafCount];

        leafSizes[directoryLocation] = size;
        if (directoryValues.length < leafSize) {
            leafValues[directoryLocation] = Arrays.copyOf(directoryValues, leafSize);
            leafCounts[directoryLocation] = Arrays.copyOf(directoryCount, leafSize);
        } else {
            Assert.eq(directoryValues.length, "directoryValues.length", leafSize, "leafSize");
            leafValues[directoryLocation] = directoryValues;
            leafCounts[directoryLocation] = directoryCount;
        }
        leafCount = 1;

        directoryCount = null;
        directoryValues = new short[desiredLeafCount - 1];
    }

    private void doAppend(WritableShortChunk<? extends Values> valuesToInsert, WritableIntChunk<ChunkLengths> counts) {
        // We are doing a special case of appending to the SSM
        final int lastLeafIndex = leafCount - 1;
        final int lastLeafSize = leafSizes[lastLeafIndex];
        final int lastLeafFree = this.leafSize - lastLeafSize;
        int rpos = 0;
        if (lastLeafFree > 0) {
            final int insertCount = Math.min(lastLeafFree, valuesToInsert.size());
            insertNewIntoLeaf(valuesToInsert, counts, rpos, insertCount, lastLeafSize, leafValues[lastLeafIndex], leafCounts[lastLeafIndex]);
            leafSizes[lastLeafIndex] += insertCount;
            rpos += insertCount;
            if (insertCount == valuesToInsert.size()) {
                size += insertCount;
                validate();
                return;
            }
        }
        final int newLeavesRequired = getDesiredLeafCount(valuesToInsert.size() - rpos);
        reallocateLeafArrays(leafCount + newLeavesRequired);
        // we need to fixup the directory from the last leaf
        if (rpos > 0) {
            directoryValues[lastLeafIndex] = valuesToInsert.get(rpos - 1);
        } else {
            assert leafSizes[lastLeafIndex] == leafSize;
            directoryValues[lastLeafIndex] = leafValues[lastLeafIndex][leafSize - 1];
        }
        final int oldLeafCount = leafCount;
        leafCount += newLeavesRequired;
        packValuesIntoLeaves(valuesToInsert, counts, rpos, oldLeafCount, leafSize);
        size += valuesToInsert.size();
        validate();
    }

    private void copyLeavesAndDirectory(int srcPos, int destPos, int length) {
        System.arraycopy(leafSizes, srcPos, leafSizes, destPos, length);
        System.arraycopy(leafValues, srcPos, leafValues, destPos, length);
        System.arraycopy(leafCounts, srcPos, leafCounts, destPos, length);
        if (destPos > srcPos) {
            System.arraycopy(directoryValues, srcPos, directoryValues, destPos, length - 1);
        } else {
            System.arraycopy(directoryValues, srcPos, directoryValues, destPos, length);
        }
    }

    private void makeLeafHole(int holePosition, int holeSize) {
        reallocateLeafArrays(holeSize + leafCount);
        if (holePosition != leafCount) {
            copyLeavesAndDirectory(holePosition, holePosition + holeSize, leafCount - holePosition);
        }

        // this is not strictly necessary; but will make debugging simpler
        Arrays.fill(leafSizes, holePosition, holePosition + holeSize, 0);
        Arrays.fill(leafValues, holePosition, holePosition + holeSize, null);
        Arrays.fill(leafCounts, holePosition, holePosition + holeSize, null);
        // region fillValue
        if (holePosition + holeSize < leafValues.length) {
            Arrays.fill(directoryValues, holePosition, holePosition + holeSize, NULL_SHORT);
        } else {
            Arrays.fill(directoryValues, holePosition, holePosition + holeSize - 1, NULL_SHORT);
        }
        // endregion fillValue
    }

    private void reallocateLeafArrays(int newSize) {
        if (leafSizes.length < newSize) {
            newSize = leafArraySize(newSize);
            leafSizes = Arrays.copyOf(leafSizes, newSize);
            leafValues = Arrays.copyOf(leafValues, newSize);
            leafCounts = Arrays.copyOf(leafCounts, newSize);
            directoryValues = Arrays.copyOf(directoryValues, newSize - 1);
        }
    }

    private void allocateLeafArrays(int newSize) {
        leafValues = new short[newSize][];
        leafCounts = new long[newSize][];
        leafSizes = new int[newSize];
        directoryValues = new short[newSize - 1];
    }

    private int leafArraySize(int minimumSize) {
        return Math.max(minimumSize, leafSizes.length * 2);
    }

    private void makeLeavesInitial(ShortChunk<? extends Values> values, IntChunk<ChunkLengths> counts) {
        leafCount = getDesiredLeafCount(values.size());
        size = values.size();

        if (leafCount == 1) {
            directoryValues = new short[values.size()];
            directoryCount = new long[values.size()];
            values.copyToTypedArray(0, directoryValues, 0, values.size());
            for (int ii = 0; ii < counts.size(); ++ii) {
                directoryCount[ii] = counts.get(ii);
            }
            return;
        }

        allocateLeafArrays(leafCount);

        final int valuesPerLeaf = valuesPerLeaf(values.size(), leafCount);
        packValuesIntoLeaves(values, counts, 0, 0, valuesPerLeaf);
    }

    private void packValuesIntoLeaves(ShortChunk<? extends Values> values, IntChunk<ChunkLengths> counts, int rpos, int startLeaf, int valuesPerLeaf) {
        while (rpos < values.size()) {
            final int thisLeafSize = Math.min(valuesPerLeaf, values.size() - rpos);
            leafSizes[startLeaf] = thisLeafSize;
            leafValues[startLeaf] = new short[leafSize];
            values.copyToTypedArray(rpos, leafValues[startLeaf], 0, thisLeafSize);
            leafCounts[startLeaf] = new long[leafSize];
            for (int ii = 0; ii < thisLeafSize; ++ii) {
                leafValues[startLeaf][ii] = values.get(rpos + ii);
                leafCounts[startLeaf][ii] = counts.get(rpos + ii);
            }
            if (startLeaf < leafCount - 1) {
                directoryValues[startLeaf] = leafValues[startLeaf][thisLeafSize - 1];
            }
            rpos += thisLeafSize;
            startLeaf++;
        }
    }
    //endregion

    private void clear() {
        leafCount = 0;
        size = 0;
        totalSize = 0;
        leafValues = null;
        leafCounts = null;
        leafSizes = null;
        directoryValues = null;
        directoryCount = null;
    }

    //region Bounds search

    /**
     * Return the lowest index geq valuesToSearch.
     *
     * @param valuesToSearch the values to search for searchValue in
     * @param lo the first index to search for
     * @param hi one past the last index to search in
     * @param searchValue the value to find
     * @return the lowest index that is greater than or equal to valuesToSearch
     */
    private static int lowerBound(short [] valuesToSearch, int lo, int hi, short searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final short testValue = valuesToSearch[mid];
            final boolean moveLo = leq(testValue, searchValue);
            if (moveLo) {
                lo = mid;
                if (lo == hi - 1) {
                    break;
                }
            } else {
                hi = mid;
            }
        }

        return lo;
    }

    /**
     * Return the lowest index geq valuesToSearch.
     *
     * @param valuesToSearch the values to search for searchValue in
     * @param lo the first index to search for
     * @param hi one past the last index to search in
     * @param searchValue the value to find
     * @return the lowest index that is greater than or equal to valuesToSearch
     */
    private static int gallopBound(ShortChunk<? extends Any> valuesToSearch, int lo, int hi, short searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final short testValue = valuesToSearch.get(mid);
            final boolean moveLo = leq(testValue, searchValue);
            if (moveLo) {
                if (mid == lo) {
                    return mid + 1;
                }
                lo = mid;
            } else {
                hi = mid;
            }
        }

        return lo;
    }

    /**
     * Return the lowest index geq valuesToSearch.
     *
     * @param valuesToSearch the values to search for searchValue in
     * @param lo the first index to search for
     * @param hi one past the last index to search in
     * @param searchValue the value to find
     * @return the lowest index that is greater than or equal to valuesToSearch
     */
    private static int gallopBound(short [] valuesToSearch, int lo, int hi, short searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final short testValue = valuesToSearch[mid];
            final boolean moveLo = leq(testValue, searchValue);
            if (moveLo) {
                if (mid == lo) {
                    return mid + 1;
                }
                lo = mid;
            } else {
                hi = mid;
            }
        }

        return lo;
    }

    /**
     * Return the highest index in valuesToSearch leq searchValue.
     *
     * @param valuesToSearch the values to search for searchValue in
     * @param lo the first index to search for
     * @param hi one past the last index to search in
     * @param searchValue the value to find
     * @return the highest index that is less than or equal to valuesToSearch
     */
    private static int upperBound(short [] valuesToSearch, int lo, int hi, short searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final short testValue = valuesToSearch[mid];
            final boolean moveHi = geq(testValue, searchValue);
            if (moveHi) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        return hi;
    }

    /**
     * Return the highest index in valuesToSearch leq searchValue.
     *
     * @param valuesToSearch the values to search for searchValue in
     * @param lo the first index to search for
     * @param hi one past the last index to search in
     * @param searchValue the value to find
     * @return the highest index that is less than or equal to valuesToSearch
     */
    private static int upperBound(ShortChunk<? extends Values> valuesToSearch, int lo, int hi, short searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final short testValue = valuesToSearch.get(mid);
            final boolean moveHi = gt(testValue, searchValue);
            if (moveHi) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        return hi;
    }

    /**
     * Return the lowest index gt valuesToSearch.
     *
     * @param valuesToSearch the values to search for searchValue in
     * @param lo the first index to search for
     * @param hi one past the last index to search in
     * @param searchValue the value to find
     * @return the lowest index that is greater than to valuesToSearch
     */
    private static int lowerBoundExclusive(short [] valuesToSearch, int lo, int hi, short searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final short testValue = valuesToSearch[mid];
            final boolean moveLo = lt(testValue, searchValue);
            if (moveLo) {
                lo = mid + 1;
                if (lo == hi) {
                    break;
                }
            } else {
                hi = mid;
            }
        }

        return lo;
    }

    //endregion

    //region Removal
    /**
     * Remove valuesToRemove from this SSA.  The valuesToRemove to remove must be sorted.
     *
     * @param valuesToRemove    the valuesToRemove to remove
     */
    @Override
    public boolean remove(RemoveContext removeContext, WritableChunk<? extends Values> valuesToRemove, WritableIntChunk<ChunkLengths> counts) {
        final long beforeSize = size();
        remove(removeContext, valuesToRemove.asShortChunk(), counts);
        return beforeSize != size();
    }

    private void remove(RemoveContext removeContext, ShortChunk<? extends Values> valuesToRemove, IntChunk<ChunkLengths> counts) {
        validate();
        validateInputs(valuesToRemove, counts);

        final int removeSize = valuesToRemove.size();
        if (removeSize == 0) {
            return;
        }

        totalSize -= SumIntChunk.sumIntChunk(counts, 0, counts.size());

        if (leafCount == 1) {
            final MutableInt sz = new MutableInt(size);
            final int consumed = removeFromLeaf(removeContext, valuesToRemove, counts, 0, valuesToRemove.size(), directoryValues, directoryCount, sz);
            assert consumed == valuesToRemove.size();
            if (sz.intValue() == 0) {
                clear();
            } else {
                size = sz.intValue();
            }
        } else {
            removeContext.ensureLeafCount((leafCount + 1)/ 2);

            int rpos = 0;
            int nextLeaf = 0;
            int cl = -1;
            do {
                // figure out what the first leaf we can remove something from is
                final short firstValueToRemove = valuesToRemove.get(rpos);
                nextLeaf = lowerBound(directoryValues, nextLeaf, leafCount - 1, firstValueToRemove);

                final MutableInt sz = new MutableInt(leafSizes[nextLeaf]);
                rpos = removeFromLeaf(removeContext, valuesToRemove, counts, rpos, valuesToRemove.size(), leafValues[nextLeaf], leafCounts[nextLeaf], sz);
                size -= leafSizes[nextLeaf] - sz.intValue();
                leafSizes[nextLeaf] = sz.intValue();
                if (sz.intValue() == 0) {
                    cl = markLeafForRemoval(removeContext, nextLeaf, cl);
                } else {
                    // we figure out if we can be pulled back into the prior leaf
                    final int priorLeaf;
                    if (cl >= 0 && removeContext.compactionLeafs[cl] + removeContext.compactionLeafLengths[cl] == nextLeaf) {
                        // we need to go to one leaf before our compaction length, if we happen to be removing all
                        // the prior leaves we end up with a negative number here.
                        priorLeaf = removeContext.compactionLeafs[cl] - 1;
                    } else {
                        priorLeaf = nextLeaf - 1;
                    }
                    if (priorLeaf >= 0 && leafSizes[priorLeaf] + leafSizes[nextLeaf] <= leafSize) {
                        final int priorAndCurrentSize = leafSizes[priorLeaf] + leafSizes[nextLeaf];
                        if (nextLeaf < leafCount - 1 && priorAndCurrentSize + leafSizes[nextLeaf + 1] <= leafSize) {
                            // we need to merge all three of these leaves
                            mergeThreeLeavesForward(priorLeaf, nextLeaf, nextLeaf + 1);
                            if (priorLeaf < nextLeaf - 1) {
                                // this means we should be adding a leaf to remove that is before a range of removals
                                cl = addLeafToLastRemovalRange(removeContext, priorLeaf, cl);
                            } else {
                                cl = markLeafForRemoval(removeContext, priorLeaf, cl);
                            }
                            cl = markLeafForRemoval(removeContext, nextLeaf, cl);
                        } else {
                            mergeTwoLeavesBack(priorLeaf, nextLeaf);
                            cl = markLeafForRemoval(removeContext, nextLeaf, cl);
                        }
                    }
                    else if (nextLeaf < leafCount - 1 && leafSizes[nextLeaf] + leafSizes[nextLeaf + 1] <= leafSize) {
                        // we shove ourselves forward into the next leaf
                        mergeTwoLeavesForward(nextLeaf, nextLeaf + 1);
                        cl = markLeafForRemoval(removeContext, nextLeaf, cl);
                    }
                }
                nextLeaf++;

                validateCompaction(removeContext, cl);
            }
            while (rpos < valuesToRemove.size());

            if (size == 0) {
                clear();
            } else {
                compactLeafs(removeContext, cl);
            }
        }

        validate();
    }

    private void validateCompaction(RemoveContext removeContext, int cl) {
        for (int ii = 0; ii <= cl - 1; ++ii) {
            final int firstCompactLeaf = removeContext.compactionLeafs[ii];
            final int lastCompactLeaf = firstCompactLeaf + removeContext.compactionLeafLengths[ii] - 1;
            final int nextCompactLeaf = removeContext.compactionLeafs[ii + 1];
            Assert.gt(nextCompactLeaf, "nextCompactLeaf", lastCompactLeaf, "lastCompactLeaf");
        }
    }

    private void mergeTwoLeavesBack(int firstLeafDestination, int secondLeafSource) {
        final int wpos = leafSizes[firstLeafDestination];
        final int secondSourceSize = leafSizes[secondLeafSource];
        System.arraycopy(leafValues[secondLeafSource], 0, leafValues[firstLeafDestination], wpos, secondSourceSize);
        System.arraycopy(leafCounts[secondLeafSource], 0, leafCounts[firstLeafDestination], wpos, secondSourceSize);
        leafSizes[firstLeafDestination] += secondSourceSize;
        leafSizes[secondLeafSource] = 0;
        if (secondLeafSource < leafCount - 1) {
            directoryValues[firstLeafDestination] = directoryValues[secondLeafSource];
        }
    }

    private void mergeTwoLeavesForward(int firstLeafSource, int secondLeafDestination) {
        final int firstSourceSize = leafSizes[firstLeafSource];
        final int secondDestinationSize = leafSizes[secondLeafDestination];
        // first make a hole
        System.arraycopy(leafValues[secondLeafDestination], 0, leafValues[secondLeafDestination], firstSourceSize, secondDestinationSize);
        System.arraycopy(leafCounts[secondLeafDestination], 0, leafCounts[secondLeafDestination], firstSourceSize, secondDestinationSize);
        // now copy the first leaf into that hole
        System.arraycopy(leafValues[firstLeafSource], 0, leafValues[secondLeafDestination], 0, firstSourceSize);
        System.arraycopy(leafCounts[firstLeafSource], 0, leafCounts[secondLeafDestination], 0, firstSourceSize);
        leafSizes[secondLeafDestination] += firstSourceSize;
        leafSizes[firstLeafSource] = 0;
        // the directory values should be ignored at this point, and is marked for removal
    }

    private void mergeThreeLeavesForward(int firstLeafSource, int secondLeafSource, int thirdLeafDestination) {
        final int firstSourceSize = leafSizes[firstLeafSource];
        final int secondSourceSize = leafSizes[secondLeafSource];
        final int totalSourceSize = firstSourceSize + secondSourceSize;
        final int thirdDestinationSize = leafSizes[thirdLeafDestination];

        // first make a hole
        System.arraycopy(leafValues[thirdLeafDestination], 0, leafValues[thirdLeafDestination], totalSourceSize, thirdDestinationSize);
        System.arraycopy(leafCounts[thirdLeafDestination], 0, leafCounts[thirdLeafDestination], totalSourceSize, thirdDestinationSize);

        // now copy the first leaf into that hole
        System.arraycopy(leafValues[firstLeafSource], 0, leafValues[thirdLeafDestination], 0, firstSourceSize);
        System.arraycopy(leafCounts[firstLeafSource], 0, leafCounts[thirdLeafDestination], 0, firstSourceSize);
        System.arraycopy(leafValues[secondLeafSource], 0, leafValues[thirdLeafDestination], firstSourceSize, secondSourceSize);
        System.arraycopy(leafCounts[secondLeafSource], 0, leafCounts[thirdLeafDestination], firstSourceSize, secondSourceSize);

        leafSizes[thirdLeafDestination] += totalSourceSize;
        leafSizes[firstLeafSource] = 0;
        leafSizes[secondLeafSource] = 0;
        // the directory values should be ignored at this point, and is marked for removal
    }

    private int markLeafForRemoval(RemoveContext removeContext, int leafToRemove, int cl) {
        validateCompaction(removeContext, cl);
        // we've removed all values in this leaf, so we need to mark it for deletion from our list
        if (cl == -1) {
            removeContext.compactionLeafs[cl = 0] = leafToRemove;
            removeContext.compactionLeafLengths[cl] = 1;
        } else if (removeContext.compactionLeafs[cl] + removeContext.compactionLeafLengths[cl] == leafToRemove) {
            removeContext.compactionLeafLengths[cl]++;
        } else {
            removeContext.compactionLeafs[++cl] = leafToRemove;
            removeContext.compactionLeafLengths[cl] = 1;
        }
        validateCompaction(removeContext, cl);
        return cl;
    }

    private int addLeafToLastRemovalRange(RemoveContext removeContext, int leafToRemove, int cl) {
        validateCompaction(removeContext, cl);
        assert cl >= 0;
        // we've removed all values in this leaf, so we need to mark it for deletion from our list
        assert removeContext.compactionLeafs[cl] == leafToRemove + 1;

        removeContext.compactionLeafs[cl]--;
        removeContext.compactionLeafLengths[cl]++;

        // we might need to collapse two adjacent ranges in the compaction
        if (cl > 0 && removeContext.compactionLeafs[cl - 1] + removeContext.compactionLeafLengths[cl - 1] == removeContext.compactionLeafs[cl]) {
            removeContext.compactionLeafLengths[cl - 1] += removeContext.compactionLeafLengths[cl];
            cl--;
        }

        validateCompaction(removeContext, cl);
        return cl;
    }

    private void compactLeafs(RemoveContext removeContext, int cl) {
        assert removeContext != null;

        int removed = 0;
        for (int cli = 0; cli <= cl; cli++) {
            final int removeSize = removeContext.compactionLeafLengths[cli];
            final int rposc = removeContext.compactionLeafs[cli] + removeSize;
            final int wpos = removeContext.compactionLeafs[cli] - removed;
            removed += removeSize;

            if (rposc <= leafCount) {
                // we are not removing everything, so have to copy
                final int lastrposc;
                if (cli < cl) {
                    lastrposc = removeContext.compactionLeafs[cli + 1];
                } else {
                    lastrposc = leafCount;
                }
                System.arraycopy(leafValues, rposc, leafValues, wpos, lastrposc - rposc);
                System.arraycopy(leafCounts, rposc, leafCounts, wpos, lastrposc - rposc);
                System.arraycopy(leafSizes, rposc, leafSizes, wpos, lastrposc - rposc);
                if (rposc < leafCount - 1) {
                    final int lastrposd = Math.min(lastrposc, leafCount - 1);
                    System.arraycopy(directoryValues, rposc, directoryValues, wpos, lastrposd - rposc);
                }
            }
        }
        Arrays.fill(leafValues, leafCount - removed, leafCount, null); // be friendly to our GC
        Arrays.fill(leafCounts, leafCount - removed, leafCount, null);
        Arrays.fill(leafSizes, leafCount - removed, leafCount, 0); // not necessary, but nice for debugging
        leafCount -= removed;
        maybePromoteLastLeaf();
    }

    private void maybePromoteLastLeaf() {
        if (leafCount == 1) {
            directoryValues = leafValues[0];
            directoryCount = leafCounts[0];
            leafValues = null;
            leafCounts = null;
            leafSizes = null;
            if (directoryValues.length > size * 2) {
                directoryValues = Arrays.copyOf(directoryValues, size);
                directoryCount = Arrays.copyOf(directoryCount, size);
            }
        }
    }

    private int removeFromLeaf(RemoveContext removeContext, ShortChunk<? extends Values> valuesToRemove, IntChunk<ChunkLengths> counts, int ripos, int end, short[] leafValues, long[] leafCounts, MutableInt sz) {
        int rlpos = 0;
        int cl = -1;
        while (ripos < end) {
            final short removeValue = valuesToRemove.get(ripos);
            rlpos = upperBound(leafValues, rlpos, sz.intValue(), removeValue);
            if (rlpos == sz.intValue()) {
                break;
            }
            leafCounts[rlpos] -= counts.get(ripos);
            Assert.geqZero(leafCounts[rlpos], "leafCounts[rlpos]");
            if (leafCounts[rlpos] == 0) {
                maybeAccumulateRemoval(removeValue);
                // we need to do some compaction at the end of this iteration
                if (cl == -1) {
                    removeContext.compactionLocations[cl = 0] = rlpos;
                    removeContext.compactionLengths[cl] = 1;
                } else {
                    final int nextCompact = removeContext.compactionLocations[cl] + removeContext.compactionLengths[cl];
                    if (nextCompact == rlpos) {
                        removeContext.compactionLengths[cl]++;
                    } else {
                        removeContext.compactionLocations[++cl] = rlpos;
                        removeContext.compactionLengths[cl] = 1;
                    }
                }
            }
            ripos++;

            for (int cli = 0; cli < cl; ++cli) {
                if (removeContext.compactionLocations[cli] + removeContext.compactionLengths[cli] == removeContext.compactionLocations[cli + 1]) {
                    throw new IllegalStateException();
                }
            }
        }
        if (cl == 0 && removeContext.compactionLengths[0] == sz.intValue()) {
            // we've removed everything, so no need to compact
            sz.setValue(0);
            return ripos;
        }
        final int removed = compactValues(removeContext, leafValues, leafCounts, sz.intValue(), cl);
        sz.subtract(removed);
        return ripos;
    }

    private int compactValues(RemoveContext removeContext, short[] leafValues, long[] leafCounts, int sz, int cl) {
        int removed = 0;
        for (int cli = 0; cli <= cl; cli++) {
            final int removeSize = removeContext.compactionLengths[cli];
            final int rpos = removeContext.compactionLocations[cli] + removeSize;
            final int wpos = removeContext.compactionLocations[cli] - removed;
            removed += removeSize;
            if (rpos <= sz) {
                // we are not removing everything, so have to copy
                final int lastrpos;
                if (cli < cl) {
                    lastrpos = removeContext.compactionLocations[cli + 1];
                } else {
                    lastrpos = sz;
                }
                System.arraycopy(leafValues, rpos, leafValues, wpos, lastrpos - rpos);
                System.arraycopy(leafCounts, rpos, leafCounts, wpos, lastrpos - rpos);
            }
        }
        return removed;
    }
    //endregion

    //region Validation
    @VisibleForTesting
    public void validate() {
        if (!SEGMENTED_SORTED_MULTISET_VALIDATION) {
            return;
        }
        validateInternal();
    }

    private void validateInputs(ShortChunk<? extends Values> valuesToInsert, IntChunk<ChunkLengths> counts) {
        if (!SEGMENTED_SORTED_MULTISET_VALIDATION) {
            return;
        }
        Assert.eq(valuesToInsert.size(), "valuesToInsert.size()", counts.size(), "counts.size()");
        if (counts.size() > 0) {
            Assert.gtZero(counts.get(0), "counts.get(ii)");
        }
        for (int ii = 1; ii < valuesToInsert.size(); ++ii) {
            Assert.gtZero(counts.get(ii), "counts.get(ii)");
            final short prevValue = valuesToInsert.get(ii - 1);
            final short curValue = valuesToInsert.get(ii);
            Assert.assertion(ShortComparisons.lt(prevValue, curValue), "ShortComparisons.lt(prevValue, curValue)", prevValue, "prevValue", curValue, "curValue");
        }
    }

    private void validateInternal() {
        Assert.geqZero(size, "size");
        Assert.geqZero(totalSize, "totalSize");
        if (size == 0) {
            Assert.eqZero(leafCount, "leafCount");
        } else {
            Assert.gtZero(leafCount, "leafCount");
        }
        Assert.geq(totalSize, "totalSize", size, "size");
        if (leafCount == 0) {
            Assert.eqNull(leafValues, "leafValues");
            Assert.eqNull(leafCounts, "leafValues");
            Assert.eqNull(leafSizes, "leafSizes");
            Assert.eqNull(directoryCount, "directoryIndex");
            Assert.eqNull(directoryValues, "directoryValues");
        } else if (leafCount == 1) {
            Assert.eqNull(leafValues, "leafValues");
            Assert.eqNull(leafCounts, "leafValues");
            Assert.eqNull(leafSizes, "leafSizes");
            Assert.neqNull(directoryCount, "directoryIndex");
            Assert.neqNull(directoryValues, "directoryValues");
            Assert.geq(directoryCount.length, "directoryIndex.length", size, "size");
            Assert.geq(directoryValues.length, "directoryValues.length", size, "size");
            Assert.leq(directoryCount.length, "directoryIndex.length", leafSize, "leafSize");
            Assert.leq(directoryValues.length, "directoryValues.length", leafSize, "leafSize");

            validateLeaf(directoryValues, directoryCount, size);
            long totalCounts = 0;
            for (int ii = 0; ii < size; ++ii) {
                totalCounts += directoryCount[ii];
            }
            Assert.eq(totalCounts, "totalCounts", totalSize, "totalSize");
        } else {
            Assert.neqNull(leafValues, "leafValues");
            Assert.neqNull(leafCounts, "leafValues");
            Assert.neqNull(leafSizes, "leafSizes");
            Assert.eqNull(directoryCount, "directoryIndex");
            Assert.neqNull(directoryValues, "directoryValues");

            Assert.geq(directoryValues.length, "directoryValues.length", leafCount - 1, "leafCount - 1");
            Assert.geq(leafSizes.length, "directoryValues.length", leafCount, "leafCount");
            Assert.geq(leafValues.length, "directoryValues.length", leafCount, "leafCount");
            Assert.geq(leafCounts.length, "directoryValues.length", leafCount, "leafCount");

            Assert.eq(computeLeafSizes(), "computeLeafSizes()", size, "size");
            Assert.eq(computeTotalSize(), "computeTotalSize()", totalSize, "totalSize");

            for (int ii = 0; ii < leafCount; ++ii) {
                validateLeaf(ii);
                final short lastValue = leafValues[ii][leafSizes[ii] - 1];
                if (ii < leafCount - 1) {
                    final short directoryValue = directoryValues[ii];
                    Assert.assertion(leq(lastValue, directoryValue), "lt(lastValue, directoryValue)", lastValue, "leafValues[ii][leafSizes[ii] - 1]", directoryValue, "directoryValue");

                    if (ii < leafCount - 2) {
                        final short nextDirectoryValue = directoryValues[ii + 1];
                        Assert.assertion(lt(directoryValue, nextDirectoryValue), "lt(directoryValue, nextDirectoryValue)", directoryValue, "directoryValue", nextDirectoryValue, "nextDirectoryValue");
                    }

                    final short nextFirstValue = leafValues[ii + 1][0];
                    Assert.assertion(lt(directoryValue, nextFirstValue), "lt(directoryValue, nextFirstValue)", directoryValue, "directoryValue", nextFirstValue, "nextFirstValue");
                }
                // It would be nice to enable an assertion to make sure we are dense after removals, but the other
                // reason this assertion can fail is that if we insert into a node that is too large we may have to
                // split it.  The last node we have could be short, and it might be possible to merge it with the node
                // afterwards, but we don't do removals during an insertion phase.
//                if (ii < leafCount - 1) {
//                    final int thisLeafSize = leafSizes[ii];
//                    final int nextLeafSize = leafSizes[ii + 1];
//                    Assert.leq(leafSize, "leafSize", thisLeafSize + nextLeafSize, "thisLeafSize + nextLeafSize");
//                }
            }

            validateLeafOrdering();
        }
    }

    private void validateLeafOrdering() {
        for (int leaf = 0; leaf < leafCount - 1; ++leaf) {
            final short lastValue = leafValues[leaf][leafSizes[leaf] - 1];
            final short nextValue = leafValues[leaf + 1][0];
            Assert.assertion(lt(lastValue, nextValue), lastValue + " < " + nextValue);
        }
    }

    private void validateLeaf(int leaf) {
        Assert.eq(leafValues[leaf].length, "leafValues[leaf].length", leafSize);
        Assert.eq(leafCounts[leaf].length, "leafCounts[leaf].length", leafSize);
        validateLeaf(leafValues[leaf], leafCounts[leaf], leafSizes[leaf]);
    }

    private static void validateLeaf(short[] values, long[] counts, int size) {
        Assert.gtZero(size, "size");
        for (int ii = 0; ii < size - 1; ++ii) {
            Assert.gtZero(counts[ii], "counts[ii]");
            final short thisValue = values[ii];
            final short nextValue = values[ii + 1];
            Assert.assertion(lt(values[ii], values[ii + 1]), "lt(values[ii], values[ii + 1])", (Short)thisValue, "values[ii]", (Short)nextValue, "values[ii + 1]", ii, "ii");
        }
        if (size > 0) {
            Assert.gtZero(counts[size - 1], "counts[size - 1]");
        }
    }

    private int computeLeafSizes() {
        int expectedSize = 0;
        for (int ii = 0; ii < leafCount; ++ii) {
            expectedSize += leafSizes[ii];
        }
        return expectedSize;
    }

    private int computeTotalSize() {
        int expectedSize = 0;
        for (int ii = 0; ii < leafCount; ++ii) {
            for (int jj = 0; jj < leafSizes[ii]; ++jj) {
                expectedSize += leafCounts[ii][jj];
            }
        }
        return expectedSize;
    }

    //endregion

    //region Comparisons
    private int getDesiredLeafCount(int newSize) {
        return (newSize + leafSize - 1) / leafSize;
    }

    private static int valuesPerLeaf(int values, int leafCount) {
        return (values + leafCount - 1) / leafCount;
    }

    private static int doComparison(short lhs, short rhs) {
        return ShortComparisons.compare(lhs, rhs);
    }

    private static boolean gt(short lhs, short rhs) {
        return doComparison(lhs, rhs) > 0;
    }

    private static boolean lt(short lhs, short rhs) {
        return doComparison(lhs, rhs) < 0;
    }

    private static boolean leq(short lhs, short rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    private static boolean geq(short lhs, short rhs) {
        return doComparison(lhs, rhs) >= 0;
    }

    private static boolean eq(short lhs, short rhs) {
        // region equality function
        return lhs == rhs;
        // endregion equality function
    }
    //endregion

    @Override
    public long totalSize() { return totalSize; }

    @Override
    public int getNodeSize() {
        return leafSize;
    }

    @Override
    public Short getMin() {
        return getMinShort();
    }

    @Override
    public Short getMax() {
        return getMaxShort();
    }

    public short getMinShort() {
        if (leafCount == 0) {
            throw new IllegalStateException();
        }
        else if (leafCount == 1) {
            return directoryValues[0];
        }
        return leafValues[0][0];
    }

    @Override
    public long getMinCount() {
        if (leafCount == 0) {
            throw new IllegalStateException();
        }
        else if (leafCount == 1) {
            return directoryCount[0];
        }
        return leafCounts[0][0];
    }

    private void addMinCount(long toAdd) {
        if (leafCount == 0) {
            throw new IllegalStateException();
        }
        else if (leafCount == 1) {
            directoryCount[0] += toAdd;
        } else {
            leafCounts[0][0] += toAdd;
        }
        totalSize += toAdd;
    }

    private void removeMin() {
        if (size == 1) {
            clear();
            return;
        }

        if (leafCount == 1) {
            totalSize -= directoryCount[0];
            System.arraycopy(directoryValues, 1, directoryValues, 0, size - 1);
            System.arraycopy(directoryCount, 1, directoryCount, 0, size - 1);
        } else {
            totalSize -= leafCounts[0][0];
            System.arraycopy(leafValues[0], 1, leafValues[0], 0, leafSizes[0] - 1);
            System.arraycopy(leafCounts[0], 1, leafCounts[0], 0, leafSizes[0] - 1);
            leafSizes[0]--;
            if (leafSizes[0] == 0) {
                // we need to remove this leaf
                leafCount--;
                System.arraycopy(leafValues, 1, leafValues, 0, leafCount);
                System.arraycopy(leafCounts, 1, leafCounts, 0, leafCount);
                System.arraycopy(leafSizes, 1, leafSizes, 0, leafCount);
                System.arraycopy(directoryValues, 1, directoryValues, 0, leafCount - 1);
                maybePromoteLastLeaf();
            }
        }
        size--;
    }

    public short getMaxShort() {
        if (leafCount == 0) {
            throw new IllegalStateException();
        }
        else if (leafCount == 1) {
            return directoryValues[size - 1];
        }
        return leafValues[leafCount - 1][leafSizes[leafCount - 1] - 1];
    }

    @Override
    public long getMaxCount() {
        if (leafCount == 0) {
            throw new IllegalStateException();
        }
        else if (leafCount == 1) {
            return directoryCount[size - 1];
        }
        return leafCounts[leafCount - 1][leafSizes[leafCount - 1] - 1];
    }

    private void addMaxCount(long toAdd) {
        if (leafCount == 0) {
            throw new IllegalStateException();
        }
        else if (leafCount == 1) {
            directoryCount[size - 1] += toAdd;
        } else {
            leafCounts[leafCount - 1][leafSizes[leafCount - 1] - 1] += toAdd;
        }
        totalSize += toAdd;
    }

    private void removeMax() {
        if (size == 1) {
            clear();
            return;
        }
        if (leafCount > 1) {
            totalSize -= leafCounts[leafCount - 1][leafSizes[leafCount - 1] - 1];
            leafSizes[leafCount - 1]--;
            size--;
            if (leafSizes[leafCount - 1] == 0) {
                leafCount--;
                maybePromoteLastLeaf();
            }
        } else {
            totalSize -= directoryCount[size - 1];
            size--;
        }
    }

    //region Moving
    @Override
    public void moveFrontToBack(SegmentedSortedMultiSet untypedDestination, long count) {
        final ShortSegmentedSortedMultiset destination = (ShortSegmentedSortedMultiset)untypedDestination;
        validate();
        destination.validate();

        Assert.eq(leafSize, "leafSize", destination.leafSize, "destination.leafSize");
        Assert.gtZero(leafCount, "leafCount");

        if (count == 0) {
            return;
        }

        if (SEGMENTED_SORTED_MULTISET_VALIDATION) {
            if (destination.size > 0) {
                Assert.assertion(geq(getMinShort(), destination.getMaxShort()), "geq(getMinShort(), destination.getMaxShort())");
            }
        }

        if (destination.size > 0 && eq(getMinShort(), destination.getMaxShort())) {
            final long minCount = getMinCount();
            final long toAdd;
            if (minCount > count) {
                toAdd = count;
                addMinCount(-count);
            } else {
                toAdd = minCount;
                removeMin();
            }

            destination.addMaxCount(toAdd);
            count -= toAdd;
        }
        if (count == 0) {
            validate();
            destination.validate();
            return;
        }

        final MutableInt remaining = new MutableInt(count);
        final MutableInt leftOverMutable = new MutableInt();
        int totalUniqueToMove = 0;
        int partialUnique = 0;
        int rleaf = 0;
        if (leafCount == 1) {
            // we need to move this many entries (the last one may be partial)
            totalUniqueToMove = countFront(directoryCount, size, remaining, leftOverMutable);
            if (remaining.intValue() > 0) {
                throw new IllegalStateException();
            }
            if (totalUniqueToMove == size) {
                partialUnique = 0;
                rleaf = 1;
            } else {
                partialUnique = totalUniqueToMove;
            }
        } else {
            while (remaining.intValue() > 0) {
                final int uniqueToMove = countFront(leafCounts[rleaf], leafSizes[rleaf], remaining, leftOverMutable);
                totalUniqueToMove += uniqueToMove;
                if (uniqueToMove == leafSizes[rleaf]) {
                    rleaf++;
                } else {
                    partialUnique = uniqueToMove;
                }
            }
        }
        final boolean appendToExtra = destination.prepareAppend(partialUnique, rleaf);

        final int leftOver = leftOverMutable.intValue();
        if (rleaf > 0) {
            int wleaf = destination.leafCount;
            // we can move full leaves to start
            if (leafCount == 1) {
                Assert.eqZero(partialUnique, "partialUnique");
                if (wleaf > 0) {
                    destination.updateDirectory(wleaf - 1);
                }
                destination.leafValues[wleaf] = Arrays.copyOf(directoryValues, leafSize);
                destination.leafCounts[wleaf] = Arrays.copyOf(directoryCount, leafSize);
                destination.leafSizes[wleaf] = size;
                destination.size += size;
                if (leftOver > 0) {
                    directoryCount[0] = leftOver;
                    directoryValues[0] = directoryValues[size - 1];
                    destination.leafCounts[wleaf][destination.leafSizes[wleaf] - 1] -= leftOver;
                    Assert.gtZero(destination.leafCounts[wleaf][destination.leafSizes[wleaf] - 1], "destination.leafCounts[wleaf][destination.leafSizes[wleaf] - 1]");
                    size = 1;
                } else {
                    directoryValues = null;
                    directoryCount = null;
                    size = 0;
                }

                if (wleaf > 0 && destination.leafSizes[wleaf] + destination.leafSizes[wleaf - 1] <= leafSize) {
                    destination.mergeTwoLeavesBack(wleaf - 1, wleaf);
                } else {
                    wleaf++;
                }
                // we don't want to do the final copy
                rleaf = 0;
            } else {
                for (int rli = 0; rli < rleaf; ++rli) {
                    destination.leafValues[wleaf] = leafValues[rli];
                    destination.leafCounts[wleaf] = leafCounts[rli];
                    if (wleaf > 0) {
                        destination.updateDirectory(wleaf - 1);
                    }
                    destination.leafSizes[wleaf] = leafSizes[rli];
                    wleaf++;

                    destination.size += leafSizes[rli];

                    if (rli == rleaf - 1 && leftOver > 0 && partialUnique == 0) {
                        final int sizeOfLeftOverLeaf = leafSizes[rli];
                        size -= (sizeOfLeftOverLeaf - 1);

                        final short [] tmpValues = new short[leafSize];
                        final long [] tmpCounts = new long[leafSize];
                        tmpValues[0] = leafValues[rli][sizeOfLeftOverLeaf - 1];
                        tmpCounts[0] = leftOver;
                        leafValues[rli] = tmpValues;
                        leafCounts[rli] = tmpCounts;
                        leafSizes[rli] = 1;

                        destination.leafCounts[wleaf - 1][sizeOfLeftOverLeaf - 1] -= leftOver;
                        Assert.gtZero(destination.leafCounts[wleaf - 1][sizeOfLeftOverLeaf - 1], "destination.leafCounts[wleaf - 1][sizeOfLeftOverLeaf - 1]");
                        if (rli < leafCount - 1) {
                            updateDirectory(rli);
                        }
                    } else {
                        size -= leafSizes[rli];
                        leafSizes[rli] = 0;
                        leafValues[rli] = null;
                        leafCounts[rli] = null;
                    }

                    // if we can actually fit the last two leaves within a single leaf, take advantage of it
                    if (wleaf > 1 && destination.leafSizes[wleaf - 1] + destination.leafSizes[wleaf - 2] <= leafSize) {
                        destination.mergeTwoLeavesBack(wleaf - 2, wleaf - 1);
                        wleaf--;
                    }
                }
                if (destination.directoryValues.length >= wleaf) {
                    destination.updateDirectory(wleaf - 1);
                }
            }
            destination.leafCount = wleaf;
            if (partialUnique == 0) {
                destination.maybePromoteLastLeaf();
            }
        }

        boolean sourceLeavesMerged = false;
        if (partialUnique > 0) {
            final short [] sourceValues;
            final long [] sourceCounts;
            final short [] destinationValues;
            final long [] destinationCounts;
            final int copySize;
            final int destOffset;
            if (leafCount == 1) {
                sourceValues = directoryValues;
                sourceCounts = directoryCount;
                copySize = size;
            } else {
                sourceValues = leafValues[rleaf];
                sourceCounts = leafCounts[rleaf];
                copySize = leafSizes[rleaf];
            }
            assert sourceValues != null;
            assert sourceCounts != null;

            final int wleaf;

            if (appendToExtra) {
                wleaf = destination.leafCount;
                destinationValues = destination.leafValues[wleaf] = new short[leafSize];
                destinationCounts = destination.leafCounts[wleaf] = new long[leafSize];
                destOffset = 0;
                destination.leafSizes[wleaf] = partialUnique;
                destination.leafCount++;
            } else {
                if (destination.directoryCount == null) {
                    wleaf = destination.leafCount - 1;
                    destOffset = destination.leafSizes[wleaf];
                    destinationValues = destination.leafValues[wleaf];
                    destinationCounts = destination.leafCounts[wleaf];
                    destination.leafSizes[wleaf] += partialUnique;
                } else {
                    wleaf = -1;
                    destOffset = destination.size;
                    destinationValues = destination.directoryValues;
                    destinationCounts = destination.directoryCount;
                    destination.leafCount = 1;
                }
            }

            // copy from the final leaf to copy into the result
            System.arraycopy(sourceValues, 0, destinationValues, destOffset, partialUnique);
            System.arraycopy(sourceCounts, 0, destinationCounts, destOffset, partialUnique);
            // we are always in the last leaf, so no directory values to fix up
            destination.size += partialUnique;
            if (leftOver > 0) {
                if (destination.directoryCount == null) {
                    destination.leafCounts[wleaf][destOffset + partialUnique - 1] -= leftOver;
                    Assert.gtZero(destination.leafCounts[wleaf][destOffset + partialUnique - 1], "destination.leafCounts[wleaf][destOffset + partialUnique - 1]");
                } else {
                    destination.directoryCount[destination.size - 1] -= leftOver;
                    Assert.gtZero(destination.directoryCount[destination.size - 1], "destination.directoryCount[destination.size]");
                }
            }

            if (wleaf > destination.leafCount - 1) {
                destination.updateDirectory(wleaf);
            }

            final int leftOverSlot = leftOver > 0 ? 1 : 0;

            // compact the remaining leaf
            final int compactCopySize = copySize - partialUnique + leftOverSlot;
            System.arraycopy(sourceValues, partialUnique - leftOverSlot, sourceValues, 0, compactCopySize);
            System.arraycopy(sourceCounts, partialUnique - leftOverSlot, sourceCounts, 0, compactCopySize);
            if (leftOver > 0) {
                sourceCounts[0] = leftOver;
            }

            final int sizeChange = partialUnique - leftOverSlot;
            size -= sizeChange;
            if (leafCount > 1) {
                leafSizes[rleaf] -= sizeChange;
            }

            // possibly merge source
            if (rleaf < leafCount - 1 && leafSizes[rleaf] + leafSizes[rleaf + 1] < leafSize) {
                mergeTwoLeavesForward(rleaf, rleaf + 1);
                sourceLeavesMerged = true;
            }

            // if we can actually fit the last two leaves within a single leaf, take advantage of it
            if (wleaf >= 1 && destination.leafSizes[wleaf] + destination.leafSizes[wleaf - 1] <= leafSize) {
                destination.mergeTwoLeavesBack(wleaf - 1, wleaf);
                destination.leafCount--;
                destination.maybePromoteLastLeaf();
            }
        }

        if ((sourceLeavesMerged || rleaf > 0) && size > 0) {
            final int copyStart;
            final int copyCount;

            if (partialUnique == 0 && leftOver > 0) {
                // we have to deal with the leaf that still contains leftovers
                copyStart = rleaf - 1;
            } else {
                // we have to deal with the leaf that still contains leftovers
                final int mergedLeafCount = sourceLeavesMerged ? 1 : 0;
                copyStart = rleaf + mergedLeafCount;
            }
            copyCount = leafCount - copyStart;

            if (copyCount > 0) {
                System.arraycopy(leafValues, copyStart, leafValues, 0, copyCount);
                System.arraycopy(leafCounts, copyStart, leafCounts, 0, copyCount);
                System.arraycopy(leafSizes, copyStart, leafSizes, 0, copyCount);
                if (copyCount > 1) {
                    System.arraycopy(directoryValues, copyStart, directoryValues, 0, copyCount - 1);
                }
            }

            leafCount -= copyStart;
            maybePromoteLastLeaf();
        }

        totalSize -= count;
        destination.totalSize += count;

        if (size == 0) {
            clear();
        }

        if (SEGMENTED_SORTED_MULTISET_VALIDATION) {
            if (size > 0 && destination.size > 0) {
                Assert.assertion(geq(getMinShort(), destination.getMaxShort()), "geq(getMinShort(), destination.getMaxShort())");
            }
        }

        validate();
        destination.validate();
    }

    private void updateDirectory(int leaf) {
        directoryValues[leaf] = leafValues[leaf][leafSizes[leaf] - 1];
    }

    /**
     *
     * @param finalSlots how many slots outside of completeLeaves are required
     * @param completeLeaves how many complete leaves are required
     *
     * @return true if we should put our finalSlots values in the "extra" leaf.  False if they should be appended to the last leaf that already exists
     */
    private boolean prepareAppend(int finalSlots, int completeLeaves) {
        Assert.leq(finalSlots, "finalSlots", leafSize, "leafSize");
        if (completeLeaves == 0) {
            // we are only going to append to the last leaf
            if (leafCount == 0) {
                directoryValues = new short[finalSlots];
                directoryCount = new long[finalSlots];
                return false;
            } else if (leafCount == 1) {
                if (size + finalSlots <= leafSize) {
                    directoryValues = Arrays.copyOf(directoryValues, finalSlots + size);
                    directoryCount = Arrays.copyOf(directoryCount, finalSlots + size);
                    return false;
                }
                moveDirectoryToLeaf(2);
                updateDirectory(0);
                return true;
            }
            if (finalSlots + leafSizes[leafCount - 1] > leafSize) {
                reallocateLeafArrays(leafCount + 1);
                updateDirectory(leafCount - 1);
                return true;
            }
            return false;
        } else {
            // we are going to add leaves, then a final partial leaf
            final boolean extraLeaf = finalSlots > 0;
            final int extraLeafCount = extraLeaf ? 1 : 0;
            if (leafCount == 0) {
                allocateLeafArrays(completeLeaves + extraLeafCount);
            } else if (leafCount == 1) {
                moveDirectoryToLeaf(1 + completeLeaves + extraLeafCount);
            } else {
                reallocateLeafArrays(leafCount + completeLeaves + extraLeafCount);
            }
            if (extraLeaf) {
                leafValues[leafCount + completeLeaves] = new short[leafSize];
                leafCounts[leafCount + completeLeaves] = new long[leafSize];
            }
            return extraLeaf;
        }
    }

    /**
     * Prepare the SSM for prepending values to it.
     *
     * @param initialSlots how many slots outside of a complete leaf will be prepended
     * @param completeLeaves how many complete leaves will be prepended
     *
     * @return true if the initialSlots values should be copied into their own private leaf, false if they should share space
     * with the next leaf
     */
    private boolean preparePrepend(int initialSlots, int completeLeaves) {
        final int extraLeafCount;
        if (completeLeaves > 0) {
            final boolean extraLeaf = initialSlots > 0;
            extraLeafCount = extraLeaf ? 1 : 0;
            if (leafCount == 0) {
                allocateLeafArrays(completeLeaves + extraLeafCount);
            } else if (leafCount == 1) {
                moveDirectoryToLeaf(completeLeaves + 1 + extraLeafCount, completeLeaves + extraLeafCount);
            } else {
                reallocateLeafArrays(leafCount + completeLeaves + extraLeafCount);
                makeLeafHole(0, completeLeaves + extraLeafCount);
            }
        } else {
            // we only have the partial leaf
            Assert.gtZero(initialSlots, "initialSlots");
            if (leafCount == 0) {
                Assert.leq(initialSlots, "initialSlots", leafSize, "leafSize");
                extraLeafCount = 1;
                directoryValues = new short[initialSlots];
                directoryCount = new long[initialSlots];
            } else if (leafCount == 1) {
                final boolean extraLeaf = initialSlots + size > leafSize;
                extraLeafCount = extraLeaf ? 1 : 0;
                if (extraLeaf) {
                    moveDirectoryToLeaf(2, 1);
                }
            } else {
                final boolean extraLeaf = initialSlots + leafSizes[0] > leafSize;
                extraLeafCount = extraLeaf ? 1 : 0;
                if (extraLeaf) {
                    makeLeafHole(0, 1);
                }
            }
        }

        final int targetLeaf = completeLeaves + extraLeafCount;
        leafCount += targetLeaf;
        if (extraLeafCount == 0 && initialSlots > 0) {
            // make a hole in the first leaf that still has values
            if (directoryCount != null) {
                final short [] tmpValues = new short[initialSlots + size];
                final long [] tmpCount = new long[initialSlots + size];
                System.arraycopy(directoryValues, 0, tmpValues, initialSlots, size);
                System.arraycopy(directoryCount, 0, tmpCount, initialSlots, size);
                directoryValues = tmpValues;
                directoryCount = tmpCount;
            } else {
                final int copySize = leafSizes[targetLeaf];
                System.arraycopy(leafValues[targetLeaf], 0, leafValues[targetLeaf], initialSlots, copySize);
                System.arraycopy(leafCounts[targetLeaf], 0, leafCounts[targetLeaf], initialSlots, copySize);
            }
        }

        return extraLeafCount > 0;
    }

    private static int countFront(long[] counts, int sz, MutableInt valuesToMove, MutableInt leftOvers) {
        leftOvers.setValue(0);
        int rpos = 0;
        // figure out how many values we must move
        while (valuesToMove.intValue() > 0 && rpos < sz) {
            final long slotCount = counts[rpos];
            if (valuesToMove.intValue() < slotCount) {
                leftOvers.setValue(slotCount - valuesToMove.intValue());
                valuesToMove.setValue(0);
            } else {
                valuesToMove.subtract(slotCount);
            }
            rpos++;
        }
        return rpos;
    }

    @Override
    public void moveBackToFront(SegmentedSortedMultiSet untypedDestination, long count) {
        final ShortSegmentedSortedMultiset destination = (ShortSegmentedSortedMultiset)untypedDestination;
        validate();
        destination.validate();

        if (count == 0) {
            return;
        }

        Assert.eq(leafSize, "leafSize", destination.leafSize, "destination.leafSize");
        Assert.gtZero(leafCount, "leafCount");

        if (SEGMENTED_SORTED_MULTISET_VALIDATION) {
            if (destination.size > 0) {
                Assert.assertion(leq(getMaxShort(), destination.getMinShort()), "leq(getMaxShort(), destination.getMinShort())");
            }
        }

        if (destination.size > 0 && eq(getMaxShort(), destination.getMinShort())) {
            final long maxCount = getMaxCount();
            final long toAdd;
            if (maxCount > count) {
                toAdd = count;
                addMaxCount(-count);
            } else {
                toAdd = maxCount;
                removeMax();
            }

            destination.addMinCount(toAdd);
            count -= toAdd;
        }

        if (count == 0) {
            return;
        }

        final MutableInt remaining = new MutableInt(count);
        final MutableInt leftOverMutable = new MutableInt();
        int totalUniqueToMove = 0;
        int slotsInPartialLeaf = 0;
        int completeLeavesToMove = 0;
        int rleaf = leafCount - 1;
        if (leafCount == 1) {
            // we need to move this many entries (the last one may be partial)
            totalUniqueToMove = countBack(directoryCount, size, remaining, leftOverMutable);
            Assert.eqZero(remaining.intValue(), "remaining.intValue()");
            Assert.leq(totalUniqueToMove, "totalUniqueToMove", count, "count");
            if (totalUniqueToMove == size && remaining.intValue() == 0) {
                // we are moving the entire leaf
                completeLeavesToMove = 1;
                slotsInPartialLeaf = 0;
            } else {
                completeLeavesToMove = 0;
                slotsInPartialLeaf = totalUniqueToMove;
            }
        } else {
            while (remaining.intValue() > 0) {
                final int uniqueToMove = countBack(leafCounts[rleaf], leafSizes[rleaf], remaining, leftOverMutable);
                Assert.leq(totalUniqueToMove, "totalUniqueToMove", count, "count");
                totalUniqueToMove += uniqueToMove;
                if (uniqueToMove == leafSizes[rleaf]) {
                    rleaf--;
                    completeLeavesToMove++;
                } else {
                    slotsInPartialLeaf = uniqueToMove;
                }
            }
        }

        final int leftOver = leftOverMutable.intValue();

        final boolean extraLeaf = destination.preparePrepend(slotsInPartialLeaf, completeLeavesToMove);
        if (slotsInPartialLeaf > 0) {
            final boolean leftOverExists = leftOver > 0;
            final short [] destValues;
            final long [] destCounts;

            final short [] srcValues;
            final long [] srcCounts;
            final int srcSize;

            if (destination.directoryCount != null) {
                destValues = destination.directoryValues;
                destCounts = destination.directoryCount;
            } else {
                if (extraLeaf) {
                    destination.leafValues[0] = new short[leafSize];
                    destination.leafCounts[0] = new long[leafSize];
                }
                destValues = destination.leafValues[0];
                destCounts = destination.leafCounts[0];
            }

            if (leafCount == 1) {
                srcValues = directoryValues;
                srcCounts = directoryCount;
                srcSize = size;
            } else {
                srcValues = leafValues[rleaf];
                srcCounts = leafCounts[rleaf];
                srcSize = leafSizes[rleaf];
            }
            final int srcOffset = srcSize - slotsInPartialLeaf;

            System.arraycopy(srcValues, srcOffset, destValues, 0, slotsInPartialLeaf);
            System.arraycopy(srcCounts, srcOffset, destCounts, 0, slotsInPartialLeaf);

            final int sizeChange = slotsInPartialLeaf + (leftOverExists ? -1 : 0);
            size -= sizeChange;
            destination.size += slotsInPartialLeaf;
            if (destination.directoryCount == null) {
                destination.leafSizes[0] += slotsInPartialLeaf;
                if (destination.leafCount > 1) {
                    destination.updateDirectory(0);
                }
            }

            if (leafCount > 1) {
                leafSizes[rleaf] -= sizeChange;
            }
            if (leftOverExists) {
                destCounts[0] -= leftOver;
                srcCounts[srcOffset] = leftOver;
            }
        }

        // now mass move a bunch of leaves over
        if (completeLeavesToMove > 0) {
            if (leafCount == 1) {
                Assert.eqZero(slotsInPartialLeaf, "slotsInPartialLeaf");
                destination.leafValues[0] = Arrays.copyOf(directoryValues, leafSize);
                destination.leafCounts[0] = Arrays.copyOf(directoryCount, leafSize);
                destination.size += size;
                destination.leafSizes[0] = size;
                if (destination.leafCount > 1) {
                    destination.updateDirectory(0);
                }
                if (leftOver > 0) {
                    destination.leafCounts[0][0] -= leftOver;
                    directoryCount[0] = leftOver;
                    size = 1;
                } else {
                    size = 0;
                }
            } else {
                final int destinationLeaf = slotsInPartialLeaf > 0 ? 1 : 0;
                System.arraycopy(leafValues, rleaf + 1, destination.leafValues, destinationLeaf, completeLeavesToMove);
                System.arraycopy(leafCounts, rleaf + 1, destination.leafCounts, destinationLeaf, completeLeavesToMove);
                System.arraycopy(leafSizes, rleaf + 1, destination.leafSizes, destinationLeaf, completeLeavesToMove);
                final int directoryMoves;
                final boolean haveLastSourceDirectoryEntry = rleaf + 1 + completeLeavesToMove < leafCount - 1;
                final boolean requireLastDestinationDirectoryEntry = destination.leafCount > (destinationLeaf + completeLeavesToMove);
                if (haveLastSourceDirectoryEntry && requireLastDestinationDirectoryEntry) {
                    directoryMoves = completeLeavesToMove;
                } else {
                    directoryMoves = completeLeavesToMove - 1;
                }
                if (directoryMoves > 0) {
                    System.arraycopy(directoryValues, rleaf + 1, destination.directoryValues, destinationLeaf, directoryMoves);
                }
                if (requireLastDestinationDirectoryEntry) {
                    destination.updateDirectory(destinationLeaf + completeLeavesToMove - 1);
                }

                final boolean hasLeftOverSlot = leftOver > 0 && slotsInPartialLeaf == 0;
                if (hasLeftOverSlot) {
                    // fixup the destination, we must have a destinationLeaf of 0
                    Assert.eqZero(destinationLeaf, "destinationLeaf");
                    destination.leafCounts[0][0] -= leftOver;
                }

                final int numberOfLeavesToRemove = hasLeftOverSlot ? completeLeavesToMove - 1 : completeLeavesToMove;
                leafCount -= numberOfLeavesToRemove;

                if (hasLeftOverSlot) {
                    // we need to copy the array, so that it is not aliased to two different nodes
                    leafCounts[rleaf + 1] = new long[leafSize];
                    leafValues[rleaf + 1] = new short[leafSize];
                    leafValues[rleaf + 1][0] = destination.leafValues[0][0];
                    leafCounts[rleaf + 1][0] = leftOver;
                    leafSizes[rleaf + 1] = 1;
                    // we'll take it away in the loop below, so we need to preserve it here
                    size++;
                }

                for (int ii = 0; ii < completeLeavesToMove; ++ii) {
                    size -= destination.leafSizes[destinationLeaf + ii];
                    destination.size += destination.leafSizes[destinationLeaf + ii];
                }
                final int firstLeafTozero = hasLeftOverSlot ? rleaf + 2 : rleaf + 1;
                Arrays.fill(leafValues, firstLeafTozero, firstLeafTozero + numberOfLeavesToRemove, null);
                Arrays.fill(leafCounts, firstLeafTozero, firstLeafTozero + numberOfLeavesToRemove, null);
                Arrays.fill(leafSizes, firstLeafTozero , firstLeafTozero + numberOfLeavesToRemove, 0);
                if (directoryMoves > 0) {
                    Arrays.fill(directoryValues, firstLeafTozero, firstLeafTozero + directoryMoves - (completeLeavesToMove - numberOfLeavesToRemove), NULL_SHORT);
                }
                maybePromoteLastLeaf();
            }
            destination.maybePromoteLastLeaf();
        }

        totalSize -= count;
        destination.totalSize += count;

        if (size == 0) {
            clear();
        }

        validate();
        destination.validate();

        if (SEGMENTED_SORTED_MULTISET_VALIDATION) {
            if (size > 0 && destination.size > 0) {
                Assert.assertion(leq(getMaxShort(), destination.getMinShort()), "leq(getMaxShort(), destination.getMinShort())");
            }
        }
    }

    private static int countBack(long[] counts, int sz, MutableInt valuesToMove, MutableInt leftOvers) {
        leftOvers.setValue(0);
        int rpos = sz;
        // figure out how many values we must move
        while (valuesToMove.intValue() > 0 && rpos > 0) {
            final long slotCount = counts[--rpos];
            if (valuesToMove.intValue() < slotCount) {
                leftOvers.setValue(slotCount - valuesToMove.intValue());
                valuesToMove.setValue(0);
            } else {
                valuesToMove.subtract(slotCount);
            }
        }
        return sz - rpos;
    }
    //endregion

    @Override
    public WritableShortChunk<?> keyChunk() {
        final WritableShortChunk<?> keyChunk = WritableShortChunk.makeWritableChunk(intSize());
        fillKeyChunk(keyChunk, 0);
        return keyChunk;
    }

    @Override
    public void fillKeyChunk(WritableChunk<?> keyChunk, int offset) {
        fillKeyChunk(keyChunk.asWritableShortChunk(), offset);
    }

    private void fillKeyChunk(WritableShortChunk<?> keyChunk, int offset) {
        if(keyChunk.capacity() < offset + intSize()) {
            throw new IllegalArgumentException("Input chunk is not large enough");
        }

        if (leafCount == 1) {
            keyChunk.copyFromTypedArray(directoryValues, 0, offset, size);
        } else if (leafCount > 0) {
            int destOffset = 0;
            for (int li = 0; li < leafCount; ++li) {
                keyChunk.copyFromTypedArray(leafValues[li], 0, offset + destOffset, leafSizes[li]);
                destOffset += leafSizes[li];
            }
        }
    }

    @Override
    public WritableLongChunk<?> countChunk() {
        final WritableLongChunk<Any> countChunk = WritableLongChunk.makeWritableChunk(intSize());
        if (leafCount == 1) {
            countChunk.copyFromTypedArray(directoryCount, 0, 0, size);
        } else if (leafCount > 0) {
            int offset = 0;
            for (int li = 0; li < leafCount; ++li) {
                countChunk.copyFromTypedArray(leafCounts[li], 0, offset, leafSizes[li]);
                offset += leafSizes[li];
            }
        }
        return countChunk;
    }

    private short[] keyArray() {
        return keyArray(0, size-1);
    }

    /**
     * Create an array of the current keys beginning with the first (inclusive) and ending with the last (inclusive)
     * @param first
     * @param last
     * @return
     */
    private short[] keyArray(long first, long last) {
        if(isEmpty()) {
            return ArrayTypeUtils.EMPTY_SHORT_ARRAY;
        }

        final int totalSize = (int)(last - first + 1);
        final short[] keyArray = new short[totalSize];
        if (leafCount == 1) {
            System.arraycopy(directoryValues, (int)first, keyArray, 0, totalSize);
        } else if (leafCount > 0) {
            int offset = 0;
            int copied = 0;
            int skipped = 0;
            for (int li = 0; li < leafCount && copied < totalSize; ++li) {
                if(skipped < first) {
                    final int toSkip = (int)first - skipped;
                    if(toSkip < leafSizes[li]) {
                        final int nToCopy = Math.min(leafSizes[li] - toSkip, totalSize);
                        System.arraycopy(leafValues[li], toSkip, keyArray, 0, nToCopy);
                        copied = nToCopy;
                        offset = copied;
                        skipped = (int)first;
                    } else {
                        skipped += leafSizes[li];
                    }
                } else {
                    int nToCopy = Math.min(leafSizes[li], totalSize - copied);
                    System.arraycopy(leafValues[li], 0, keyArray, offset, nToCopy);
                    offset += leafSizes[li];
                    copied += nToCopy;
                }
            }
        }
        return keyArray;
    }

    // region Delta Management
    private void maybeAccumulateAdditions(WritableShortChunk<? extends Values> valuesToInsert) {
        if (!accumulateDeltas || valuesToInsert.size() == 0) {
            return;
        }

        if(prevValues == null) {
            prevValues = new ShortVectorDirect(keyArray());
        }

        if (added == null) {
            added = new TShortHashSet(valuesToInsert.size());
        }

        if(removed == null) {
            for (int ii = 0; ii < valuesToInsert.size(); ii++) {
                added.add(valuesToInsert.get(ii));
            }
        } else {
            for (int ii = 0; ii < valuesToInsert.size(); ii++) {
                short val = valuesToInsert.get(ii);
                // Only add to the 'added' set if it was not removed before.
                // if it was then this key is a net-no-change.
                if (!removed.remove(val)) {
                    added.add(val);
                }
            }
        }
    }

    private void maybeAccumulateRemoval(short valueRemoved) {
        if(!accumulateDeltas) {
            return;
        }

        if(prevValues == null) {
            prevValues = new ShortVectorDirect(keyArray());
        }

        if(removed == null) {
            removed = new TShortHashSet();
        }

        if(added == null || !added.remove(valueRemoved)) {
            removed.add(valueRemoved);
        }
    }

    @Override
    public void setTrackDeltas(boolean shouldTrackDeltas) {
        this.accumulateDeltas = shouldTrackDeltas;
    }

    @Override
    public void clearDeltas() {
        added = removed = null;
        prevValues = null;
    }

    @Override
    public int getAddedSize() {
        return added == null ? 0 : added.size();
    }

    @Override
    public int getRemovedSize() {
        return removed == null ? 0 : removed.size();
    }

    public void fillRemovedChunk(WritableShortChunk<? extends Values> chunk, int position) {
        chunk.copyFromTypedArray(removed.toArray(), 0, position, removed.size());
    }

    public void fillAddedChunk(WritableShortChunk<? extends Values> chunk, int position) {
        chunk.copyFromTypedArray(added.toArray(), 0, position, added.size());
    }

    public ShortVector getPrevValues() {
        return prevValues == null ? this : prevValues;
    }
    // endregion

    // region ShortVector
    @Override
    public short get(long i) {
        if(i < 0 || i > size()) {
            throw new IllegalArgumentException("Illegal index " + i + " current size: " + size());
        }

        if(leafCount == 1) {
            return directoryValues[(int)i];
        } else {
            for(int ii = 0; ii < leafCount; ii++) {
                if(i < leafSizes[ii]) {
                    return leafValues[ii][(int)(i)];
                }
                i -= leafSizes[ii];
            }
        }

        throw new IllegalStateException("Index " + i + " not found in this SSM");
    }

    @Override
    public ShortVector subVector(long fromIndex, long toIndex) {
        return new ShortVectorDirect(keyArray(fromIndex, toIndex));
    }

    @Override
    public ShortVector subVectorByPositions(long[] positions) {
        final short[] keyArray = new short[positions.length];
        int writePos = 0;
        for (long position : positions) {
            keyArray[writePos++] = get(position);
        }

        return new ShortVectorDirect(keyArray);
    }

    @Override
    public short[] toArray() {
        return keyArray();
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public ShortVector getDirect() {
        return new ShortVectorDirect(keyArray());
    }
    //endregion

    //region VectorEquals
    private boolean equalsArray(ShortVector o) {
        if(size() != o.size()) {
            return false;
        }

        if(leafCount == 1) {
            for(int ii = 0; ii < size; ii++) {
                //region DirObjectEquals
                if(directoryValues[ii] != o.get(ii)) {
                    return false;
                }
                //endregion DirObjectEquals
            }

            return true;
        }

        int nCompared = 0;
        for (int li = 0; li < leafCount; ++li) {
            for(int ai = 0; ai < leafSizes[li]; ai++) {
                if(leafValues[li][ai] != o.get(nCompared++)) {
                    return false;
                }
            }
        }

        return true;
    }
    //endregion VectorEquals

    private boolean equalsArray(ObjectVector<?> o) {
        //region EqualsArrayTypeCheck
        if(o.getComponentType() != short.class && o.getComponentType() != Short.class) {
            return false;
        }
        //endregion EqualsArrayTypeCheck

        if(size() != o.size()) {
            return false;
        }

        if(leafCount == 1) {
            for(int ii = 0; ii < size; ii++) {
                final Short val = (Short)o.get(ii);
                //region VectorEquals
                if(directoryValues[ii] == NULL_SHORT && val != null && val != NULL_SHORT) {
                    return false;
                }
                //endregion VectorEquals

                if(!Objects.equals(directoryValues[ii], val)) {
                    return false;
                }
            }

            return true;
        }

        int nCompared = 0;
        for (int li = 0; li < leafCount; ++li) {
            for(int ai = 0; ai < leafSizes[li]; ai++) {
                final Short val = (Short)o.get(nCompared++);
                //region VectorEquals
                if(leafValues[li][ai] == NULL_SHORT && val != null && val != NULL_SHORT) {
                    return false;
                }
                //endregion VectorEquals

                if(!Objects.equals(leafValues[li][ai],  val)) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ShortSegmentedSortedMultiset)) {
            //region VectorEquals
            if(o instanceof ShortVector) {
                return equalsArray((ShortVector)o);
            }
            //endregion VectorEquals

            if(o instanceof ObjectVector) {
                return equalsArray((ObjectVector)o);
            }
            return false;
        }
        final ShortSegmentedSortedMultiset that = (ShortSegmentedSortedMultiset) o;

        if(size() != that.size()) {
            return false;
        }

        if(leafCount == 1) {
            if(that.leafCount != 1 || size != that.size) {
                return false;
            }

            for(int ii = 0; ii < size; ii++) {
                //region DirObjectEquals
                if(directoryValues[ii] != that.directoryValues[ii]) {
                    return false;
                }
                //endregion DirObjectEquals
            }

            return true;
        }

        int otherLeaf = 0;
        int otherLeafIdx = 0;
        for (int li = 0; li < leafCount; ++li) {
            for(int ai = 0; ai < leafSizes[li]; ai++) {
                //region LeafObjectEquals
                if(leafValues[li][ai] != that.leafValues[otherLeaf][otherLeafIdx++]) {
                    return false;
                }
                //endregion LeafObjectEquals

                if(otherLeafIdx >= that.leafSizes[otherLeaf]) {
                    otherLeaf++;
                    otherLeafIdx = 0;
                }

                if(otherLeaf >= that.leafCount) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        if(leafCount == 1) {
            int result = Objects.hash(size);
            for(int ii = 0; ii < size; ii++) {
                result = result * 31 + Objects.hash(directoryValues[ii]);
            }

            return result;
        }

        int result = Objects.hash(leafCount, size);

        for (int li = 0; li < leafCount; ++li) {
            for(int ai = 0; ai < leafSizes[li]; ai++) {
                result = result * 31 + Objects.hash(leafValues[li][ai]);
            }
        }

        return result;
    }

    @Override
    public String toString() {
        if (leafCount == 1) {
            return ArrayTypeUtils.toString(directoryValues, 0, intSize());
        } else if (leafCount > 0) {
            StringBuilder arrAsString = new StringBuilder("[");
            for (int li = 0; li < leafCount; ++li) {
                for(int ai = 0; ai < leafSizes[li]; ai++) {
                    arrAsString.append(leafValues[li][ai]).append(", ");
                }
            }

            arrAsString.replace(arrAsString.length() - 2, arrAsString.length(), "]");
            return arrAsString.toString();
        }

        return "[]";
    }

    // region Extensions
    // endregion Extensions
}
