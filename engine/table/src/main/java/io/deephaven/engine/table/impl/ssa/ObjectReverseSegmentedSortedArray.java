/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSegmentedSortedArray and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.ssa;

import java.util.Objects;

import java.util.Objects;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.sort.timsort.TimsortUtils;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.*;
import io.deephaven.util.annotations.VisibleForTesting;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.function.LongConsumer;

/**
 * For keeping track of incremental states of sorted values, we would ideally like to hold them in an Array or a Chunk;
 * with parallel row keys.  However, if we just put them in an array we can not insert or remove values without
 * unnecessarily shifting everything.
 *
 * The segmented array allows us to either insert or remove elements and only shift values in a "leaf" block and
 * possibly a "directory" block.  It can be thought of as similar to a single-level b+ tree with only keys.
 *
 * We must be totally ordered, which is accomplished by sorting on the Object values, and then on the corresponding
 * row key.
 */
public final class ObjectReverseSegmentedSortedArray implements SegmentedSortedArray {
    final private int leafSize;
    private int leafCount;
    private int size;

    private int minGallop = TimsortUtils.INITIAL_GALLOP;

    /**
     * If we have only a single leaf, then we use the directory arrays for the leaf values, otherwise we use it to
     * track the largest value in a given leaf.  The values are valid for 0 ... leafCount - 2, because the last leaf
     * must accept any value that is greater than the second to last leave's maximum.
     */
    private Object [] directoryValues;
    private long [] directoryRowKeys;

    private int [] leafSizes;
    private Object [][] leafValues;
    private long [][] leafRowKeys;

    /**
     * Create a ObjectReverseSegmentedSortedArray with the given leafSize.
     *
     * @param leafSize the maximumSize for any leaf
     */
    public ObjectReverseSegmentedSortedArray(int leafSize) {
        this.leafSize = leafSize;
        leafCount = 0;
        size = 0;
    }

    @Override
    public void insert(Chunk<? extends Any> valuesToInsert, LongChunk<? extends RowKeys> rowKeysToInsert) {
        insert(valuesToInsert.asObjectChunk(), rowKeysToInsert);
    }

    @Override
    public <T extends Any> int insertAndGetNextValue(Chunk<T> valuesToInsert, LongChunk<? extends RowKeys> rowKeysToInsert, WritableChunk<T> nextValue) {
        insert(valuesToInsert.asObjectChunk(), rowKeysToInsert);
        // TODO: Integrate this into insert, so we do not need to do a double binary search
        return findNext(valuesToInsert.asObjectChunk(), rowKeysToInsert, nextValue.asWritableObjectChunk());
    }

    /**
     * Find the next value for each stamp.
     *
     * @param stampValues  the stamp values to search for (must be sorted, with ties broken by the row key)
     * @param stampRowKeys the stamp rowKeys to search for (parallel to stampValues)
     * @param nextValues    the next value after a given stamp
     * @param <T>          the type of our chunks
     * @return how many next values we found (the last value has no next if less than stampValues.size())
     */
    private <T extends Any> int findNext(ObjectChunk<Object, T> stampValues, LongChunk<? extends RowKeys> stampRowKeys, WritableObjectChunk<Object, T> nextValues) {
        if (stampValues.size() == 0) {
            return 0;
        }

        if (leafCount == 0) {
            throw new IllegalArgumentException("No values to find.");
        }

        if (leafCount == 1) {
            return findNextOneLeaf(0, stampValues, stampRowKeys, nextValues, size, directoryValues, directoryRowKeys);
        }

        int stampsFound = 0;
        int currentLeaf = 0;
        while (stampsFound < stampValues.size()) {
            if (currentLeaf >= leafCount) {
                break;
            }
            final Object searchValue = stampValues.get(stampsFound);
            final long searchKey = stampRowKeys.get(stampsFound);
            // we need to check the last value in the leaf
            if (leafRowKeys[currentLeaf][leafSizes[currentLeaf] - 1] == searchKey) {
                if (currentLeaf == leafCount - 1) {
                    return stampsFound;
                }
                nextValues.set(stampsFound, leafValues[currentLeaf + 1][0]);
                stampsFound++;
                continue;
            }

            currentLeaf = bound(directoryValues, directoryRowKeys, searchValue, searchKey, currentLeaf, leafCount);
            final int found = findNextOneLeaf(stampsFound, stampValues, stampRowKeys, nextValues, leafSizes[currentLeaf], leafValues[currentLeaf], leafRowKeys[currentLeaf]);
            stampsFound += found;
        }

        return stampsFound;
    }

    private static <T extends Any> int findNextOneLeaf(int offset, ObjectChunk<Object, T> stampValues, LongChunk<? extends RowKeys> stampRowKeys, WritableObjectChunk<Object, T> nextValues, int leafSize, Object [] leafValues, long [] leafKeys) {
        int lo = 0;

        for (int ii = offset; ii < stampValues.size(); ++ii) {
            final Object searchValue = stampValues.get(ii);
            final long searchKey = stampRowKeys.get(ii);

            lo = bound(leafValues, leafKeys, searchValue, searchKey, lo, leafSize);

            if (lo < leafSize - 1) {
                nextValues.set(ii, leafValues[lo + 1]);
            } else {
                // if lo == leafSize - 1 it is the caller's responsibility to use the first value of the next leaf
                return ii - offset;
            }
        }

        return stampValues.size() - offset;
    }

    /**
     * Insert new valuesToInsert into this SSA.  The valuesToInsert to insert must be sorted.
     *
     * @param valuesToInsert the valuesToInsert to insert (must be sorted, with ties broken by the row key)
     * @param rowKeysToInsert the corresponding rowKeysToInsert
     */
    void insert(ObjectChunk<Object, ? extends Any> valuesToInsert, LongChunk<? extends RowKeys> rowKeysToInsert) {
        final int insertSize = valuesToInsert.size();
        validate();

        if (insertSize == 0) {
            return;
        }

        if (leafCount == 0) {
            // we are creating something brand new
            makeLeavesInitial(valuesToInsert, rowKeysToInsert);
        } else if (leafCount == 1) {
            final int newSize = insertSize + size;
            if (newSize <= leafSize) {
                if (directoryValues.length < newSize) {
                    directoryValues = Arrays.copyOf(directoryValues, Math.min(leafSize, newSize * 2));
                    directoryRowKeys = Arrays.copyOf(directoryRowKeys, Math.min(leafSize, newSize * 2));
                }
                insertIntoLeaf(size, directoryValues, valuesToInsert, directoryRowKeys, rowKeysToInsert);
                validateLeaf(directoryValues, directoryRowKeys, insertSize + size);
            } else {
                // we must split the leaf
                final int newLeafCount = getDesiredLeafCount(newSize);
                promoteDirectory(newLeafCount);
                distributeValues(newSize / newLeafCount + 1, 0, newLeafCount, valuesToInsert, rowKeysToInsert);
                for (int ii = 0; ii < newLeafCount; ++ii) {
                    validateLeaf(ii);
                }
                validateLeafOrdering(0, newLeafCount - 1);
            }
        } else {
            try (final ResettableObjectChunk<Object, Any> leafValuesInsertChunk = ResettableObjectChunk.makeResettableChunk();
                 final ResettableLongChunk<RowKeys> leafKeysInsertChunk = ResettableLongChunk.makeResettableChunk()) {
                int firstValuesPosition = 0;
                int totalCount = 0;
                while (firstValuesPosition < insertSize) {
                    // we need to find out where our valuesToInsert should go using a binary search of the directory
                    final int firstLeaf = Math.min(bound(directoryValues, directoryRowKeys, valuesToInsert.get(firstValuesPosition), rowKeysToInsert.get(firstValuesPosition), 0, leafCount), leafCount - 1);

                    final int lastValueForLeaf;
                    if (firstLeaf == leafCount - 1) {
                        lastValueForLeaf = insertSize - 1;
                    } else {
                        final Object leafMaxValue = leafValues[firstLeaf + 1][0];
                        final long leafMaxRowKey = leafRowKeys[firstLeaf + 1][0];
                        lastValueForLeaf = lowerBound(valuesToInsert, rowKeysToInsert, firstValuesPosition, insertSize, leafMaxValue, leafMaxRowKey);
                    }


                    final int count = lastValueForLeaf - firstValuesPosition + 1;

                    totalCount += count;

                    leafValuesInsertChunk.resetFromTypedChunk(valuesToInsert, firstValuesPosition, count);
                    leafKeysInsertChunk.resetFromTypedChunk(rowKeysToInsert, firstValuesPosition, count);

                    final int sizeForThisLeaf = count + leafSizes[firstLeaf];
                    if (sizeForThisLeaf <= leafSize) {
                        insertIntoLeaf(leafSizes[firstLeaf], leafValues[firstLeaf], leafValuesInsertChunk, leafRowKeys[firstLeaf], leafKeysInsertChunk);
                        leafSizes[firstLeaf] += count;
                        directoryValues[firstLeaf] = leafValues[firstLeaf][leafSizes[firstLeaf] - 1];
                        directoryRowKeys[firstLeaf] = leafRowKeys[firstLeaf][leafSizes[firstLeaf] - 1];
                        validateLeafRange(firstLeaf, 1);
                    } else {
                        // else make an appropriate sized hole
                        final int newLeafCount = getDesiredLeafCount(sizeForThisLeaf);
                        final boolean isLastLeaf = firstLeaf == leafCount - 1;
                        makeLeafHole(firstLeaf + 1, newLeafCount - 1);
                        if (isLastLeaf && isAfterLeaf(leafSizes[firstLeaf], leafValues[firstLeaf], leafValuesInsertChunk, leafRowKeys[firstLeaf], leafKeysInsertChunk)) {
                            int copyLimit = leafSize - leafSizes[firstLeaf];
                            int offset = 0;

                            for (int leaf = firstLeaf; leaf < firstLeaf + newLeafCount; ++leaf) {
                                if (leaf > firstLeaf) {
                                    leafValues[leaf] = new Object[leafSize];
                                    leafRowKeys[leaf] = new long[leafSize];
                                }
                                copyToLeaf(leafSizes[leaf], leafValues[leaf], leafValuesInsertChunk, leafRowKeys[leaf], leafKeysInsertChunk, offset, copyLimit);
                                final int newLeafSize = leafSizes[leaf] += copyLimit;

                                directoryValues[leaf] = leafValues[leaf][newLeafSize - 1];
                                directoryRowKeys[leaf] = leafRowKeys[leaf][newLeafSize - 1];

                                offset += copyLimit;
                                copyLimit = Math.min(leafSize, leafValuesInsertChunk.size() - offset);
                            }
                        } else {
                            distributeValues(valuesPerLeaf(sizeForThisLeaf, newLeafCount), firstLeaf, newLeafCount, leafValuesInsertChunk, leafKeysInsertChunk);
                        }
                        validateLeafRange(firstLeaf, newLeafCount);
                    }
                    firstValuesPosition = lastValueForLeaf + 1;

                    if (SEGMENTED_SORTED_ARRAY_VALIDATION) {
                        Assert.eq(computeLeafSizes(), "computeLeafSizes()", totalCount + size, "totalCount + size");
                    }

                }
                Assert.eq(totalCount, "totalCount", valuesToInsert.size(), "valuesToInsert.size()");
            }
        }

        size += insertSize;
        validate();
    }

    private int getDesiredLeafCount(int newSize) {
        return (newSize + leafSize - 1) / leafSize;
    }

    private static int valuesPerLeaf(int values, int leafCount) {
        return (values + leafCount - 1) / leafCount;
    }

    private void mergeLeaves(int leaf, TIntList leavesToRemove) {
        Assert.lt(leaf, "leaf", leafCount - 1, "leafCount - 1");

        final int destinationSize = leafSizes[leaf];
        final int sourceSize = leafSizes[leaf + 1];

        copyLeafValues(leaf + 1, leaf, destinationSize, sourceSize, 0);

        final Object [] tmpValues = leafValues[leaf + 1];
        final long [] tmpRowKeys = leafRowKeys[leaf + 1];

        leafValues[leaf + 1] = leafValues[leaf];
        leafRowKeys[leaf + 1] = leafRowKeys[leaf];
        leafValues[leaf] = tmpValues;
        leafRowKeys[leaf] = tmpRowKeys;

        directoryValues[leaf] = leafValues[leaf + 1][0];
        directoryRowKeys[leaf] = leafRowKeys[leaf + 1][0] - 1;

        leafSizes[leaf] = 0;
        leafSizes[leaf + 1] += destinationSize;

        leavesToRemove.add(leaf);
    }

    private void mergeThreeLeaves(int leaf, TIntList leavesToRemove) {
        Assert.lt(leaf, "leaf", leafCount - 2, "leafCount - 2");

        final int destinationSize = leafSizes[leaf];
        final int sourceMiddleSize = leafSizes[leaf + 1];
        final int sourceRightSize = leafSizes[leaf + 2];

        copyLeafValues(leaf + 1, leaf, destinationSize, sourceMiddleSize, 0);
        copyLeafValues(leaf + 2, leaf, destinationSize + sourceMiddleSize, sourceRightSize, 0);

        final Object [] tmpValues = leafValues[leaf + 1];
        final long [] tmpRowKeys = leafRowKeys[leaf + 1];

        leafValues[leaf + 2] = leafValues[leaf];
        leafRowKeys[leaf + 2] = leafRowKeys[leaf];

        leafValues[leaf] = tmpValues;
        leafRowKeys[leaf] = tmpRowKeys;

        final int newSize = destinationSize + sourceMiddleSize + sourceRightSize;
        leafSizes[leaf] = 0;
        leafSizes[leaf + 1] = 0;
        leafSizes[leaf + 2] = newSize;

        directoryValues[leaf] = leafValues[leaf + 2][0];
        directoryValues[leaf + 1] = leafValues[leaf + 2][0];

        directoryRowKeys[leaf] = leafRowKeys[leaf + 2][0] - 2;
        directoryRowKeys[leaf + 1] = leafRowKeys[leaf + 2][0] - 1;

        leavesToRemove.add(leaf);
        leavesToRemove.add(leaf + 1);


        if (SEGMENTED_SORTED_ARRAY_VALIDATION) {
            if (leafCount > 1) {
                validateLeaf(leaf);
            } else {
                validateLeaf(directoryValues, directoryRowKeys, newSize);
            }
        }
    }

    private void copyLeafValues(int sourceLeaf, int destLeaf, int destinationSize, int sourceMiddleSize, int srcPos) {
        System.arraycopy(leafValues[sourceLeaf], srcPos, leafValues[destLeaf], destinationSize, sourceMiddleSize);
        System.arraycopy(leafRowKeys[sourceLeaf], srcPos, leafRowKeys[destLeaf], destinationSize, sourceMiddleSize);
    }

    private void copyLeavesAndDirectory(int srcPos, int destPos, int length) {
        System.arraycopy(leafSizes, srcPos, leafSizes, destPos, length);
        System.arraycopy(leafValues, srcPos, leafValues, destPos, length);
        System.arraycopy(leafRowKeys, srcPos, leafRowKeys, destPos, length);
        System.arraycopy(directoryValues, srcPos, directoryValues, destPos, length);
        System.arraycopy(directoryRowKeys, srcPos, directoryRowKeys, destPos, length);
    }

    private void copyToLeaf(int leafOffset, Object[] leafValues, ObjectChunk<Object, ? extends Any> insertValues, long[] leafRowKeys, LongChunk<? extends RowKeys> insertRowKeys) {
        copyToLeaf(leafOffset, leafValues, insertValues, leafRowKeys, insertRowKeys, 0, insertRowKeys.size());
    }

    private void copyToLeaf(int leafOffset, Object[] leafValues, ObjectChunk<Object, ? extends Any> insertValues, long[] leafRowKeys, LongChunk<? extends RowKeys> insertRowKeys, int srcOffset, int length) {
        insertValues.copyToTypedArray(srcOffset, leafValues, leafOffset, length);
        insertRowKeys.copyToTypedArray(srcOffset, leafRowKeys, leafOffset, length);
    }

    private void moveLeafValues(Object[] leafValues, long[] leafRowKeys, int srcPos, int destPos, int length) {
        System.arraycopy(leafValues, srcPos, leafValues, destPos, length);
        System.arraycopy(leafRowKeys, srcPos, leafRowKeys, destPos, length);
    }

    private void promoteDirectory(int newLeafCount) {
        leafSizes = new int[newLeafCount];
        leafValues = new Object[newLeafCount][];
        leafRowKeys = new long[newLeafCount][];

        leafSizes[0] = size;
        leafValues[0] = directoryValues;
        leafRowKeys[0] = directoryRowKeys;

        if (leafValues[0].length < leafSize) {
            leafValues[0] = Arrays.copyOf(leafValues[0], leafSize);
            leafRowKeys[0] = Arrays.copyOf(leafRowKeys[0], leafSize);
        }

        directoryValues = new Object[newLeafCount];
        directoryRowKeys = new long[newLeafCount];
        directoryValues[0] = leafValues[0][size - 1];
        directoryRowKeys[0] = leafRowKeys[0][size - 1];

        leafCount = newLeafCount;
    }

    private void makeLeafHole(int holePosition, int holeSize) {
        if (leafSizes.length < holeSize + leafCount) {
            reallocateLeafArrays(leafArraySize(holeSize + leafCount));
        }
        if (holePosition != leafCount) {
            copyLeavesAndDirectory(holePosition, holePosition + holeSize, leafCount - holePosition);
        }

        // this is not strictly necessary; but will make debugging simpler
        Arrays.fill(leafSizes, holePosition, holePosition + holeSize, 0);
        Arrays.fill(leafValues, holePosition, holePosition + holeSize, null);
        Arrays.fill(leafRowKeys, holePosition, holePosition + holeSize, null);
        Arrays.fill(directoryRowKeys, holePosition, holePosition + holeSize, -1);
        // region fillValue
        Arrays.fill(directoryValues, holePosition, holePosition + holeSize, null);
        // endregion fillValue

        leafCount += holeSize;
    }

    private void reallocateLeafArrays(int newSize) {
        leafSizes = Arrays.copyOf(leafSizes, newSize);
        leafValues = Arrays.copyOf(leafValues, newSize);
        leafRowKeys = Arrays.copyOf(leafRowKeys, newSize);
        directoryValues = Arrays.copyOf(directoryValues, newSize);
        directoryRowKeys = Arrays.copyOf(directoryRowKeys, newSize);
    }

    private int leafArraySize(int minimumSize) {
        return Math.max(minimumSize, leafSizes.length * 2);
    }

    private void distributeValues(int targetSize, int startingLeaf, int distributionSlots, ObjectChunk<Object, ? extends Any> valuesToInsert, LongChunk<? extends RowKeys> rowKeys) {
        final int totalInsertions = valuesToInsert.size() + leafSizes[startingLeaf];
        final int shortLeaves = (distributionSlots * targetSize) - totalInsertions;
        final int lastFullSlot = startingLeaf + shortLeaves;

        int rposl = leafSizes[startingLeaf] - 1;
        int rposi = valuesToInsert.size() - 1;

        int insertedValues = 0;

        // we are distributing our values from right to left (i.e. higher slots to lower slots), this way we can keep
        // the values in the starting leaf, and  will not overwrite them until they have already been consumed
        for (int workingSlot = startingLeaf + distributionSlots - 1; workingSlot >= startingLeaf; workingSlot--) {
            if (workingSlot > startingLeaf) {
                leafValues[workingSlot] = new Object[leafSize];
                leafRowKeys[workingSlot] = new long[leafSize];
            }

            final int leafSize;
            if (workingSlot < lastFullSlot) {
                leafSize = targetSize - 1;
            } else {
                leafSize = targetSize;
            }

            for (int wpos = leafSize - 1; wpos >= 0; wpos--) {

                if (rposi < 0) {
                    // only leaf values left
                    if (workingSlot != startingLeaf) {
                        // we are going to copy from the source leaf to this leaf
                        copyLeafValues(startingLeaf, workingSlot, 0, wpos + 1, rposl - wpos);
                        rposl -= (wpos + 1);
                    } else {
                        // in this case, we only have values left in the leaf; and they must already be at the front
                        Assert.eq(rposl, "rposl", wpos, "wpos");
                    }
                    break;
                }
                else if (rposl < 0) {
                    // only insert values left
                    Assert.geqZero(rposi, "rposi");
                    copyToLeaf(0, leafValues[workingSlot], valuesToInsert, leafRowKeys[workingSlot], rowKeys, rposi - wpos, wpos + 1);
                    rposi -= (wpos + 1);
                    break;
                }

                final Object vall = leafValues[startingLeaf][rposl];
                final long idxl = leafRowKeys[startingLeaf][rposl];
                final Object vali = valuesToInsert.get(rposi);
                final long idxi = rowKeys.get(rposi);
                final boolean takeFromLeaf = eq(vall, vali) ? idxl > idxi : gt(vall, vali);
                if (takeFromLeaf) {
                    leafValues[workingSlot][wpos] = vall;
                    leafRowKeys[workingSlot][wpos] = idxl;
                    rposl--;
                } else {
                    leafValues[workingSlot][wpos] = vali;
                    leafRowKeys[workingSlot][wpos] = idxi;
                    rposi--;
                }
            }

            directoryValues[workingSlot] = leafValues[workingSlot][leafSize - 1];
            directoryRowKeys[workingSlot] = leafRowKeys[workingSlot][leafSize - 1];
            leafSizes[workingSlot] = leafSize;
            insertedValues += leafSize;
        }

        Assert.eq(totalInsertions, "totalInsertions", insertedValues, "insertedValues");
    }

    private void makeSingletonLeaf(ObjectChunk<Object, ? extends Any> values, LongChunk<? extends RowKeys> rowKeys) {
        directoryValues = new Object[values.size()];
        directoryRowKeys = new long[rowKeys.size()];
        copyToLeaf(0, directoryValues, values, directoryRowKeys, rowKeys);
        leafCount = 1;
    }

    private void makeLeavesInitial(ObjectChunk<Object, ? extends Any> values, LongChunk<? extends RowKeys> rowKeys) {
        final int insertSize = values.size();
        if (insertSize <= leafSize) {
            makeSingletonLeaf(values, rowKeys);
            return;
        }

        leafCount = getDesiredLeafCount(insertSize);
        final int valuesPerLeaf = valuesPerLeaf(insertSize, leafCount);
        Assert.leq(valuesPerLeaf, "valuesPerLeaf", leafSize, "leafSize");

        leafSizes = new int[leafCount];
        leafValues = new Object[leafCount][];
        leafRowKeys = new long[leafCount][];
        directoryValues = new Object[leafCount];
        directoryRowKeys = new long[leafCount];

        int offset = 0;
        for (int ii = 0; ii < leafCount; ++ii) {
            final int valuesInThisLeaf = Math.min(valuesPerLeaf, insertSize - offset);
            leafSizes[ii] = valuesInThisLeaf;
            leafValues[ii] = new Object[leafSize];
            leafRowKeys[ii] = new long[leafSize];

            copyToLeaf(0, leafValues[ii], values, leafRowKeys[ii], rowKeys, offset, valuesInThisLeaf);

            directoryValues[ii] = leafValues[ii][valuesInThisLeaf - 1];
            directoryRowKeys[ii] = leafRowKeys[ii][valuesInThisLeaf - 1];
            offset += valuesInThisLeaf;
        }
    }

    // the caller is responsible for updating the directoryValues and directoryRowKeys if required
    private void insertIntoLeaf(int leafSize, Object [] leafValues, ObjectChunk<Object, ? extends Any> insertValues, long [] leafRowKeys, LongChunk<? extends RowKeys> insertRowKeys) {
        final int insertSize = insertValues.size();

        // if we are at the end; we can just copy to the end
        if (isAfterLeaf(leafSize, leafValues, insertValues, leafRowKeys, insertRowKeys)) {
            copyToLeaf(leafSize, leafValues, insertValues, leafRowKeys, insertRowKeys);
            return;
        }

        int wpos = leafSize + insertSize - 1;
        int rposl = leafSize - 1;
        int rposi = insertSize - 1;

        int lwins = 0; // consecutive leaf wins
        int iwins = 0; // consecutive insert wins


        while (wpos >= 0) {
            if (rposi < 0) {
                Assert.eq(rposl, "rposl", wpos, "wpos");
                break;
            }
            if (rposl < 0) {
                // we should just copy everything remaining, there is no need to test anymore
                copyToLeaf(0, leafValues, insertValues, leafRowKeys, insertRowKeys, 0, rposi + 1);
                break;
            }

            final Object vall = leafValues[rposl];
            final Object vali = insertValues.get(rposi);
            final long idxl = leafRowKeys[rposl];
            final long idxi = insertRowKeys.get(rposi);
            final boolean takeFromLeaf = eq(vall, vali) ? idxl > idxi : gt(vall, vali);

            if (takeFromLeaf) {
                lwins++;
                iwins = 0;
                leafValues[wpos] = vall;
                leafRowKeys[wpos] = idxl;
                rposl--;
            } else {
                lwins = 0;
                iwins++;
                leafValues[wpos] = vali;
                leafRowKeys[wpos] = idxi;
                rposi--;
            }
            wpos--;

            // We are using a galloping method that is similar to timsort; the intuition is that if we have a lot of
            // wins in either the leaf or the values to insert, it is likely that we will have more.  Instead of examining
            // each element independently; we perform a binary search to find out where the next value in the leaf lands
            // in our insert values (assuming the insert is winning, vice versa otherwise).  Then we can copy the
            // found values in bulk.  If our gallop length exceeds the initial gallop, then we reduce the number of
            // consecutive wins before we enter gallop mode.  If we did not exceed the initial gallop length, we increase
            // the number of consecutive wins so that we don't enter gallop mode too early.
            if (iwins > minGallop && rposl >= 0) {
                // find position the smallest position in insertValues that is larger than the next leaf value
                final Object searchValue = leafValues[rposl];
                final long searchKey = leafRowKeys[rposl];

                final Object firstInsert = insertValues.get(0);
                final int gallopLength;
                if (lt(searchValue, firstInsert) || (eq(searchValue, firstInsert) && searchKey < insertRowKeys.get(0))) {
                    // copy the whole thing
                    gallopLength = rposi + 1;
                } else {
                    final int firstLowerPosition = lowerBound(insertValues, insertRowKeys, 0, rposi + 1, searchValue, searchKey);
                    gallopLength = rposi - firstLowerPosition;
                }

                if (gallopLength > 0) {
                    // copy from the insert values into the leaf
                    copyToLeaf(wpos - (gallopLength - 1), leafValues, insertValues, leafRowKeys, insertRowKeys, rposi - (gallopLength - 1), gallopLength);
                    rposi -= gallopLength;
                    wpos -= gallopLength;
                }

                leafValues[wpos] = searchValue;
                leafRowKeys[wpos--] = searchKey;
                rposl--;
                lwins = 1;
                iwins = 0;

                if (gallopLength < TimsortUtils.INITIAL_GALLOP) {
                    minGallop++;
                } else {
                    minGallop = Math.max(2, minGallop - 1);
                }
            }
            else if (lwins > minGallop && rposi >= 0) {
                // find the next insert value in the leaf
                final Object searchValue = insertValues.get(rposi);
                final long searchKey = insertRowKeys.get(rposi);

                final Object firstLeaf = leafValues[0];
                final int gallopLength;
                if (lt(searchValue, firstLeaf) || (eq(searchValue, firstLeaf) && searchKey < leafRowKeys[0])) {
                    // copy the whole thing
                    gallopLength = rposl + 1;
                } else {
                    final int firstLowerPosition = lowerBound(leafValues, leafRowKeys, 0, rposl + 1, searchValue, searchKey);
                    gallopLength = rposl - firstLowerPosition;
                }

                if (gallopLength > 0) {
                    // copy from the leaf values into the leaf
                    moveLeafValues(leafValues, leafRowKeys, rposl - (gallopLength - 1), wpos - (gallopLength - 1), gallopLength);
                    rposl -= gallopLength;
                    wpos -= gallopLength;
                }

                leafValues[wpos] = searchValue;
                leafRowKeys[wpos--] = searchKey;
                rposi--;
                lwins = 0;
                iwins = 1;

                if (gallopLength < TimsortUtils.INITIAL_GALLOP) {
                    minGallop++;
                } else {
                    minGallop = Math.max(2, minGallop - 1);
                }
            }

        }
    }

    private boolean isAfterLeaf(int leafSize, Object[] leafValues, ObjectChunk<Object, ? extends Any> insertValues, long[] leafRowKeys, LongChunk<? extends RowKeys> insertRowKeys) {
        final Object firstInsertValue = insertValues.get(0);
        final Object lastLeafValue = leafValues[leafSize - 1];
        final int comparison = doComparison(lastLeafValue, firstInsertValue);
        if (comparison == 0) {
            final long firstInsertKey = insertRowKeys.get(0);
            final long lastLeafKey = leafRowKeys[leafSize - 1];
            return lastLeafKey < firstInsertKey;
        }
        return comparison < 0;
    }

    private void clear() {
        leafCount = 0;
        size = 0;
        leafValues = null;
        leafRowKeys = null;
        leafSizes = null;
        directoryValues = null;
        directoryRowKeys = null;
    }

    private void removeFromLeaf(int leafSize, Object [] leafValues, ObjectChunk<Object, ? extends Any> removeValues, long [] leafRowKeys, LongChunk<? extends RowKeys> removeRowKeys, @Nullable WritableLongChunk<? extends RowKeys> priorRedirections, long firstPriorRedirection) {
        Assert.leq(leafSize, "leafSize", this.leafSize, "this.leafSize");
        final int removeSize = removeValues.size();

        int lwin = 0; // how many consecutive times the leaf won

        long prior = firstPriorRedirection;

        int wpos = 0;
        int rposi = 0;
        for (int rposl = 0; rposl < leafSize; ) {
            if (eq(leafValues[rposl], removeValues.get(rposi)) && leafRowKeys[rposl] == removeRowKeys.get(rposi)) {
                if (priorRedirections != null) {
                    priorRedirections.set(rposi, prior);
                }
                rposl++;
                if (++rposi == removeSize) {
                    // we've hit the end of the removal values
                    moveLeafValues(leafValues, leafRowKeys, rposl, wpos, leafSize - rposl);
                    break;
                }
                lwin = 0;
                continue;
            }

            prior = leafRowKeys[rposl];

            leafValues[wpos] = leafValues[rposl];
            leafRowKeys[wpos++] = leafRowKeys[rposl++];
            if (++lwin >= minGallop) {
                lwin = 0;
                // the leaf is consistently winning, so we need to search in the leaf for the next value in remove

                final Object searchValue = removeValues.get(rposi);
                final long searchKey = removeRowKeys.get(rposi);

                final int locationToRemoveInLeaf = bound(leafValues, leafRowKeys, searchValue, searchKey, rposl, leafSize);

                final int gallopSize = locationToRemoveInLeaf - rposl;

                // what do we do now?
                if (gallopSize > 0) {
                    moveLeafValues(leafValues, leafRowKeys, rposl, wpos, gallopSize);
                    rposl += gallopSize;
                    wpos += gallopSize;
                    prior = leafRowKeys[rposl - 1];
                }

                if (gallopSize > TimsortUtils.INITIAL_GALLOP) {
                    minGallop = Math.max(2, minGallop - 1);
                } else {
                    minGallop++;
                }
            }
        }
    }


    private static int lowerBound(ObjectChunk<Object, ? extends Any> valuesToSearch, LongChunk<? extends RowKeys> rowKeysToSearch, int lo, int hi, Object searchValue, long searchKey) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final Object testValue = valuesToSearch.get(mid);
            final int comparison = doComparison(testValue, searchValue);
            final boolean moveLo = comparison == 0 ? rowKeysToSearch.get(mid) < searchKey :  comparison < 0;
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

    private static int upperBound(ObjectChunk<Object, ? extends Any> valuesToSearch, LongChunk<? extends RowKeys> rowKeysToSearch, int lo, int hi, Object searchValue, long searchKey) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final Object testValue = valuesToSearch.get(mid);
            final int comparison = doComparison(testValue, searchValue);
            final boolean moveLo = comparison == 0 ? rowKeysToSearch.get(mid) <= searchKey : comparison < 0;
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

    private static int lowerBound(Object [] valuesToSearch, long [] rowKeysToSearch, int lo, int hi, Object searchValue, long searchKey) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final Object testValue = valuesToSearch[mid];
            final int comparison = doComparison(testValue, searchValue);
            final boolean moveLo = comparison == 0 ? rowKeysToSearch[mid] < searchKey :  comparison < 0;
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

    private static int upperBound(Object[] valuesToSearch, int lo, int hi, Object searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final Object testValue = valuesToSearch[mid];
            final int comparison = doComparison(testValue, searchValue);
            final boolean moveLo = comparison < 0;
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
     * Given a searchValue and searchRowKey, find the leaf that they belong in.
     *
     * @param valuesToSearch  the array of values to search in
     * @param rowKeysToSearch the array of rowKeys to search in
     * @param lo              inclusive first position
     * @param hi              exclusive last position
     * @param searchValue     the value to search for
     * @param searchRowKey     the row key to search for
     * @return the highest row key with a value greater than searchValue
     */
    private static int bound(Object [] valuesToSearch, long [] rowKeysToSearch, final Object searchValue, long searchRowKey, int lo, int hi) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final Object testValue = valuesToSearch[mid];
            final int comparison = doComparison(testValue, searchValue);
            final boolean moveLo = comparison == 0 ?  rowKeysToSearch[mid] < searchRowKey : comparison <= 0;
            if (moveLo) {
                // For bound, (testValue lt searchValue) means that the result is somewhere later than 'mid'
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        return lo;
    }

    @Override
    public void remove(Chunk<? extends Any> valuesToRemove, LongChunk<? extends RowKeys> rowKeysToRemove) {
        remove(valuesToRemove.asObjectChunk(), rowKeysToRemove);
    }

    @Override
    public void removeAndGetPrior(Chunk<? extends Any> stampChunk, LongChunk<? extends RowKeys> rowKeysToRemove, WritableLongChunk<? extends RowKeys> priorRedirections) {
        removeAndGetNextInternal(stampChunk.asObjectChunk(), rowKeysToRemove, priorRedirections);
    }

    private void remove(ObjectChunk<Object, ? extends Any> valuesToRemove, LongChunk<? extends RowKeys> rowKeysToRemove) {
        removeAndGetNextInternal(valuesToRemove, rowKeysToRemove, null);
    }

    /**
     * Remove valuesToRemove from this SSA.  The valuesToRemove to remove must be sorted.
     *
     * @param valuesToRemove    the valuesToRemove to remove
     * @param rowKeysToRemove   the corresponding rowKeys
     * @param priorRedirections the prior redirection for a removed value
     */
    private void removeAndGetNextInternal(ObjectChunk<Object, ? extends Any> valuesToRemove, LongChunk<? extends RowKeys> rowKeysToRemove, @Nullable WritableLongChunk<? extends RowKeys> priorRedirections) {
        validate();

        if (priorRedirections != null) {
            priorRedirections.setSize(valuesToRemove.size());
        }

        final int removeSize = valuesToRemove.size();
        if (removeSize == 0) {
            return;
        }
        if (removeSize == size) {
            if (priorRedirections != null) {
                priorRedirections.fillWithValue(0, valuesToRemove.size(), RowSequence.NULL_ROW_KEY);
            }
            clear();
            return;
        }
        Assert.gtZero(leafCount, "leafCount");
        if (leafCount == 1) {
            removeFromLeaf(size, directoryValues, valuesToRemove, directoryRowKeys, rowKeysToRemove, priorRedirections, RowSequence.NULL_ROW_KEY);
        } else {
            try (final ResettableObjectChunk<Object, Any> leafValuesRemoveChunk = ResettableObjectChunk.makeResettableChunk();
                 final ResettableLongChunk<RowKeys> leafKeysRemoveChunk = ResettableLongChunk.makeResettableChunk();
                 final ResettableWritableLongChunk<RowKeys> priorRedirectionsSlice = priorRedirections == null ? null : ResettableWritableLongChunk.makeResettableChunk()) {
                int firstValuesPosition = 0;
                int totalCount = 0;

                final TIntList leavesToRemove = new TIntArrayList();

                while (firstValuesPosition < removeSize) {
                    // we need to find out where our valuesToRemove should go using a binary search of the directory
                    final int firstLeaf = Math.min(bound(directoryValues, directoryRowKeys, valuesToRemove.get(firstValuesPosition), rowKeysToRemove.get(firstValuesPosition), 0, leafCount), leafCount - 1);

                    final int lastPositionForLeaf;
                    if (firstLeaf == leafCount - 1) {
                        lastPositionForLeaf = removeSize - 1;
                    } else {
                        final Object leafMaxValue = directoryValues[firstLeaf];
                        final long leafMaxRowKey = directoryRowKeys[firstLeaf];
                        lastPositionForLeaf = upperBound(valuesToRemove, rowKeysToRemove, firstValuesPosition, removeSize, leafMaxValue, leafMaxRowKey);
                    }

                    final int count = lastPositionForLeaf - firstValuesPosition + 1;
                    totalCount += count;

                    if (count == leafSizes[firstLeaf]) {
                        // we are going to remove the whole leaf
                        final long firstPrior = priorRedirections == null ? RowSequence.NULL_ROW_KEY : getFirstPrior(firstLeaf);
                        leavesToRemove.add(firstLeaf);
                        leafSizes[firstLeaf] = 0;
                        if (priorRedirections != null) {
                            priorRedirections.fillWithValue(firstValuesPosition, count, firstPrior);
                        }
                    } else {

                        leafValuesRemoveChunk.resetFromTypedChunk(valuesToRemove, firstValuesPosition, count);
                        leafKeysRemoveChunk.resetFromTypedChunk(rowKeysToRemove, firstValuesPosition, count);
                        if (priorRedirections != null) {
                            priorRedirectionsSlice.resetFromTypedChunk(priorRedirections, firstValuesPosition, count);
                        }

                        final long firstPrior = priorRedirections == null ? RowSequence.NULL_ROW_KEY : getFirstPrior(firstLeaf);
                        removeFromLeaf(leafSizes[firstLeaf], leafValues[firstLeaf], leafValuesRemoveChunk, leafRowKeys[firstLeaf], leafKeysRemoveChunk, priorRedirectionsSlice, firstPrior);
                        leafSizes[firstLeaf] -= count;

                        final boolean hasLeft = firstLeaf > 0 && (leavesToRemove.isEmpty() || (leavesToRemove.get(leavesToRemove.size() - 1) != (firstLeaf - 1)));
                        final boolean hasRight = firstLeaf < leafCount - 1;

                        // in cases where we do not have a left or right, we just set the size to leafSize so we will  not consider a merge
                        final int leftSize = hasLeft ? leafSizes[firstLeaf - 1] : leafSize;
                        final int rightSize = hasRight ? leafSizes[firstLeaf + 1] : leafSize;
                        final int middleSize = leafSizes[firstLeaf];

                        final boolean threeWay = leftSize + rightSize + middleSize < leafSize;
                        final boolean leftMerge = !threeWay && leftSize + middleSize < leafSize;
                        final boolean rightMerge = !threeWay && rightSize + middleSize < leafSize;

                        if (threeWay) {
                            mergeThreeLeaves(firstLeaf - 1, leavesToRemove);
                        } else if (leftMerge) {
                            mergeLeaves(firstLeaf - 1, leavesToRemove);
                        } else if (rightMerge) {
                            mergeLeaves(firstLeaf, leavesToRemove);
                        }
                    }
                    firstValuesPosition += count;

                    if (leafCount - leavesToRemove.size() > 1) {
                        if (SEGMENTED_SORTED_ARRAY_VALIDATION) {
                            Assert.eq(computeLeafSizes(), "computeLeafSizes()", size - totalCount, "size - totalCount");
                        }
                    } else if (firstValuesPosition < removeSize) {
                        leavesToRemove.clear();
                        // we need to promote the last remaining leaf to the directory values, because there is only a single leaf left
                        promoteLastLeafToDirectory();

                        // we have removed all but the last leaf, so we need to remove the remaining values from it
                        final int remainingRemovals = removeSize - firstValuesPosition;
                        leafValuesRemoveChunk.resetFromTypedChunk(valuesToRemove, firstValuesPosition, remainingRemovals);
                        leafKeysRemoveChunk.resetFromTypedChunk(rowKeysToRemove, firstValuesPosition, remainingRemovals);
                        if (priorRedirections != null) {
                            priorRedirectionsSlice.resetFromTypedChunk(priorRedirections, firstValuesPosition, remainingRemovals);
                        }

                        removeFromLeaf(size - totalCount, directoryValues, leafValuesRemoveChunk, directoryRowKeys, leafKeysRemoveChunk, priorRedirectionsSlice, RowSequence.NULL_ROW_KEY);
                        totalCount += remainingRemovals;
                        firstValuesPosition += remainingRemovals + 1;
                    }
                }

                if (!leavesToRemove.isEmpty()) {

                    int destIdx = leavesToRemove.get(0);
                    int srcIdx = destIdx + 1;
                    int removeIdx = 1;

                    while (removeIdx < leavesToRemove.size()) {
                        final int nextRemoval = leavesToRemove.get(removeIdx);
                        final int keepLeaves = nextRemoval - srcIdx;
                        copyLeavesAndDirectory(srcIdx, destIdx, keepLeaves);
                        srcIdx += keepLeaves + 1;
                        destIdx += keepLeaves;

                        removeIdx++;
                    }
                    final int remainingLeaves = leafCount - srcIdx;
                    if (remainingLeaves > 0) {
                        copyLeavesAndDirectory(srcIdx, destIdx, remainingLeaves);
                        destIdx += remainingLeaves;
                    }
                    if (destIdx == 1) {
                        directoryRowKeys = leafRowKeys[0];
                        directoryValues = leafValues[0];
                        leafRowKeys = null;
                        leafValues = null;
                        leafSizes = null;
                    } else {
                        Arrays.fill(leafValues, destIdx, leafCount, null);
                        Arrays.fill(leafRowKeys, destIdx, leafCount, null);
                        Arrays.fill(leafSizes, destIdx, leafCount, 0);
                    }
                    leafCount = destIdx;
                }

                Assert.eq(totalCount, "totalCount", valuesToRemove.size(), "valuesToInsert.size()");
            }
        }
        size -= removeSize;

        validate();
    }

    private long getFirstPrior(int firstLeaf) {
        int priorLeaf = firstLeaf - 1;
        while (priorLeaf >= 0 && leafSizes[priorLeaf] == 0) {
            priorLeaf--;
        }
        if (priorLeaf < 0) {
            return RowSequence.NULL_ROW_KEY;
        } else {
            return leafRowKeys[priorLeaf][leafSizes[priorLeaf] - 1];
        }
    }

    private void promoteLastLeafToDirectory() {
        directoryRowKeys = leafRowKeys[leafCount - 1];
        directoryValues = leafValues[leafCount - 1];
        leafCount = 1;
        leafSizes = null;
        leafRowKeys = null;
        leafValues = null;
    }

    @Override
    public void applyShift(Chunk<? extends Any> stampChunk, LongChunk<? extends RowKeys> keyChunk, long shiftDelta) {
        applyShift(stampChunk.asObjectChunk(), keyChunk, shiftDelta);
    }

    @Override
    public void applyShiftReverse(Chunk<? extends Any> stampChunk, LongChunk<? extends RowKeys> keyChunk, long shiftDelta) {
        applyShiftReverse(stampChunk.asObjectChunk(), keyChunk, shiftDelta);
    }

    private void applyShift(ObjectChunk<Object, ? extends Any> stampChunk, LongChunk<? extends RowKeys> keyChunk, long shiftDelta) {
        validate();
        final int shiftSize = stampChunk.size();
        if (shiftSize == 0) {
            return;
        }
        Assert.gtZero(leafCount, "leafCount");
        if (leafCount == 1) {
            shiftLeaf(size, directoryValues, stampChunk, directoryRowKeys, keyChunk, shiftDelta);
        } else {
            try (final ResettableObjectChunk<Object, Any> leafValuesChunk = ResettableObjectChunk.makeResettableChunk();
                 final ResettableLongChunk<RowKeys> leafKeyChunk = ResettableLongChunk.makeResettableChunk()) {
                int firstValuesPosition = 0;
                while (firstValuesPosition < shiftSize) {
                    // we need to find out where our stampChunk should go using a binary search of the directory
                    final int firstLeaf = Math.min(bound(directoryValues, directoryRowKeys, stampChunk.get(firstValuesPosition), keyChunk.get(firstValuesPosition), 0, leafCount), leafCount - 1);

                    final int lastValueForLeaf;
                    if (firstLeaf == leafCount - 1) {
                        lastValueForLeaf = shiftSize - 1;
                    } else {
                        final Object leafMaxValue = directoryValues[firstLeaf];
                        final long leafMaxRowKey = directoryRowKeys[firstLeaf];
                        lastValueForLeaf = lowerBound(stampChunk, keyChunk, firstValuesPosition, shiftSize, leafMaxValue, leafMaxRowKey);
                    }

                    final int count = lastValueForLeaf - firstValuesPosition + 1;

                    leafValuesChunk.resetFromTypedChunk(stampChunk, firstValuesPosition, count);
                    leafKeyChunk.resetFromTypedChunk(keyChunk, firstValuesPosition, count);

                    shiftLeaf(leafSizes[firstLeaf], leafValues[firstLeaf], leafValuesChunk, leafRowKeys[firstLeaf], leafKeyChunk, shiftDelta);
                    final int predecessorLeaf = firstLeaf - 1;
                    if (predecessorLeaf >= 0) {
                        directoryValues[predecessorLeaf] = leafValues[predecessorLeaf][leafSizes[predecessorLeaf] - 1];
                        directoryRowKeys[predecessorLeaf] = leafRowKeys[predecessorLeaf][leafSizes[predecessorLeaf] - 1];
                    }
                    directoryValues[firstLeaf] = leafValues[firstLeaf][leafSizes[firstLeaf] - 1];
                    directoryRowKeys[firstLeaf] = leafRowKeys[firstLeaf][leafSizes[firstLeaf] - 1];

                    firstValuesPosition += count;
                }
            }
        }
        validate();
    }

    private void shiftLeaf(int leafSize, Object [] leafValues, ObjectChunk<Object, ? extends Any> shiftValues, long [] leafRowKeys, LongChunk<? extends RowKeys> shiftRowKeys, long shiftDelta) {
        Assert.leq(leafSize, "leafSize", this.leafSize, "this.leafSize");
        final int shiftSize = shiftValues.size();

        int lwin = 0; // how many consecutive times the leaf won

        int shiftPos = 0;
        for (int leafPos = 0; leafPos < leafSize; ) {
            if (eq(leafValues[leafPos], shiftValues.get(shiftPos)) && leafRowKeys[leafPos] == shiftRowKeys.get(shiftPos)) {
                leafRowKeys[leafPos++] += shiftDelta;
                if (++shiftPos == shiftSize) {
                    // we've hit the end of the removal values
                    break;
                }
                lwin = 0;
                continue;
            }

            leafPos++;
            if (++lwin >= minGallop) {
                lwin = 0;
                // the leaf is consistently winning, so we need to search in the leaf for the next value in shift

                final Object searchValue = shiftValues.get(shiftPos);
                final long searchKey = shiftRowKeys.get(shiftPos);

                final int locationToShiftInLeaf = bound(leafValues, leafRowKeys, searchValue, searchKey, leafPos, leafSize);

                final int gallopSize = locationToShiftInLeaf - leafPos;

                // what do we do now?
                if (gallopSize > 0) {
                    leafPos += gallopSize;
                }

                if (gallopSize > TimsortUtils.INITIAL_GALLOP) {
                    minGallop = Math.max(2, minGallop - 1);
                } else {
                    minGallop++;
                }
            }
        }
    }

    private void applyShiftReverse(ObjectChunk<Object, ? extends Any> stampChunk, LongChunk<? extends RowKeys> keyChunk, long shiftDelta) {
        validate();
        final int shiftSize = stampChunk.size();
        if (shiftSize == 0) {
            return;
        }
        Assert.gtZero(leafCount, "leafCount");
        if (leafCount == 1) {
            shiftLeafReverse(size, directoryValues, stampChunk, directoryRowKeys, keyChunk, shiftDelta);
        } else {
            try (final ResettableObjectChunk<Object, Any> leafValuesChunk = ResettableObjectChunk.makeResettableChunk();
                 final ResettableLongChunk<RowKeys> leafKeyChunk = ResettableLongChunk.makeResettableChunk()) {
                int lastValuesPosition = shiftSize - 1;
                while (lastValuesPosition >= 0) {
                    // we need to find out where our stampChunk should go using a binary search of the directory
                    final int firstLeaf = Math.min(bound(directoryValues, directoryRowKeys, stampChunk.get(lastValuesPosition), keyChunk.get(lastValuesPosition), 0, leafCount), leafCount - 1);

                    int firstValueForLeaf;
                    if (firstLeaf == 0) {
                        firstValueForLeaf = 0;
                    } else {
                        final Object leafMinValue = leafValues[firstLeaf][0];
                        final long leafMinRowKey = leafRowKeys[firstLeaf][0];
                        firstValueForLeaf = lowerBound(stampChunk, keyChunk, 0, lastValuesPosition + 1, leafMinValue, leafMinRowKey);
                        Object foundValue = stampChunk.get(firstValueForLeaf);
                        if (lt(foundValue, leafMinValue) || (eq(foundValue, leafMinValue) && keyChunk.get(firstValueForLeaf) < leafMinRowKey)) {
                            firstValueForLeaf++;
                            foundValue = stampChunk.get(firstValueForLeaf);
                        }
                        Assert.assertion(geq(foundValue, leafMinValue), "geq(stampChunk.get(firstValueForLeaf), leafMinValue)", foundValue, "foundValue", leafMinValue, "leafMinValue");
                        if (eq(foundValue, leafMinValue)) {
                            Assert.geq(keyChunk.get(firstValueForLeaf), "keyChunk.get(firstValueForLeaf)", leafMinRowKey, "leafMinRowKey");
                        }
                    }

                    final int count = lastValuesPosition - firstValueForLeaf + 1;

                    leafValuesChunk.resetFromTypedChunk(stampChunk, firstValueForLeaf, count);
                    leafKeyChunk.resetFromTypedChunk(keyChunk, firstValueForLeaf, count);

                    shiftLeafReverse(leafSizes[firstLeaf], leafValues[firstLeaf], leafValuesChunk, leafRowKeys[firstLeaf], leafKeyChunk, shiftDelta);
                    directoryValues[firstLeaf] = leafValues[firstLeaf][leafSizes[firstLeaf] - 1];
                    directoryRowKeys[firstLeaf] = leafRowKeys[firstLeaf][leafSizes[firstLeaf] - 1];

                    lastValuesPosition -= count;
                }
            }
        }
        validate();
    }

    private void shiftLeafReverse(int leafSize, Object [] leafValues, ObjectChunk<Object, ? extends Any> shiftValues, long [] leafRowKeys, LongChunk<? extends RowKeys> shiftRowKeys, long shiftDelta) {
        Assert.leq(leafSize, "leafSize", this.leafSize, "this.leafSize");
        final int shiftSize = shiftValues.size();

        int lwin = 0; // how many consecutive times the leaf won

        int shiftPos = shiftSize - 1;
        for (int leafPos = leafSize - 1; leafPos >= 0; ) {
            if (eq(leafValues[leafPos], shiftValues.get(shiftPos)) && leafRowKeys[leafPos] == shiftRowKeys.get(shiftPos)) {
                leafRowKeys[leafPos--] += shiftDelta;
                if (--shiftPos < 0) {
                    // we've hit the end of the shift values
                    break;
                }
                lwin = 0;
                continue;
            }

            leafPos--;

            if (++lwin >= minGallop) {
                lwin = 0;
                // the leaf is consistently winning, so we need to search in the leaf for the next value in shift

                final Object searchValue = shiftValues.get(shiftPos);
                final long searchKey = shiftRowKeys.get(shiftPos);

                final int locationToShiftInLeaf = bound(leafValues, leafRowKeys, searchValue, searchKey, 0, leafPos + 1);

                final int gallopSize = leafPos - locationToShiftInLeaf;

                // what do we do now?
                if (gallopSize > 0) {
                    leafPos -= gallopSize;
                }

                if (gallopSize > TimsortUtils.INITIAL_GALLOP) {
                    minGallop = Math.max(2, minGallop - 1);
                } else {
                    minGallop++;
                }
            }
        }
    }

    @VisibleForTesting
    public void validate() {
        if (!SEGMENTED_SORTED_ARRAY_VALIDATION) {
            return;
        }
        validateInternal();
    }

    @VisibleForTesting
    void validateInternal() {
        Assert.geqZero(size, "size");
        if (size == 0) {
            Assert.eqZero(leafCount, "leafCount");
        } else {
            Assert.gtZero(leafCount, "leafCount");
        }
        if (leafCount == 0) {
            Assert.eqNull(leafValues, "leafValues");
            Assert.eqNull(leafRowKeys, "leafValues");
            Assert.eqNull(leafSizes, "leafSizes");
            Assert.eqNull(directoryRowKeys, "directoryRowKeys");
            Assert.eqNull(directoryValues, "directoryValues");
        } else if (leafCount == 1) {
            Assert.eqNull(leafValues, "leafValues");
            Assert.eqNull(leafRowKeys, "leafRowKeys");
            Assert.eqNull(leafSizes, "leafSizes");
            Assert.neqNull(directoryRowKeys, "directoryRowKeys");
            Assert.neqNull(directoryValues, "directoryValues");
            Assert.geq(directoryRowKeys.length, "directoryRowKeys.length", size, "size");
            Assert.geq(directoryValues.length, "directoryValues.length", size, "size");

            validateLeaf(directoryValues, directoryRowKeys, size);
        } else {

            Assert.neqNull(leafValues, "leafValues");
            Assert.neqNull(leafRowKeys, "leafRowKeys");
            Assert.neqNull(leafSizes, "leafSizes");
            Assert.neqNull(directoryRowKeys, "directoryRowKeys");
            Assert.neqNull(directoryValues, "directoryValues");

            Assert.geq(directoryRowKeys.length, "directoryRowKeys.length", leafCount, "leafCount");
            Assert.geq(directoryValues.length, "directoryValues.length", leafCount, "leafCount");
            Assert.geq(leafSizes.length, "leafSizes.length", leafCount, "leafCount");
            Assert.geq(leafValues.length, "leafValues.length", leafCount, "leafCount");
            Assert.geq(leafRowKeys.length, "leafRowKeys.length", leafCount, "leafCount");

            Assert.eq(computeLeafSizes(), "computeLeafSizes()", size, "size");

            for (int ii = 0; ii < leafCount; ++ii) {
                validateLeaf(ii);
                final Object lastValue = leafValues[ii][leafSizes[ii] - 1];
                final Object directoryValue = directoryValues[ii];
                Assert.assertion(leq(lastValue, directoryValue), "lt(lastValue, directoryValue)", lastValue, "leafValues[ii][leafSizes[ii] - 1]", directoryValue, "directoryValue");
                if (eq(lastValue, directoryValue)) {
                    Assert.leq(leafRowKeys[ii][leafSizes[ii] - 1], "leafValues[ii][leafSizes[ii] - 1]", directoryRowKeys[ii]);
                }
                if (ii < leafCount - 1) {
                    final Object nextFirstValue = leafValues[ii + 1][0];
                    final long nextFirstKey = leafRowKeys[ii + 1][0];
                    Assert.assertion(leq(directoryValue, nextFirstValue), "leq(directoryValue, nextFirstValue)", directoryValue, "directoryValue", nextFirstValue, "nextFirstValue");
                    if (eq(nextFirstValue, directoryValue)) {
                        Assert.lt(directoryRowKeys[ii], "directoryRowKeys[ii]", nextFirstKey, "nextFirstKey");
                    }
                }
            }

            validateLeafOrdering(0, leafCount - 1);
        }
    }


    private void validateLeafRange(int firstLeaf, int leafCount) {
        if (!SEGMENTED_SORTED_ARRAY_VALIDATION) {
            return;
        }
        final int lastLeaf = firstLeaf + leafCount;
        for (int leaf = firstLeaf; leaf < lastLeaf; ++leaf) {
            validateLeaf(leaf);
        }
        if (firstLeaf > 0) {
            validateLeafOrdering(firstLeaf - 1, firstLeaf);
        }
        validateLeafOrdering(firstLeaf, lastLeaf - 1);
        if (lastLeaf < this.leafCount - 1) {
            validateLeafOrdering(lastLeaf, lastLeaf + 1);
        }
    }

    private void validateLeafOrdering(int firstLeaf, int lastLeaf) {
        if (!SEGMENTED_SORTED_ARRAY_VALIDATION) {
            return;
        }
        for (int leaf = firstLeaf; leaf < lastLeaf; ++leaf) {
            final Object lastValue = leafValues[leaf][leafSizes[leaf] - 1];
            final long lastKey = leafRowKeys[leaf][leafSizes[leaf] - 1];
            final Object nextValue = leafValues[leaf + 1][0];
            final long nextKey = leafRowKeys[leaf + 1][0];
            Assert.assertion(leq(lastValue, nextValue), lastValue + " < " + nextValue);
            if (lastValue == nextValue) {
                Assert.lt(lastKey, "lastRowKey (" + leaf + ")", nextKey, "nextKey");
            }
        }
    }

    private void validateLeaf(int leaf) {
        validateLeaf(leafValues[leaf], leafRowKeys[leaf], leafSizes[leaf]);
    }

    private static void validateLeaf(Object[] values, long[] rowKeys, int size) {
        if (!SEGMENTED_SORTED_ARRAY_VALIDATION) {
            return;
        }
        for (int ii = 0; ii < size - 1; ++ii) {
            if (lt(values[ii], values[ii + 1])) {
                continue;
            }
            Assert.assertion(eq(values[ii], values[ii + 1]), "values[" + ii + "] == values[" + (ii + 1) + "]", values[ii], "values[" + ii + "]", values[ii + 1], "values[" + (ii + 1) + "]");
            Assert.lt(rowKeys[ii], "rowKeys[" + ii + "]", rowKeys[ii + 1], "rowKeys[" + (ii + 1) + "]");
        }
    }

    private int computeLeafSizes() {
        int expectedSize = 0;
        for (int ii = 0; ii < leafCount; ++ii) {
            expectedSize += leafSizes[ii];
        }
        return expectedSize;
    }

    /**
     * Produce a single chunk of all the valuesToRemove in this SSA.
     *
     * @return a chunk of the SSAs value's, the caller owns the chunk and should close it
     */
    WritableObjectChunk<Object, ? extends Any> asObjectChunk() {
        final int chunkSize = intSize();
        final WritableObjectChunk<Object, ? extends Any> values = WritableObjectChunk.makeWritableChunk(chunkSize);
        if (leafCount == 0) {
            return values;
        }
        if (leafCount == 1) {
            values.copyFromTypedArray(directoryValues, 0, 0, chunkSize);
            return values;
        }

        int copied = 0;
        for (int ii = 0; ii < leafCount; ++ii) {
            final int leafSize = leafSizes[ii];
            values.copyFromTypedArray(leafValues[ii], 0, copied, leafSize);
            copied += leafSize;
        }
        return values;
    }

    /**
     * Produce a single chunk of all the rowKeys in this SSA.
     *
     * @return a chunk of the SSA's rowKeys, the caller owns the chunk and should close it
     */
    WritableLongChunk<RowKeys> rowKeysChunk() {
        final int chunkSize = intSize();
        final WritableLongChunk<RowKeys> rowKeys = WritableLongChunk.makeWritableChunk(chunkSize);
        if (leafCount == 0) {
            return rowKeys;
        }
        if (leafCount == 1) {
            rowKeys.copyFromTypedArray(directoryRowKeys, 0, 0, chunkSize);
            return rowKeys;
        }

        int copied = 0;
        for (int ii = 0; ii < leafCount; ++ii) {
            final int leafSize = leafSizes[ii];
            rowKeys.copyFromTypedArray(leafRowKeys[ii], 0, copied, leafSize);
            copied += leafSize;
        }
        return rowKeys;
    }

    @Override
    public void forAllKeys(LongConsumer longConsumer) {
        if (leafCount == 0) {
            return;
        }
        if (leafCount == 1) {
            for (int ii = 0; ii < size; ++ii) {
                longConsumer.accept(directoryRowKeys[ii]);
            }
            return;
        }
        for (int ii = 0; ii < leafCount; ++ii) {
            final int leafSize = leafSizes[ii];
            for (int jj = 0; jj < leafSize; ++jj) {
                longConsumer.accept(leafRowKeys[ii][jj]);
            }
        }
    }

    // region comparison functions
    // descending comparison
    private static int doComparison(Object lhs, Object rhs) {
        if (lhs == rhs) {
            return 0;
        }
        if (lhs == null) {
            return 1;
        }
        if (rhs == null) {
            return -1;
        }
        //noinspection unchecked,rawtypes
        return ((Comparable)rhs).compareTo(lhs);
    }
    // endregion comparison functions

    private static boolean gt(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) > 0;
    }

    private static boolean lt(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) < 0;
    }

    private static boolean leq(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    private static boolean geq(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) >= 0;
    }

    private static boolean eq(Object lhs, Object rhs) {
        // region equality function
        return Objects.equals(lhs, rhs);
        // endregion equality function
    }

    @Override
    public long size() {
        return size;
    }

    /**
     * Create an iterator for this ssa.
     *
     * @param disallowExactMatch if true, then we are operating as a right side that does not allow equal matches, but
     *                           only lt matches when calling advance
     * @param isRightSide        if true, then we ignore eqaual values; which is suitable for right side processing.  We also
     *                           start off with the first value.  When false, we do not advance while equal, and we start off
     *                           one before the first value (so that next must be called)
     * @return an iterator for this SSA
     */
    public Iterator iterator(boolean disallowExactMatch, boolean isRightSide) {
        return new Iterator(disallowExactMatch, isRightSide);
    }

    public final class Iterator {
        int leafIndex = 0;
        int indexWithinLeaf = 0;
        private final boolean disallowExactMatch;

        Iterator(boolean disallowExactMatch, boolean isRightSide) {
            this.disallowExactMatch = disallowExactMatch;
            if (isRightSide) {
                advanceWhileEqual();
            } else {
                indexWithinLeaf = -1;
            }
        }

        public void next() {
            indexWithinLeaf++;
            if (leafCount > 1) {
                if (indexWithinLeaf == leafSizes[leafIndex]) {
                    indexWithinLeaf = 0;
                    leafIndex++;
                }
            }
        }

        public boolean hasNext() {
            if (leafCount == 0) {
                return false;
            }
            if (leafCount == 1) {
                return indexWithinLeaf < size - 1;
            }
            return leafIndex < leafCount - 1 || (indexWithinLeaf < leafSizes[leafIndex] - 1);
        }

        public Object getValue() {
            if (leafCount == 1) {
                return directoryValues[indexWithinLeaf];
            }
            else {
                return leafValues[leafIndex][indexWithinLeaf];
            }
        }

        public Object nextValue() {
            Assert.assertion(hasNext(), "hasNext()");
            if (leafCount == 1) {
                return directoryValues[indexWithinLeaf + 1];
            }
            else if (indexWithinLeaf == leafSizes[leafIndex] - 1) {
                return leafValues[leafIndex + 1][0];
            } else {
                return leafValues[leafIndex][indexWithinLeaf + 1];
            }
        }

        public long getKey() {
            if (leafCount == 1) {
                return directoryRowKeys[indexWithinLeaf];
            }
            else {
                return leafRowKeys[leafIndex][indexWithinLeaf];
            }
        }

        public long nextKey() {
            Assert.assertion(hasNext(), "hasNext()");
            if (leafCount == 1) {
                return directoryRowKeys[indexWithinLeaf + 1];
            }
            else if (indexWithinLeaf == leafSizes[leafIndex] - 1) {
                return leafRowKeys[leafIndex + 1][0];
            } else {
                return leafRowKeys[leafIndex][indexWithinLeaf + 1];
            }
        }


        /**
         * Advance this iterator to the highest value that is less than or equal to value.
         *
         * @param value the value we are searching for
         */
        public void advanceToLast(Object value) {
            advanceToInternal(value, true);
        }

        /**
         * Advance this iterator to one position before the lowest value that is less than or equal to value.
         *
         * @param value the value we are searching for
         */
        public void advanceToBeforeFirst(Object value) {
            advanceToInternal(value, false);
            if (disallowExactMatch) {
                if (hasNext() && nextValue() == value) {
                    next();
                    advanceWhileEqual();
                }
            }
        }

        private void advanceToInternal(Object value, boolean advanceToLast) {
            if (leafCount == 0) {
                return;
            }

            // if the current value is geq value; then we are done
            // for disallowExactMatch, we should never have advanced to eq; so this check is still good
            if (indexWithinLeaf >= 0 && leq(value, getValue())) {
                return;
            }

            if (leafCount == 1) {
                if (advanceToLast) {
                    findInLeaf(value);
                } else {
                    findFirstInLeaf(value);
                }
                return;
            }
            final int comparison = doComparison(value, directoryValues[leafIndex]);
            if (comparison < 0 || (disallowExactMatch && comparison == 0)) {
                // we are certainly within this leaf
                if (advanceToLast) {
                    findInLeaf(value);
                } else {
                    findFirstInLeaf(value);
                }
                return;
            }

            if (comparison == 0) {
                if (advanceToLast) {
                    // we might be in this leaf or the next leaf (we'll never hit this case with disallowExactMatch)
                    if (leafIndex < leafCount - 1) {
                        if (eq(value, leafValues[leafIndex + 1][0])) {
                            // we need to search for it more aggressively, to find the last value in the next leaf
                            leafIndex++;
                            indexWithinLeaf = 0;
                        } else {
                            // we are still in this leaf
                            findInLeaf(value);
                        }
                    }
                } else {
                    findFirstInLeaf(value);
                    return;
                }
            }

            // we are not in this leaf, so we should binary search the directory for the correct leaf within the remaining leaves
            int newLeafIndex;
            if (disallowExactMatch || !advanceToLast) {
                newLeafIndex = upperBound(directoryValues, leafIndex, leafCount, value);
                if (newLeafIndex >= leafCount) {
                    newLeafIndex--;
                }
                else if (newLeafIndex < leafCount - 1) {
                    // our bound found the leaf that is less value, than but we might really be in the next leaf
                    if (lt(leafValues[newLeafIndex + 1][0], value)) {
                        newLeafIndex++;
                    }
                }
            } else {
                newLeafIndex = bound(directoryValues, directoryRowKeys, value, Long.MAX_VALUE, leafIndex, leafCount);
                if (newLeafIndex >= leafCount) {
                    newLeafIndex--;
                }
                else if (newLeafIndex > 0) {
                    // we are beyond the last value of leafIndex - 1, but we may not actually be within the leaf found by leafIndex yet
                    if (gt(leafValues[newLeafIndex][0], value)) {
                        newLeafIndex--;
                    }
                }
            }
            if (newLeafIndex != leafIndex) {
                indexWithinLeaf = 0;
                leafIndex = newLeafIndex;
            }
            if (advanceToLast) {
                findInLeaf(value);
            } else {
                findFirstInLeaf(value);
            }
        }

        /**
         * Advance the iterator to the last value which is equal to the current value.
         */
        public void advanceWhileEqual() {
            final Object value = getValue();
            findLastInLeaf(value);
            while (leafIndex < leafCount - 1) {
                if (indexWithinLeaf < leafSizes[leafIndex] - 1) {
                    return;
                }
                if (leafValues[leafIndex + 1][0] != value) {
                    return;
                }
                leafIndex++;
                indexWithinLeaf = 0;
                findLastInLeaf(value);
            }
        }

        /**
         * We know that if the value is to be found, it is to be found in this leaf, advance indexWithinLeaf until we find it.
         */
        private void findInLeaf(Object value) {
            if (disallowExactMatch) {
                if (leafCount == 1) {
                    indexWithinLeaf = upperBound(directoryValues, indexWithinLeaf, size, value);
                } else {
                    indexWithinLeaf = upperBound(leafValues[leafIndex], indexWithinLeaf, leafSizes[leafIndex], value);
                }
            } else {
                findLastInLeaf(value);
            }
        }

        private void findLastInLeaf(Object value) {
            if (leafCount == 1) {
                indexWithinLeaf = lowerBound(directoryValues, directoryRowKeys, indexWithinLeaf, size, value, Long.MAX_VALUE);
            } else {
                indexWithinLeaf = lowerBound(leafValues[leafIndex], leafRowKeys[leafIndex], indexWithinLeaf, leafSizes[leafIndex], value, Long.MAX_VALUE);
            }
        }

        /**
         * We know that if the value is to be found, it is to be found in this leaf, advance indexWithinLeaf until we find it.
         */
        private void findFirstInLeaf(Object value) {
            final int startIndex = Math.max(0, indexWithinLeaf);
            if (leafCount == 1) {
                indexWithinLeaf = upperBound(directoryValues, startIndex, size, value);
                if (indexWithinLeaf == 0 && disallowExactMatch ? lt(value, directoryValues[0]) : leq(value, directoryValues[0])) {
                    // we want the user to call next() to get to the relevant value
                    indexWithinLeaf--;
                }
            } else {
                indexWithinLeaf = upperBound(leafValues[leafIndex], startIndex, leafSizes[leafIndex], value);
                if (indexWithinLeaf == 0 && disallowExactMatch ? lt(value, leafValues[leafIndex][0]) : leq(value, leafValues[leafIndex][0])) {
                    // we want the user to call next() to get to the relevant value
                    indexWithinLeaf--;
                }
            }
        }
    }


    @Override
    public int getNodeSize() {
        return leafSize;
    }

    @Override
    public boolean isReversed() {
        // region isReversed
        return true;
        // endregion isReversed
    }

    @Override
    public long getFirst() {
        if (size == 0) {
            return RowSequence.NULL_ROW_KEY;
        }
        if (leafCount == 1) {
            return directoryRowKeys[0];
        }
        return leafRowKeys[0][0];
    }

    public long getLast() {
        if (size == 0) {
            return RowSequence.NULL_ROW_KEY;
        }
        if (leafCount == 1) {
            return directoryRowKeys[size - 1];
        }
        return leafRowKeys[leafCount - 1][leafSizes[leafCount - 1] - 1];
    }

    @Override
    public SsaChecker makeChecker() {
        return ObjectReverseSsaChecker.INSTANCE;
    }
}
