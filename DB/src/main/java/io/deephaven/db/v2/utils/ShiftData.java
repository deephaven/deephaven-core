/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;

public class ShiftData {

    private final TLongArrayList startIndex;
    private final TLongArrayList endIndex;
    private final TLongArrayList offsets;
    private final int size;
    private int runningSize = 0;
    private long runningOffset = 0;

    public Index getAddedPos() {
        return addedPos;
    }

    private Index addedPos;

    public ShiftData(Index index, Index removed, Index added) {
        TLongList[] removedKeys = index.findMissing(removed);
        addedPos = index.invert(added);
        endIndex = new TLongArrayList();
        startIndex = new TLongArrayList();
        offsets = new TLongArrayList();
        int removedIndex = 0;
        TLongList removedPositions = removedKeys[0];
        TLongList removedCount = removedKeys[1];
        for (Index.RangeIterator addedIt = addedPos.rangeIterator(); addedIt.hasNext();) {
            addedIt.next();
            int startOffset = (int) addedIt.currentRangeStart();
            int endOffset = (int) addedIt.currentRangeEnd();
            while (removedIndex < removedPositions.size() && removedPositions.get(removedIndex) < startOffset) {
                removeRange(removedPositions.get(removedIndex), removedCount.get(removedIndex));
                removedIndex++;
            }
            int deleteCount = 0;
            while (removedIndex < removedPositions.size() && removedPositions.get(removedIndex) <= endOffset) {
                deleteCount += removedCount.get(removedIndex);
                removedIndex++;
            }
            addRange(startOffset, endOffset, deleteCount);
        }
        while (removedIndex < removedPositions.size()) {
            removeRange(removedPositions.get(removedIndex), removedCount.get(removedIndex));
            removedIndex++;
        }
        if (runningSize > 0) {
            if (startIndex.get(runningSize - 1) <= (index.size() - added.size() + removed.size() - 1)) {
                endIndex.set(runningSize - 1, (int) (index.size() - added.size() + removed.size() - 1));
            } else {
                runningSize--;
            }
        }
        size = runningSize;
    }

    void addRange(long firstIndex, long lastIndex, long deletionCount) {
        if (lastIndex - firstIndex + 1 == deletionCount) {
            return;
        }
        if (runningSize > 0) {
            endIndex.set(runningSize - 1, firstIndex - runningOffset - 1);
        }


        long newStartIndex = firstIndex + deletionCount - runningOffset;
        runningOffset = lastIndex + runningOffset + 1 - (deletionCount + firstIndex);

        if (runningSize > 0 && ((newStartIndex + runningOffset) == (startIndex.get(runningSize - 1)
                + offsets.get(runningSize - 1)))) {
            startIndex.set(runningSize - 1, newStartIndex);
            offsets.set(runningSize - 1, runningOffset);
        } else {
            startIndex.add(newStartIndex);
            offsets.add(runningOffset);
            endIndex.add(0);
            runningSize++;
        }
    }

    void removeRange(long firstIndex, long count) {
        if (runningSize > 0) {
            endIndex.set(runningSize - 1, firstIndex - runningOffset - 1);
        }

        long newStartIndex = firstIndex - runningOffset + count;
        runningOffset = runningOffset - count;

        if (runningSize > 0
                && (newStartIndex + runningOffset == startIndex.get(runningSize - 1) + offsets.get(runningSize - 1))) {
            startIndex.set(runningSize - 1, newStartIndex);
            offsets.set(runningSize - 1, runningOffset);
        } else {
            startIndex.add(newStartIndex);
            offsets.add(runningOffset);
            endIndex.add(0);
            runningSize++;
        }
    }

    public interface ShiftCallback {
        void shift(long start, long end, long offset);
    }


    public void applyDataShift(ShiftCallback shiftCallback) {
        int startPos = 0;
        int currentPos = 0;
        while (currentPos < size) {
            if (offsets.get(startPos) > 0) {
                while (currentPos < size && offsets.get(currentPos) > 0) {
                    currentPos++;
                }
                for (int ii = currentPos - 1; ii >= startPos; ii--) {
                    shiftCallback.shift(startIndex.get(ii), endIndex.get(ii), offsets.get(ii));
                }
            } else if (offsets.get(startPos) < 0) {
                while (currentPos < size && offsets.get(currentPos) < 0) {
                    currentPos++;
                }
                for (int ii = startPos; ii < currentPos; ii++) {
                    shiftCallback.shift(startIndex.get(ii), endIndex.get(ii), offsets.get(ii));
                }
            } else {
                while (currentPos < size && offsets.get(currentPos) == 0) {
                    currentPos++;
                }
            }
            startPos = currentPos;
        }
    }

}
