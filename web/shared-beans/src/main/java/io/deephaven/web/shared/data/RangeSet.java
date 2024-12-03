//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.stream.LongStream;

/**
 * This class allows iteration over non-contiguous indexes. In the future, this will support the EcmaScript 2015
 * Iteration protocol, but for now has one method which returns an iterator, and also supports querying the size.
 * Additionally, we may add support for creating RangeSet objects to better serve some use cases.
 */
public class RangeSet {

    public static RangeSet empty() {
        return new RangeSet();
    }

    public static RangeSet ofRange(long first, long last) {
        RangeSet rangeSet = new RangeSet();
        rangeSet.addRange(new Range(first, last));
        return rangeSet;
    }

    public static RangeSet ofItems(long... items) {
        RangeSet rangeSet = new RangeSet();

        for (int i = 0; i < items.length; i++) {
            final long item = items[i];
            rangeSet.addRange(new Range(item, item));
        }

        return rangeSet;
    }

    public static RangeSet fromSortedRanges(List<Range> sortedRanges) {
        assertOrderedAndNonOverlapping(sortedRanges.toArray(new Range[0]));
        RangeSet rangeSet = new RangeSet();
        rangeSet.sortedRanges = sortedRanges;
        return rangeSet;
    }

    public static RangeSet fromSortedRanges(Range[] sortedRanges) {
        assertOrderedAndNonOverlapping(sortedRanges);
        RangeSet rangeSet = new RangeSet();
        rangeSet.sortedRanges.addAll(Arrays.asList(sortedRanges));
        return rangeSet;
    }

    private static void assertOrderedAndNonOverlapping(Range[] sortedRanges) {
        long lastSeen = -1;
        for (int i = 0; i < sortedRanges.length; i++) {
            assert lastSeen < sortedRanges[i].getFirst() || lastSeen == -1
                    : sortedRanges[i - 1] + " came before " + sortedRanges[i] + " (index=" + i + ")";
            lastSeen = sortedRanges[i].getLast();
        }
    }

    private List<Range> sortedRanges = new ArrayList<>();

    private int firstWrongCacheEntry = 0;
    private long[] cardinality = new long[0];

    public void addRangeSet(RangeSet rangeSet) {
        if (isEmpty() && !rangeSet.isEmpty()) {
            sortedRanges = new ArrayList<>(rangeSet.sortedRanges);
            poisonCache(0);
        } else if (!rangeSet.isEmpty()) {
            RangeAccumulator newRanges = new RangeAccumulator();
            Iterator<Range> rangeIterator = sortedRanges.iterator();
            Iterator<Range> addIterator = rangeSet.sortedRanges.iterator();

            Range toCheck = rangeIterator.next();
            Range toAdd = addIterator.next();
            while (true) {
                if (toCheck.getLast() < toAdd.getFirst()) {
                    newRanges.appendRange(toCheck);
                    if (!rangeIterator.hasNext()) {
                        toCheck = null;
                        break;
                    }
                    toCheck = rangeIterator.next();
                } else if (toCheck.getFirst() > toAdd.getLast()) {
                    newRanges.appendRange(toAdd);

                    if (!addIterator.hasNext()) {
                        toAdd = null;
                        break;
                    }
                    toAdd = addIterator.next();
                } else {
                    Range overlap = toCheck.overlap(toAdd);
                    assert overlap != null;
                    newRanges.appendRange(overlap);

                    if (!rangeIterator.hasNext()) {
                        toCheck = null;
                        break;
                    }
                    toCheck = rangeIterator.next();

                    if (!addIterator.hasNext()) {
                        toAdd = null;
                        break;
                    }
                    toAdd = addIterator.next();
                }
            }

            // Grab remaining ranges
            if (toCheck != null) {
                assert toAdd == null;
                newRanges.appendRange(toCheck);
                while (rangeIterator.hasNext()) {
                    newRanges.appendRange(rangeIterator.next());
                }
            } else {
                assert toAdd != null;
                newRanges.appendRange(toAdd);
                while (addIterator.hasNext()) {
                    newRanges.appendRange(addIterator.next());
                }
            }

            this.sortedRanges = newRanges.build();
            poisonCache(0);
        }
    }

    public void addRange(Range range) {
        addRangeSet(RangeSet.fromSortedRanges(Collections.singletonList(range)));
    }

    public void removeRangeSet(RangeSet rangeSet) {
        if (isEmpty() || rangeSet.isEmpty()) {
            return;
        }

        RangeAccumulator newRanges = new RangeAccumulator();
        RangeIterator rangeIterator = new RangeIterator(sortedRanges);
        Iterator<Range> removeIterator = rangeSet.sortedRanges.iterator();

        Range toCheck = rangeIterator.next();
        Range toRemove = removeIterator.next();
        while (true) {
            if (toCheck.getLast() < toRemove.getFirst()) {
                newRanges.appendRange(toCheck);
                if (!rangeIterator.hasNext()) {
                    toCheck = null;
                    break;
                }
                toCheck = rangeIterator.next();
            } else if (toCheck.getFirst() > toRemove.getLast()) {
                if (!removeIterator.hasNext()) {
                    break;
                }
                toRemove = removeIterator.next();
            } else {
                Range[] remaining = toCheck.minus(toRemove);
                if (remaining.length == 0) {
                    // entire range removed, advance to the next range to check
                    if (!rangeIterator.hasNext()) {
                        toCheck = null;
                        break;
                    }
                    toCheck = rangeIterator.next();
                } else if (remaining.length == 1) {
                    Range remainingRange = remaining[0];
                    if (remainingRange.compareTo(toCheck) > 0) {
                        // unremoved range still needs to be checked
                        rangeIterator.advanceInCurrentRangeToKey(remainingRange.getFirst());
                        toCheck = rangeIterator.next();

                        if (!removeIterator.hasNext()) {
                            break;
                        }
                        toRemove = removeIterator.next();
                    } else {
                        // keep the leading, remaining section
                        newRanges.appendRange(remainingRange);

                        // look at the next range
                        if (!rangeIterator.hasNext()) {
                            toCheck = null;
                            break;
                        }
                        toCheck = rangeIterator.next();
                    }
                } else {
                    assert remaining.length == 2;
                    newRanges.appendRange(remaining[0]);

                    rangeIterator.advanceInCurrentRangeToKey(remaining[1].getFirst());
                    toCheck = rangeIterator.next();

                    if (!removeIterator.hasNext()) {
                        break;
                    }
                    toRemove = removeIterator.next();
                }
            }
        }

        // Grab remaining ranges
        if (toCheck != null) {
            newRanges.appendRange(toCheck);
            while (rangeIterator.hasNext()) {
                newRanges.appendRange(rangeIterator.next());
            }
        }

        this.sortedRanges = newRanges.build();
        poisonCache(0);
    }

    public void removeRange(Range range) {
        removeRangeSet(RangeSet.fromSortedRanges(Collections.singletonList(range)));
    }

    public void clear() {
        sortedRanges.clear();
        poisonCache(0);
    }

    private static class RangeAccumulator {
        private final List<Range> replacement = new ArrayList<>();

        public void appendRange(Range range) {
            if (!replacement.isEmpty()) {
                Range lastSeen = replacement.get(replacement.size() - 1);
                Range overlap = lastSeen.overlap(range);
                if (overlap != null) {
                    replacement.set(replacement.size() - 1, overlap);
                } else {
                    replacement.add(range);
                }
            } else {
                replacement.add(range);
            }
        }

        public void appendRanges(List<Range> ranges) {
            appendRange(ranges.get(0));
            replacement.addAll(ranges.subList(0, ranges.size() - 1));
        }

        public void appendRanges(List<Range> ranges, long firstItemSubindex) {
            Range first = ranges.get(0);
            appendRange(new Range(first.getFirst() + firstItemSubindex, first.getLast()));
            replacement.addAll(ranges.subList(0, ranges.size() - 1));
        }

        public List<Range> build() {
            return replacement;
        }
    }

    private static class RangeIterator {
        private int index = -1;
        private final List<Range> ranges;
        private long key = 0;

        private RangeIterator(List<Range> ranges) {
            this.ranges = ranges;
        }

        public void advanceInCurrentRangeToKey(long key) {
            assert key != 0;
            this.key = key;
        }

        public boolean hasNext() {
            return key == -1 || index < ranges.size() - 1;
        }

        public Range next() {
            if (key != 0) {
                Range r = ranges.get(index);
                assert key > r.getFirst() && key <= r.getLast();
                r = new Range(key, r.getLast());
                key = 0;

                return r;
            }
            return ranges.get(++index);
        }

    }

    public void applyShifts(ShiftedRange[] shiftedRanges) {
        if (shiftedRanges.length == 0 || isEmpty()) {
            return;
        }
        RangeAccumulator newRanges = new RangeAccumulator();
        RangeIterator rangeIterator = new RangeIterator(sortedRanges);
        Iterator<ShiftedRange> shiftIterator = Arrays.asList(shiftedRanges).iterator();
        Range toCheck = rangeIterator.next();
        ShiftedRange shiftedRange = shiftIterator.next();
        do {
            if (toCheck.getLast() < shiftedRange.getRange().getFirst()) {
                // leave this range alone, the range to shift is after it
                newRanges.appendRange(toCheck);
                if (!rangeIterator.hasNext()) {
                    toCheck = null;
                    break;
                }
                toCheck = rangeIterator.next();
            } else if (toCheck.getFirst() > shiftedRange.getRange().getLast()) {
                // skip the rest of this shift, the next range is after it
                if (!shiftIterator.hasNext()) {
                    break;
                }
                shiftedRange = shiftIterator.next();
            } else {
                Range[] remaining = toCheck.minus(shiftedRange.getRange());
                if (remaining.length == 0) {
                    // entire range shifted
                    newRanges.appendRange(toCheck.shift(shiftedRange.getDelta()));
                    if (!rangeIterator.hasNext()) {
                        toCheck = null;
                        break;
                    }
                    toCheck = rangeIterator.next();
                } else if (remaining.length == 1) {
                    Range remainingRange = remaining[0];

                    Range[] complimentArr = toCheck.minus(remainingRange);
                    assert complimentArr.length == 1;
                    Range compliment = complimentArr[0];
                    if (remainingRange.compareTo(toCheck) > 0) {
                        // shift the compliment
                        newRanges.appendRange(compliment.shift(shiftedRange.getDelta()));

                        // rest of the range still needs to be checked
                        rangeIterator.advanceInCurrentRangeToKey(remainingRange.getFirst());
                        toCheck = rangeIterator.next();

                        // shift is consumed, move to the next one
                        if (!shiftIterator.hasNext()) {
                            break;
                        }
                        shiftedRange = shiftIterator.next();
                    } else {
                        // keep the remaining section
                        newRanges.appendRange(remainingRange);
                        // leftovers after, shift the compliment
                        newRanges.appendRange(compliment.shift(shiftedRange.getDelta()));

                        // look at the next range
                        if (!rangeIterator.hasNext()) {
                            toCheck = null;
                            break;
                        }
                        toCheck = rangeIterator.next();
                    }
                } else {
                    assert remaining.length == 2;
                    // We matched the entire shift range, plus a prefix and suffix
                    // First append the before section
                    newRanges.appendRange(remaining[0]);
                    // Then the entire shift range
                    newRanges.appendRange(shiftedRange.getResultRange());

                    // Visit the rest of the range next
                    rangeIterator.advanceInCurrentRangeToKey(remaining[1].getFirst());
                    toCheck = rangeIterator.next();

                    if (!shiftIterator.hasNext()) {
                        break;
                    }
                    shiftedRange = shiftIterator.next();
                }
            }
        } while (true);

        // Grab remaining ranges
        if (toCheck != null) {
            newRanges.appendRange(toCheck);
            while (rangeIterator.hasNext()) {
                newRanges.appendRange(rangeIterator.next());
            }
        }

        sortedRanges = newRanges.build();
        poisonCache(0);
    }

    /**
     * a new iterator over all indexes in this collection.
     * 
     * @return Iterator of {@link Range}
     */
    public Iterator<Range> rangeIterator() {
        return sortedRanges.iterator();
    }

    public Iterator<Range> reverseRangeIterator() {
        return new Iterator<>() {
            int i = sortedRanges.size();

            @Override
            public boolean hasNext() {
                return i > 0;
            }

            @Override
            public Range next() {
                return sortedRanges.get(--i);
            }
        };
    }

    public PrimitiveIterator.OfLong indexIterator() {
        if (isEmpty()) {
            return LongStream.empty().iterator();
        }
        return new PrimitiveIterator.OfLong() {
            private int rangeIndex = 0;
            private Range current = sortedRanges.get(0);
            private long offsetInRange = 0;

            @Override
            public long nextLong() {
                long value = current.getFirst() + offsetInRange;
                if (++offsetInRange >= current.size()) {
                    rangeIndex++;
                    offsetInRange = 0;
                    if (rangeIndex < rangeCount()) {
                        current = sortedRanges.get(rangeIndex);
                    } else {
                        current = null;
                    }
                }
                return value;
            }

            @Override
            public boolean hasNext() {
                return rangeIndex < rangeCount();
            }
        };
    }

    public int rangeCount() {
        return sortedRanges.size();
    }

    public boolean isFlat() {
        return sortedRanges.isEmpty() || sortedRanges.size() == 1 && getFirstRow() == 0;
    }

    /**
     * The total count of items contained in this collection. In some cases this can be expensive to compute, and
     * generally should not be needed except for debugging purposes, or preallocating space (i.e., do not call this
     * property each time through a loop).
     * 
     * @return long
     */
    public long size() {
        if (rangeCount() == 0) {
            return 0;
        }
        ensureCardinalityCache();
        return cardinality[sortedRanges.size() - 1];
    }

    public boolean isEmpty() {
        return rangeCount() == 0;
    }

    public boolean contains(long value) {
        // TODO this should be the simple case, optimize this
        return includesAllOf(RangeSet.ofItems(value));
    }

    public boolean includesAllOf(RangeSet other) {
        Iterator<Range> seenIterator = rangeIterator();
        Iterator<Range> mustMatchIterator = other.rangeIterator();
        if (!mustMatchIterator.hasNext()) {
            return true;
        }
        if (!seenIterator.hasNext()) {
            return false;
        }

        while (mustMatchIterator.hasNext()) {
            Range match = mustMatchIterator.next();

            while (seenIterator.hasNext()) {
                Range current = seenIterator.next();

                if (match.getFirst() < current.getFirst()) {
                    // can't match at all, starts too early
                    return false;
                }

                if (match.getFirst() > current.getLast()) {
                    // doesn't start until after the current range, so keep moving forward
                    continue;
                }
                if (match.getLast() > current.getLast()) {
                    // since the match starts within current, if it ends afterward, we know at least one item is
                    // missing: current.getLast() + 1
                    return false;
                }
                // else, the match is fully contained in current, so move on to the next item
                break;
            }
        }
        return true;
    }

    public boolean includesAnyOf(Range range) {
        if (isEmpty()) {
            return false;
        }
        // search the sorted list of ranges and find where the current range starts. two case here when using
        // binarySearch, either the removed range starts in the same place as an existing range starts, or
        // it starts before an item (and so we check the item before and the item after)
        int index = Collections.binarySearch(sortedRanges, range);
        if (index >= 0) {
            // matching start position
            return true;
        }
        // adjusted index notes where the item would be if it were added, minus _one more_ to see if
        // it overlaps the item before it. To compute "the position where the new item belongs", we
        // would do (-index - 1), so to examine one item prior to that we'll subtract one more. Then,
        // to confirm that we are inserting in a valid position, take the max of that value and zero.
        index = Math.max(0, -index - 2);

        // Check if there is any overlap with the prev item
        Range target = sortedRanges.get(index);
        if (range.getFirst() <= target.getLast() && range.getLast() >= target.getFirst()) {
            return true;
        }

        // Check if there is a later item, and if there is an overlap with it
        index++;
        if (index >= rangeCount()) {
            return false;
        }
        target = sortedRanges.get(index);
        return range.getFirst() <= target.getLast() && range.getLast() >= target.getFirst();
    }

    public long find(long key) {
        long cnt = 0;
        Iterator<Range> seenIterator = rangeIterator();

        while (seenIterator.hasNext()) {
            Range current = seenIterator.next();

            if (key < current.getFirst()) {
                // can't match at all, starts too early
                return -cnt - 1;
            }

            if (key > current.getLast()) {
                // doesn't start until after the current range, so keep moving forward
                cnt += current.size();
                continue;
            }
            if (key <= current.getLast()) {
                // this is a match
                return cnt + key - current.getFirst();
            }
        }
        return -cnt - 1;
    }

    @Override
    public String toString() {
        return "RangeSet{" +
                "sortedRanges=" + sortedRanges +
                '}';
    }

    public long getFirstRow() {
        return sortedRanges.get(0).getFirst();
    }

    public long getLastRow() {
        return sortedRanges.get(rangeCount() - 1).getLast();
    }

    public RangeSet copy() {
        RangeSet copy = new RangeSet();
        copy.sortedRanges = new ArrayList<>(sortedRanges);
        return copy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final RangeSet rangeSet = (RangeSet) o;
        return sortedRanges.equals(rangeSet.sortedRanges);
    }

    @Override
    public int hashCode() {
        return sortedRanges.hashCode();
    }

    /**
     * Indicates that this item has been changed, and should be recomputed. Stores the earliest offset that should be
     * recomputed.
     */
    private void poisonCache(int rangeIndex) {
        firstWrongCacheEntry = Math.min(rangeIndex, firstWrongCacheEntry);
    }

    /**
     * Ensures that the cardinality cache is correct, by correcting any values after the first wrong entry.
     */
    private void ensureCardinalityCache() {
        if (firstWrongCacheEntry == rangeCount()) {
            return;
        }
        if (cardinality.length < rangeCount()) {
            long[] replacement = new long[rangeCount()];
            System.arraycopy(cardinality, 0, replacement, 0, cardinality.length);
            cardinality = replacement;
        }
        assert firstWrongCacheEntry >= 0 : this;
        long cumulative = firstWrongCacheEntry == 0 ? 0 : cardinality[firstWrongCacheEntry - 1];
        for (int i = firstWrongCacheEntry; i < rangeCount(); i++) {
            cumulative += sortedRanges.get(i).size();
            this.cardinality[i] = cumulative;
        }
        firstWrongCacheEntry = rangeCount();
        assert cardinality.length >= rangeCount() : this;
    }

    public RangeSet subsetForPositions(RangeSet positions, boolean reversed) {
        if (reversed) {
            throw new UnsupportedOperationException("reversed=true");
        }
        if (positions.isEmpty() || isEmpty()) {
            return empty();
        }
        ensureCardinalityCache();

        List<Range> ranges = new ArrayList<>();

        Iterator<Range> positionsIter = positions.rangeIterator();

        int from = 0;
        while (positionsIter.hasNext()) {
            Range nextPosRange = positionsIter.next();
            if (nextPosRange.getFirst() > size()) {
                // Entire range is past the end - since ranges are sorted, we're done
                break;
            } else if (nextPosRange.getFirst() == size()) {
                ranges.add(new Range(getLastRow(), getLastRow()));
                break;
            }
            long rangeToTake = nextPosRange.size();

            int pos = Arrays.binarySearch(cardinality, from, rangeCount(), nextPosRange.getFirst() + 1);

            long first;
            Range target;
            long offset;
            if (pos >= 0) {
                // Position matches the last item in the current range
                target = sortedRanges.get(pos);
                offset = 1;
            } else {
                // Position matches an earlier item in
                pos = -pos - 1;
                target = sortedRanges.get(pos);
                long c = cardinality[pos];
                offset = c - nextPosRange.getFirst();// positive value to offset backwards from the end of target
            }
            assert target != null : this + ".subsetForPositions(" + positions + ")";
            assert offset >= 0 && offset <= target.size() : offset;
            first = target.getLast() - offset + 1;

            while (rangeToTake > 0) {
                long count = Math.min(offset, rangeToTake);
                Range res = new Range(first, first + count - 1);
                assert count == res.size();
                ranges.add(res);

                rangeToTake -= count;
                pos++;
                if (pos >= rangeCount()) {
                    break;
                }
                target = sortedRanges.get(pos);
                first = target.getFirst();
                offset = target.size();
            }

            from = pos - 1;
        }
        RangeSet result = RangeSet.fromSortedRanges(ranges.toArray(new Range[0]));
        assert result.size() <= positions.size();
        return result;
    }

    /**
     *
     * @param keys
     * @return
     */
    public RangeSet invert(RangeSet keys) {
        if (keys.isEmpty()) {
            return empty();
        }
        if (isEmpty()) {
            throw new IllegalArgumentException("Keys not found: " + keys);
        }
        ensureCardinalityCache();

        List<Range> positions = new ArrayList<>();

        int from = 0;

        Iterator<Range> keysIter = keys.rangeIterator();
        long startPos = -1;
        long endPos = Long.MIN_VALUE;
        while (keysIter.hasNext()) {
            Range nextKeyRange = keysIter.next();

            int index = Collections.binarySearch(sortedRanges.subList(from, sortedRanges.size()), nextKeyRange);
            if (index < 0) {
                index = -index - 2;// examine the previous element
            }
            index += from;
            if (index < 0) {
                throw new IllegalArgumentException("Key " + nextKeyRange.getFirst() + " not found");
            }
            Range target = sortedRanges.get(index);

            long newStartPos =
                    (index == 0 ? 0 : this.cardinality[index - 1]) + (nextKeyRange.getFirst() - target.getFirst());
            if (newStartPos - 1 == endPos) {
                // nothing to do, only grow the existing range
                // endPos = newStartPos + (nextKeyRange.size() - 1);
            } else {
                // append the existing range and start a new one
                if (endPos != Long.MIN_VALUE) {
                    positions.add(new Range(startPos, endPos));
                }

                startPos = newStartPos;
                // endPos = startPos + (nextKeyRange.size() - 1);
            }
            endPos = newStartPos + (nextKeyRange.size() - 1);

            from = index;
        }
        assert endPos != Long.MIN_VALUE;
        positions.add(new Range(startPos, endPos));

        RangeSet result = RangeSet.fromSortedRanges(positions.toArray(new Range[0]));
        assert result.size() == keys.size();
        return result;
    }


    public long get(long key) {
        if (key == 0) {
            return getFirstRow();
        }
        if (key >= size()) {
            return -1;
        }
        ensureCardinalityCache();

        int pos = Arrays.binarySearch(cardinality, 0, sortedRanges.size(), key);

        if (pos >= 0) {
            return sortedRanges.get(pos + 1).getFirst();
        }
        Range target = sortedRanges.get(-pos - 1);
        long c = cardinality[-pos - 1];
        long offset = c - key;// positive value to offset backwards from the end of target
        assert offset >= 0;
        return target.getLast() - offset + 1;
    }

    /**
     * Removes all keys in the provided rangeset that are present in this.
     *
     * @param other the rows to remove
     * @return any removed keys
     */
    public RangeSet extract(RangeSet other) {
        RangeSet populatedCopy = copy();
        populatedCopy.removeRangeSet(other);
        RangeSet removed = copy();
        removed.removeRangeSet(populatedCopy);
        removeRangeSet(removed);
        return removed;
    }
}
