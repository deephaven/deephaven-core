//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.data;

import java.util.ArrayList;
import java.util.Arrays;
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

    public static RangeSet fromSortedRanges(Range[] sortedRanges) {
        assert orderedAndNonOverlapping(sortedRanges) : Arrays.toString(sortedRanges);
        RangeSet rangeSet = new RangeSet();
        rangeSet.sortedRanges = sortedRanges;
        return rangeSet;
    }

    private static boolean orderedAndNonOverlapping(Range[] sortedRanges) {
        long lastSeen = -1;
        for (int i = 0; i < sortedRanges.length; i++) {
            if (lastSeen >= sortedRanges[i].getFirst()) {
                return false;
            }
            lastSeen = sortedRanges[i].getLast();
        }
        return true;
    }

    private Range[] sortedRanges = new Range[0];

    private int firstWrongCacheEntry = 0;
    private long[] cardinality = new long[0];

    public void addRangeSet(RangeSet rangeSet) {
        if (sortedRanges.length == 0 && rangeSet.sortedRanges.length != 0) {
            sortedRanges = new Range[rangeSet.sortedRanges.length];
            System.arraycopy(rangeSet.sortedRanges, 0, sortedRanges, 0, sortedRanges.length);
            poisonCache(0);
        } else {
            rangeSet.rangeIterator().forEachRemaining(this::addRange);
        }
    }

    public void addRange(Range range) {
        // if empty, add as the only entry
        if (sortedRanges.length == 0) {
            sortedRanges = new Range[] {range};
            poisonCache(0);
            return;
        }
        // if one other entry, test if before, after, or overlapping
        if (sortedRanges.length == 1) {
            Range existing = sortedRanges[0];
            Range overlap = range.overlap(existing);
            if (overlap != null) {
                sortedRanges = new Range[] {overlap};
                poisonCache(0);
            } else if (existing.compareTo(range) < 0) {
                sortedRanges = new Range[] {existing, range};
                poisonCache(1);
            } else {
                assert existing.compareTo(range) > 0;
                sortedRanges = new Range[] {range, existing};
                poisonCache(0);
            }
            return;
        }

        // if more than one other entry, binarySearch to find before and after entry, and test both for overlapping
        int index = Arrays.binarySearch(sortedRanges, range);
        if (index >= 0) {

            // starting with that item, check to see if each following item is part of the existing range
            // we know that no range before it will need to be considered, since the set should previously
            // have been broken into non-contiguous ranges
            Range merged = range;
            int end = sortedRanges.length - 1;
            for (int i = index; i < sortedRanges.length; i++) {
                Range existing = sortedRanges[i];
                // there is an item with the same start, either new item falls within it, or should replace it
                Range overlap = existing.overlap(merged);

                if (overlap == null) {
                    // index before this one is the last item to be replaced
                    end = i - 1;
                    break;
                }
                if (overlap.equals(existing)) {
                    // the entire range to be added existed within an existing range, we're done
                    return;
                }

                // grow the region used for replacing
                merged = overlap;
            }
            // splice out [index, end] items, replacing with the newly grown overlap object (may be the same
            // size, and only replacing one item)
            int newLength = sortedRanges.length - (end - index);
            Range[] newArray = new Range[newLength];
            if (index > 0) {
                System.arraycopy(sortedRanges, 0, newArray, 0, index);
            }
            newArray[index] = merged;
            poisonCache(index);
            if (end < sortedRanges.length - 1) {
                System.arraycopy(sortedRanges, end + 1, newArray, index + 1, sortedRanges.length - 1 - end);
            }
            sortedRanges = newArray;
        } else {
            int proposedIndex = -(index) - 1;
            Range merged = range;
            // test the item before the proposed location (if any), try to merge it
            if (proposedIndex > 0) {
                Range before = sortedRanges[proposedIndex - 1];
                Range overlap = before.overlap(merged);
                if (overlap != null) {
                    // replace the range that we are merging, and start the slice here instead
                    merged = overlap;
                    proposedIndex--;
                    // TODO this will make the loop start here, considering this item twice. not ideal, but not a big
                    // deal either
                }
            }
            // "end" represents the last item that needs to be merged in to the newly added item. if no items are to be
            // merged in, then end will be proposedIndex-1, meaning nothing gets merged in, and the array will grow
            // instead of shrinking.
            // if we never find an item we cannot merge with, the end of the replaced range is the last item of the old
            // array, which could result in the new array having as little as only 1 item
            int end = sortedRanges.length - 1;
            // until we quit finding matches, test subsequent items
            for (int i = proposedIndex; i < sortedRanges.length; i++) {
                Range existing = sortedRanges[i];
                Range overlap = existing.overlap(merged);
                if (overlap == null) {
                    // stop at the item before this one
                    end = i - 1;
                    break;
                }
                merged = overlap;
            }
            int newLength = sortedRanges.length - (end - proposedIndex);
            assert newLength > 0 && newLength <= sortedRanges.length + 1;
            Range[] newArray = new Range[newLength];
            if (proposedIndex > 0) {
                System.arraycopy(sortedRanges, 0, newArray, 0, proposedIndex);
            }
            newArray[proposedIndex] = merged;
            poisonCache(proposedIndex);
            if (end < sortedRanges.length - 1) {
                System.arraycopy(sortedRanges, end + 1, newArray, proposedIndex + 1, sortedRanges.length - (end + 1));
            }
            sortedRanges = newArray;
        }
    }

    public void removeRangeSet(RangeSet rangeSet) {
        rangeSet.rangeIterator().forEachRemaining(this::removeRange);
    }

    public void removeRange(Range range) {
        // if empty, nothing to do
        if (sortedRanges.length == 0) {
            return;
        }

        // search the sorted list of ranges and find where the current range starts. two case here when using
        // binarySearch, either the removed range starts in the same place as an existing range starts, or
        // it starts before an item (and so we check the item before and the item after)
        int index = Arrays.binarySearch(sortedRanges, range);
        if (index < 0) {
            // adjusted index notes where the item would be if it were added, minus _one more_ to see if
            // it overlaps the item before it. To compute "the position where the new item belongs", we
            // would do (-index - 1), so to examine one item prior to that we'll subtract one more. Then,
            // to confirm that we are inserting in a valid position, take the max of that value and zero.
            index = Math.max(0, -index - 2);
        }

        int beforeCount = -1;
        int toRemove = 0;
        for (; index < sortedRanges.length; index++) {
            Range toCheck = sortedRanges[index];
            if (toCheck.getFirst() > range.getLast()) {
                break;// done, this is entirely after the range we're removing
            }
            if (toCheck.getLast() < range.getFirst()) {
                continue;// skip, we don't overlap at all yet
            }
            Range[] remaining = toCheck.minus(range);
            assert remaining != null : "Only early ranges are allowed to not match at all";

            if (remaining.length == 2) {
                // Removed region is entirely within the range we are checking:
                // Splice in the one extra item and we're done - this entry
                // both started before and ended after the removed section,
                // so we don't even "break", we just return
                assert toCheck.getFirst() < range.getFirst() : "Expected " + range + " to start after " + toCheck;
                assert toCheck.getLast() > range.getLast() : "Expected " + range + " to end after " + toCheck;
                assert toRemove == 0 && beforeCount == -1
                        : "Expected that no previous items in the RangeSet had been removed toRemove=" + toRemove
                                + ", beforeCount=" + beforeCount;

                Range[] replacement = new Range[sortedRanges.length + 1];
                if (index > 0) {
                    System.arraycopy(sortedRanges, 0, replacement, 0, index);
                }
                replacement[index] = remaining[0];
                replacement[index + 1] = remaining[1];
                poisonCache(index);
                System.arraycopy(sortedRanges, index + 1, replacement, index + 2, sortedRanges.length - (index + 1));

                sortedRanges = replacement;

                return;
            }
            if (remaining.length == 1) {
                // swap shortened item and move on
                sortedRanges[index] = remaining[0];
                poisonCache(index);
            } else {
                assert remaining.length == 0 : "Array contains a surprising number of items: " + remaining.length;

                // splice out this item as nothing exists here any more and move on
                if (toRemove == 0) {
                    beforeCount = index;
                }
                toRemove++;
            }

        }
        if (toRemove > 0) {
            Range[] replacement = new Range[sortedRanges.length - toRemove];
            System.arraycopy(sortedRanges, 0, replacement, 0, beforeCount);
            System.arraycopy(sortedRanges, beforeCount + toRemove, replacement, beforeCount,
                    sortedRanges.length - beforeCount - toRemove);
            poisonCache(beforeCount + 1);

            sortedRanges = replacement;
        } else {
            assert beforeCount == -1 : "No items to remove, but beforeCount set?";
        }
    }

    /**
     * a new iterator over all indexes in this collection.
     * 
     * @return Iterator of {@link Range}
     */
    public Iterator<Range> rangeIterator() {
        return Arrays.asList(sortedRanges).iterator();
    }

    public PrimitiveIterator.OfLong indexIterator() {
        return Arrays.stream(sortedRanges)
                .flatMapToLong(range -> LongStream.rangeClosed(range.getFirst(), range.getLast()))
                .iterator();
    }

    public int rangeCount() {
        return sortedRanges.length;
    }

    /**
     * The total count of items contained in this collection. In some cases this can be expensive to compute, and
     * generally should not be needed except for debugging purposes, or preallocating space (i.e., do not call this
     * property each time through a loop).
     * 
     * @return long
     */
    public long size() {
        if (sortedRanges.length == 0) {
            return 0;
        }
        ensureCardinalityCache();
        return cardinality[cardinality.length - 1];
    }

    public boolean isEmpty() {
        return sortedRanges.length == 0;
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
        int index = Arrays.binarySearch(sortedRanges, range);
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
        Range target = sortedRanges[index];
        if (range.getFirst() <= target.getLast() && range.getLast() >= target.getFirst()) {
            return true;
        }

        // Check if there is a later item, and if there is an overlap with it
        index++;
        if (index >= sortedRanges.length) {
            return false;
        }
        target = sortedRanges[index];
        return range.getFirst() <= target.getLast() && range.getLast() >= target.getFirst();
    }

    @Override
    public String toString() {
        return "RangeSet{" +
                "sortedRanges=" + Arrays.toString(sortedRanges) +
                '}';
    }

    public long getFirstRow() {
        return sortedRanges[0].getFirst();
    }

    public long getLastRow() {
        return sortedRanges[sortedRanges.length - 1].getLast();
    }

    public RangeSet copy() {
        RangeSet copy = new RangeSet();
        copy.sortedRanges = Arrays.copyOf(sortedRanges, sortedRanges.length);
        return copy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final RangeSet rangeSet = (RangeSet) o;
        return Arrays.equals(sortedRanges, rangeSet.sortedRanges);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(sortedRanges);
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
        if (firstWrongCacheEntry == sortedRanges.length) {
            return;
        }
        if (cardinality.length < sortedRanges.length) {
            long[] replacement = new long[sortedRanges.length];
            System.arraycopy(cardinality, 0, replacement, 0, cardinality.length);
            cardinality = replacement;
        }
        assert firstWrongCacheEntry >= 0 : this;
        long cumulative = firstWrongCacheEntry == 0 ? 0 : cardinality[firstWrongCacheEntry - 1];
        for (int i = firstWrongCacheEntry; i < sortedRanges.length; i++) {
            cumulative += sortedRanges[i].size();
            this.cardinality[i] = cumulative;
        }
        firstWrongCacheEntry = sortedRanges.length;
        assert cardinality.length >= sortedRanges.length : this;
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

            int pos = Arrays.binarySearch(cardinality, from, sortedRanges.length, nextPosRange.getFirst() + 1);

            long first;
            Range target;
            long offset;
            if (pos >= 0) {
                // Position matches the last item in the current range
                target = sortedRanges[pos];
                offset = 1;
            } else {
                // Position matches an earlier item in
                pos = -pos - 1;
                target = sortedRanges[pos];
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
                if (pos >= sortedRanges.length) {
                    break;
                }
                target = sortedRanges[pos];
                first = target.getFirst();
                offset = target.size();
            }

            from = pos - 1;
        }
        RangeSet result = RangeSet.fromSortedRanges(ranges.toArray(new Range[0]));
        assert result.size() <= positions.size();
        return result;
    }

    public long get(long key) {
        if (key == 0) {
            return sortedRanges[0].getFirst();
        }
        ensureCardinalityCache();

        int pos = Arrays.binarySearch(cardinality, key);

        if (pos >= 0) {
            return sortedRanges[pos + 1].getFirst();
        }
        Range target = sortedRanges[-pos - 1];
        long c = cardinality[-pos - 1];
        long offset = c - key;// positive value to offset backwards from the end of target
        assert offset >= 0;
        return target.getLast() - offset + 1;
    }

    /**
     * Removes all keys in the provided rangeset that are present in this.
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
