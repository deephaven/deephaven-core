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

    public static RangeSet fromSortedRanges(Range[] sortedRanges) {
        assert orderedAndNonOverlapping(sortedRanges) : Arrays.toString(sortedRanges);
        RangeSet rangeSet = new RangeSet();
        rangeSet.sortedRanges.addAll(Arrays.asList(sortedRanges));
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

    private List<Range> sortedRanges = new ArrayList<>();

    private int firstWrongCacheEntry = 0;
    private long[] cardinality = new long[0];

    public void addRangeSet(RangeSet rangeSet) {
        if (rangeCount() == 0 && rangeSet.rangeCount() != 0) {
            sortedRanges = new ArrayList<>(rangeSet.sortedRanges);
            poisonCache(0);
        } else {
            rangeSet.rangeIterator().forEachRemaining(this::addRange);
        }
    }

    public void addRange(Range range) {
        // if empty, add as the only entry
        if (rangeCount() == 0) {
            sortedRanges.add(range);
            poisonCache(0);
            return;
        }
        // if one other entry, test if before, after, or overlapping
        if (rangeCount() == 1) {
            Range existing = sortedRanges.get(0);
            Range overlap = range.overlap(existing);
            if (overlap != null) {
                sortedRanges.set(0, overlap);
                poisonCache(0);
            } else if (existing.compareTo(range) < 0) {
                sortedRanges.add(range);
                poisonCache(1);
            } else {
                assert existing.compareTo(range) > 0;
                sortedRanges.add(0, range);
                poisonCache(0);
            }
            return;
        }

        // if more than one other entry, binarySearch to find before and after entry, and test both for overlapping
        int index = Collections.binarySearch(sortedRanges, range);
        if (index >= 0) {

            // starting with that item, check to see if each following item is part of the existing range
            // we know that no range before it will need to be considered, since the set should previously
            // have been broken into non-contiguous ranges
            Range merged = range;
            int end = rangeCount() - 1;
            for (int i = index; i < rangeCount(); i++) {
                Range existing = sortedRanges.get(i);
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
            sortedRanges.set(index, merged);
            sortedRanges.subList(index + 1, end + 1).clear();
            poisonCache(index);
        } else {
            int proposedIndex = -(index) - 1;
            Range merged = range;
            // test the item before the proposed location (if any), try to merge it
            if (proposedIndex > 0) {
                Range before = sortedRanges.get(proposedIndex - 1);
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
            int end = rangeCount() - 1;
            // until we quit finding matches, test subsequent items
            for (int i = proposedIndex; i < rangeCount(); i++) {
                Range existing = sortedRanges.get(i);
                Range overlap = existing.overlap(merged);
                if (overlap == null) {
                    // stop at the item before this one
                    end = i - 1;
                    break;
                }
                merged = overlap;
            }
            int newLength = rangeCount() - (end - proposedIndex);
            assert newLength > 0 && newLength <= rangeCount() + 1;
            if (end == proposedIndex) {
                sortedRanges.set(proposedIndex, merged);
            } else if (newLength < rangeCount()) {
                sortedRanges.set(proposedIndex, merged);
                sortedRanges.subList(proposedIndex + 1, end + 1).clear();
            } else {
                sortedRanges.add(proposedIndex, merged);
            }
            poisonCache(proposedIndex);
        }
    }

    public void removeRangeSet(RangeSet rangeSet) {
        rangeSet.rangeIterator().forEachRemaining(this::removeRange);
    }

    public void removeRange(Range range) {
        // if empty, nothing to do
        if (rangeCount() == 0) {
            return;
        }

        // search the sorted list of ranges and find where the current range starts. two case here when using
        // binarySearch, either the removed range starts in the same place as an existing range starts, or
        // it starts before an item (and so we check the item before and the item after)
        int index = Collections.binarySearch(sortedRanges, range);
        if (index < 0) {
            // adjusted index notes where the item would be if it were added, minus _one more_ to see if
            // it overlaps the item before it. To compute "the position where the new item belongs", we
            // would do (-index - 1), so to examine one item prior to that we'll subtract one more. Then,
            // to confirm that we are inserting in a valid position, take the max of that value and zero.
            index = Math.max(0, -index - 2);
        }

        int beforeCount = -1;
        int toRemove = 0;
        for (; index < rangeCount(); index++) {
            Range toCheck = sortedRanges.get(index);
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

                sortedRanges.set(index, remaining[0]);
                sortedRanges.add(index + 1, remaining[1]);
                poisonCache(index);

                return;
            }
            if (remaining.length == 1) {
                // swap shortened item and move on
                sortedRanges.set(index, remaining[0]);
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
            sortedRanges.subList(beforeCount, beforeCount + toRemove).clear();
            poisonCache(beforeCount);
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
        return sortedRanges.iterator();
    }

    public PrimitiveIterator.OfLong indexIterator() {
        return sortedRanges
                .stream()
                .flatMapToLong(range -> LongStream.rangeClosed(range.getFirst(), range.getLast()))
                .iterator();
    }

    public int rangeCount() {
        return sortedRanges.size();
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

    public long get(long key) {
        if (key == 0) {
            return getFirstRow();
        }
        ensureCardinalityCache();

        int pos = Arrays.binarySearch(cardinality, key);

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
