package io.deephaven.web.shared.data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.stream.LongStream;

public class RangeSet implements Serializable {

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

    public void addRange(Range range) {
        // if empty, add as the only entry
        if (sortedRanges.length == 0) {
            sortedRanges = new Range[] {range};
            return;
        }
        // if one other entry, test if before, after, or overlapping
        if (sortedRanges.length == 1) {
            Range existing = sortedRanges[0];
            Range overlap = range.overlap(existing);
            if (overlap != null) {
                sortedRanges = new Range[] {overlap};
            } else if (existing.compareTo(range) < 0) {
                sortedRanges = new Range[] {existing, range};
            } else {
                assert existing.compareTo(range) > 0;
                sortedRanges = new Range[] {range, existing};
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
            if (end < sortedRanges.length - 1) {
                System.arraycopy(sortedRanges, end + 1, newArray, proposedIndex + 1, sortedRanges.length - (end + 1));
            }
            sortedRanges = newArray;
        }
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
                System.arraycopy(sortedRanges, index + 1, replacement, index + 2, sortedRanges.length - (index + 1));

                sortedRanges = replacement;

                return;
            }
            if (remaining.length == 1) {
                // swap shortened item and move on
                sortedRanges[index] = remaining[0];
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

            sortedRanges = replacement;
        } else {
            assert beforeCount == -1 : "No items to remove, but beforeCount set?";
        }
    }

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

    public long size() {
        return Arrays.stream(sortedRanges).mapToLong(Range::size).sum();
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


    Range[] getSortedRanges() {
        return sortedRanges;
    }

    void setSortedRanges(Range[] sortedRanges) {
        this.sortedRanges = sortedRanges;
    }
}
