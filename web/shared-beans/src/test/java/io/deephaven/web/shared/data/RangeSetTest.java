package io.deephaven.web.shared.data;

import com.google.common.collect.Collections2;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import static org.junit.Assert.*;

public class RangeSetTest {

    @Test
    public void testEmpty() {
        RangeSet empty = RangeSet.empty();

        assertEquals(0, empty.size());
        assertFalse(empty.rangeIterator().hasNext());
        assertFalse(empty.indexIterator().hasNext());
    }

    @Test
    public void testSingleItem() {
        RangeSet item = new RangeSet();
        item.addRange(new Range(1, 1));

        assertEquals(1, item.size());
        assertEquals(1, asList(item).size());
        assertEquals(new Range(1, 1), asList(item).get(0));

        List<Long> allIndicies = new ArrayList<>();
        item.indexIterator().forEachRemaining((LongConsumer) allIndicies::add);
        assertEquals(1, allIndicies.size());
        assertEquals(Long.valueOf(1), allIndicies.get(0));
    }

    private List<Range> asList(RangeSet rangeSet) {
        List<Range> allRanges = new ArrayList<>();
        rangeSet.rangeIterator().forEachRemaining(allRanges::add);
        return allRanges;
    }

    private RangeSet of(Range... ranges) {
        RangeSet rs = new RangeSet();
        for (Range range : ranges) {
            rs.addRange(range);
        }
        return rs;
    }

    @Test
    public void testOverlappingRanges() {
        RangeSet rangeSet = new RangeSet();
        rangeSet.addRange(new Range(1, 20));
        assertEquals(20, rangeSet.size());
        assertEquals(Collections.singletonList(new Range(1, 20)), asList(rangeSet));

        // exactly the same
        rangeSet.addRange(new Range(1, 20));
        assertEquals(20, rangeSet.size());
        assertEquals(Collections.singletonList(new Range(1, 20)), asList(rangeSet));

        // entirely contained
        rangeSet.addRange(new Range(6, 15));
        assertEquals(20, rangeSet.size());
        assertEquals(Collections.singletonList(new Range(1, 20)), asList(rangeSet));

        // overlapping, sharing no boundaries
        rangeSet.addRange(new Range(18, 25));
        assertEquals(25, rangeSet.size());
        assertEquals(Collections.singletonList(new Range(1, 25)), asList(rangeSet));

        // overlapping, sharing one boundary
        rangeSet.addRange(new Range(0, 1));
        assertEquals(26, rangeSet.size());
        assertEquals(Collections.singletonList(new Range(0, 25)), asList(rangeSet));
    }

    @Test
    public void testOverlappingRangesInDifferentOrder() {

        // add three items in each possible order to a rangeset, ensure results are always the same
        Range rangeA = new Range(100, 108);
        Range rangeB = new Range(105, 112);
        Range rangeC = new Range(110, 115);
        Collections2.permutations(Arrays.asList(rangeA, rangeB, rangeC)).forEach(list -> {
            RangeSet rangeSet = new RangeSet();
            list.forEach(rangeSet::addRange);

            assertEquals(16, rangeSet.size());
            assertEquals(list.toString(), Collections.singletonList(new Range(100, 115)),
                asList(rangeSet));
        });

        // same three items, but with another before that will not overlap with them
        Range before = new Range(0, 4);
        Collections2.permutations(Arrays.asList(before, rangeA, rangeB, rangeC)).forEach(list -> {
            RangeSet rangeSet = new RangeSet();
            list.forEach(rangeSet::addRange);

            assertEquals(21, rangeSet.size());
            assertEquals(list.toString(), Arrays.asList(new Range(0, 4), new Range(100, 115)),
                asList(rangeSet));
        });

        // same three items, but with another following that will not overlap with them
        Range after = new Range(200, 204);
        Collections2.permutations(Arrays.asList(after, rangeA, rangeB, rangeC)).forEach(list -> {
            RangeSet rangeSet = new RangeSet();
            list.forEach(rangeSet::addRange);

            assertEquals(21, rangeSet.size());
            assertEquals(list.toString(), Arrays.asList(new Range(100, 115), new Range(200, 204)),
                asList(rangeSet));
        });
    }

    @Test
    public void testNonOverlappingRanges() {
        RangeSet rangeSet = new RangeSet();
        rangeSet.addRange(new Range(1, 20));

        // touching, without sharing a boundary
        rangeSet.addRange(new Range(21, 30));
        assertEquals(Collections.singletonList(new Range(1, 30)), asList(rangeSet));
        assertEquals(30, rangeSet.size());

        // not touching at all
        rangeSet.addRange(new Range(41, 50));
        assertEquals(40, rangeSet.size());
        assertEquals(Arrays.asList(new Range(1, 30), new Range(41, 50)), asList(rangeSet));
    }

    @Test
    public void testIncludesAllOf() {
        RangeSet rangeSet = new RangeSet();
        rangeSet.addRange(new Range(0, 19));
        rangeSet.addRange(new Range(50, 54));

        assertTrue(rangeSet.includesAllOf(RangeSet.ofRange(0, 19)));
        assertTrue(rangeSet.includesAllOf(RangeSet.ofRange(50, 54)));

        rangeSet.indexIterator().forEachRemaining((LongConsumer) l -> {
            assertTrue(rangeSet.includesAllOf(RangeSet.ofRange(l, l)));
        });

        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(0, 20)));
        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(10, 20)));
        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(19, 20)));

        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(19, 30)));
        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(20, 30)));
        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(21, 30)));

        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(30, 40)));

        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(40, 49)));
        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(40, 50)));
        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(40, 41)));
        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(40, 54)));

        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(49, 54)));
        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(50, 55)));
        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(50, 60)));

        assertFalse(rangeSet.includesAllOf(RangeSet.ofRange(54, 60)));
    }


    @Test
    public void testRemove() {
        // Remove when nothing is present
        RangeSet rangeSet = RangeSet.empty();
        rangeSet.removeRange(new Range(3, 5));
        assertEquals(RangeSet.empty(), rangeSet);
        // Remove until nothing is left
        rangeSet = RangeSet.ofRange(0, 9);
        rangeSet.removeRange(new Range(0, 9));
        assertEquals(RangeSet.empty(), rangeSet);
        rangeSet = RangeSet.ofRange(1, 8);
        rangeSet.removeRange(new Range(0, 9));
        assertEquals(RangeSet.empty(), rangeSet);

        // Remove section before/between/after any actual existing element (no effect)
        rangeSet = RangeSet.ofRange(5, 10);
        rangeSet.removeRange(new Range(0, 3));
        rangeSet.removeRange(new Range(11, 12));
        assertEquals(RangeSet.ofRange(5, 10), rangeSet);
        rangeSet = RangeSet.ofItems(5, 8, 10);
        rangeSet.removeRange(new Range(6, 7));
        rangeSet.removeRange(new Range(9, 9));
        assertEquals(RangeSet.ofItems(5, 8, 10), rangeSet);

        // Remove the very first or very last item from a region
        rangeSet = RangeSet.ofRange(5, 10);
        rangeSet.removeRange(new Range(5, 5));
        assertEquals(RangeSet.ofRange(6, 10), rangeSet);

        rangeSet = RangeSet.ofRange(5, 10);
        rangeSet.removeRange(new Range(10, 10));
        assertEquals(RangeSet.ofRange(5, 9), rangeSet);

        // Remove section overlapping-before/after first/last/middle
        Supplier<RangeSet> create = () -> of(
            new Range(5, 10),
            new Range(15, 20),
            new Range(25, 30));

        rangeSet = create.get();
        rangeSet.removeRange(new Range(3, 6));
        assertEquals(of(
            new Range(7, 10),
            new Range(15, 20),
            new Range(25, 30)), rangeSet);
        rangeSet = create.get();
        rangeSet.removeRange(new Range(8, 12));
        assertEquals(of(
            new Range(5, 7),
            new Range(15, 20),
            new Range(25, 30)), rangeSet);

        rangeSet = create.get();
        rangeSet.removeRange(new Range(12, 16));
        assertEquals(of(
            new Range(5, 10),
            new Range(17, 20),
            new Range(25, 30)), rangeSet);
        rangeSet = create.get();
        rangeSet.removeRange(new Range(18, 22));
        assertEquals(of(
            new Range(5, 10),
            new Range(15, 17),
            new Range(25, 30)), rangeSet);

        rangeSet = create.get();
        rangeSet.removeRange(new Range(22, 27));
        assertEquals(of(
            new Range(5, 10),
            new Range(15, 20),
            new Range(28, 30)), rangeSet);
        rangeSet = create.get();
        rangeSet.removeRange(new Range(26, 31));
        assertEquals(of(
            new Range(5, 10),
            new Range(15, 20),
            new Range(25, 25)), rangeSet);

        // Remove section entirely within another range, touching start or end or none
        rangeSet = create.get();
        rangeSet.removeRange(new Range(5, 7));
        assertEquals(of(
            new Range(8, 10),
            new Range(15, 20),
            new Range(25, 30)), rangeSet);
        rangeSet = create.get();
        rangeSet.removeRange(new Range(7, 10));
        assertEquals(of(
            new Range(5, 6),
            new Range(15, 20),
            new Range(25, 30)), rangeSet);
        rangeSet = create.get();
        rangeSet.removeRange(new Range(6, 8));
        assertEquals(of(
            new Range(5, 5),
            new Range(9, 10),
            new Range(15, 20),
            new Range(25, 30)), rangeSet);

        rangeSet = create.get();
        rangeSet.removeRange(new Range(15, 17));
        assertEquals(of(
            new Range(5, 10),
            new Range(18, 20),
            new Range(25, 30)), rangeSet);
        rangeSet = create.get();
        rangeSet.removeRange(new Range(17, 20));
        assertEquals(of(
            new Range(5, 10),
            new Range(15, 16),
            new Range(25, 30)), rangeSet);
        rangeSet = create.get();
        rangeSet.removeRange(new Range(16, 18));
        assertEquals(of(
            new Range(5, 10),
            new Range(15, 15),
            new Range(19, 20),
            new Range(25, 30)), rangeSet);

        rangeSet = create.get();
        rangeSet.removeRange(new Range(25, 27));
        assertEquals(of(
            new Range(5, 10),
            new Range(15, 20),
            new Range(28, 30)), rangeSet);
        rangeSet = create.get();
        rangeSet.removeRange(new Range(27, 30));
        assertEquals(of(
            new Range(5, 10),
            new Range(15, 20),
            new Range(25, 26)), rangeSet);
        rangeSet = create.get();
        rangeSet.removeRange(new Range(26, 28));
        assertEquals(of(
            new Range(5, 10),
            new Range(15, 20),
            new Range(25, 25),
            new Range(29, 30)), rangeSet);


        // Remove section overlapping 2+ sections
        rangeSet = create.get();
        rangeSet.removeRange(new Range(5, 20));
        assertEquals(RangeSet.ofRange(25, 30), rangeSet);

        rangeSet = create.get();
        rangeSet.removeRange(new Range(15, 30));
        assertEquals(RangeSet.ofRange(5, 10), rangeSet);

        rangeSet = create.get();
        rangeSet.removeRange(new Range(5, 30));
        assertEquals(RangeSet.empty(), rangeSet);

        rangeSet = create.get();
        rangeSet.removeRange(new Range(4, 16));
        assertEquals(of(
            new Range(17, 20),
            new Range(25, 30)), rangeSet);

        rangeSet = create.get();
        rangeSet.removeRange(new Range(6, 21));
        assertEquals(of(
            new Range(5, 5),
            new Range(25, 30)), rangeSet);

        rangeSet = create.get();
        rangeSet.removeRange(new Range(9, 26));
        assertEquals(of(
            new Range(5, 8),
            new Range(27, 30)), rangeSet);

        rangeSet = create.get();
        rangeSet.removeRange(new Range(11, 31));
        assertEquals(of(
            new Range(5, 10)), rangeSet);

        rangeSet = create.get();
        rangeSet.removeRange(new Range(4, 31));
        assertEquals(RangeSet.empty(), rangeSet);



        // Remove exact section
        rangeSet = create.get();
        rangeSet.removeRange(new Range(5, 10));
        assertEquals(of(
            new Range(15, 20),
            new Range(25, 30)), rangeSet);
        rangeSet = create.get();
        rangeSet.removeRange(new Range(15, 20));
        assertEquals(of(
            new Range(5, 10),
            new Range(25, 30)), rangeSet);
        rangeSet = create.get();
        rangeSet.removeRange(new Range(25, 30));
        assertEquals(of(
            new Range(5, 10),
            new Range(15, 20)), rangeSet);


    }

}
