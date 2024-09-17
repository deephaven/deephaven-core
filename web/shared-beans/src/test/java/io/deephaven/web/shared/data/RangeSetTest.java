//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.data;

import com.google.common.collect.Collections2;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PrimitiveIterator;
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

        rangeSet = new RangeSet();
        rangeSet.addRange(new Range(1, 1));
        rangeSet.addRange(new Range(20, 21));

        // shared start; ranges follow
        rangeSet.addRange(new Range(1, 10));
        assertEquals(12, rangeSet.size());
    }

    @Test
    public void testAddExistingRange() {
        RangeSet rangeSet = RangeSet.ofRange(5, 10);
        rangeSet.addRange(new Range(5, 10));
        rangeSet.addRange(new Range(5, 6));
        rangeSet.addRange(new Range(6, 8));
        rangeSet.addRange(new Range(8, 10));
        assertEquals(RangeSet.ofRange(5, 10), rangeSet);

        rangeSet = RangeSet.ofItems(5, 10, 15);
        rangeSet.addRange(new Range(5, 5));
        rangeSet.addRange(new Range(10, 10));
        rangeSet.addRange(new Range(15, 15));
        assertEquals(rangeSet, RangeSet.ofItems(5, 10, 15));

        rangeSet = RangeSet.ofItems(5, 6, 7, 11, 12, 13, 26, 27, 28);
        rangeSet.addRange(new Range(5, 7));
        rangeSet.addRange(new Range(11, 13));
        rangeSet.addRange(new Range(26, 28));

        rangeSet.addRange(new Range(5, 6));
        rangeSet.addRange(new Range(6, 6));
        rangeSet.addRange(new Range(6, 7));

        rangeSet.addRange(new Range(11, 12));
        rangeSet.addRange(new Range(12, 12));
        rangeSet.addRange(new Range(12, 13));

        rangeSet.addRange(new Range(26, 27));
        rangeSet.addRange(new Range(27, 27));
        rangeSet.addRange(new Range(27, 28));
        assertEquals(RangeSet.ofItems(5, 6, 7, 11, 12, 13, 26, 27, 28), rangeSet);
    }

    @Test
    public void testOverlappingRangesInDifferentOrder() {

        // add five ranges, where some overlap others, in each possible order to a rangeset, ensure results are always
        // the same
        Range rangeA = new Range(100, 108);
        Range rangeB = new Range(105, 112);
        Range rangeC = new Range(110, 115);
        Range rangeD = new Range(100, 113);
        Range rangeE = new Range(101, 115);
        Collections2.permutations(Arrays.asList(rangeA, rangeB, rangeC, rangeD, rangeE)).forEach(list -> {
            RangeSet rangeSet = new RangeSet();
            list.forEach(rangeSet::addRange);

            assertEquals(list.toString(), 16, rangeSet.size());
            assertEquals(list.toString(), Collections.singletonList(new Range(100, 115)), asList(rangeSet));
        });

        // same five items, but with another before that will not overlap with them
        Range before = new Range(0, 4);
        Collections2.permutations(Arrays.asList(before, rangeA, rangeB, rangeC, rangeD, rangeE)).forEach(list -> {
            RangeSet rangeSet = new RangeSet();
            list.forEach(rangeSet::addRange);

            assertEquals(21, rangeSet.size());
            assertEquals(list.toString(), Arrays.asList(new Range(0, 4), new Range(100, 115)), asList(rangeSet));
        });

        // same five items, but with another following that will not overlap with them
        Range after = new Range(200, 204);
        Collections2.permutations(Arrays.asList(after, rangeA, rangeB, rangeC, rangeD, rangeE)).forEach(list -> {
            RangeSet rangeSet = new RangeSet();
            list.forEach(rangeSet::addRange);

            assertEquals(21, rangeSet.size());
            assertEquals(list.toString(), Arrays.asList(new Range(100, 115), new Range(200, 204)), asList(rangeSet));
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
    public void testIncludesAnyOf() {
        RangeSet rangeSet = new RangeSet();
        rangeSet.addRange(new Range(0, 19));
        rangeSet.addRange(new Range(50, 54));

        assertTrue(rangeSet.includesAnyOf(new Range(0, 19)));
        assertTrue(rangeSet.includesAnyOf(new Range(50, 54)));

        rangeSet.indexIterator().forEachRemaining((LongConsumer) l -> {
            assertTrue(rangeSet.includesAnyOf(new Range(l, l)));
        });

        assertTrue(rangeSet.includesAnyOf(new Range(0, 20)));
        assertTrue(rangeSet.includesAnyOf(new Range(10, 20)));
        assertTrue(rangeSet.includesAnyOf(new Range(19, 20)));

        assertTrue(rangeSet.includesAnyOf(new Range(19, 30)));
        assertFalse(rangeSet.includesAnyOf(new Range(20, 30)));
        assertFalse(rangeSet.includesAnyOf(new Range(21, 30)));

        assertFalse(rangeSet.includesAnyOf(new Range(30, 40)));

        assertFalse(rangeSet.includesAnyOf(new Range(40, 49)));
        assertTrue(rangeSet.includesAnyOf(new Range(40, 50)));
        assertFalse(rangeSet.includesAnyOf(new Range(40, 41)));
        assertTrue(rangeSet.includesAnyOf(new Range(40, 54)));

        assertTrue(rangeSet.includesAnyOf(new Range(49, 54)));
        assertTrue(rangeSet.includesAnyOf(new Range(50, 55)));
        assertTrue(rangeSet.includesAnyOf(new Range(50, 60)));

        assertTrue(rangeSet.includesAnyOf(new Range(54, 60)));
        assertFalse(rangeSet.includesAnyOf(new Range(55, 60)));
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

    @Test
    public void testLarge() {
        long largeA = (long) Integer.MAX_VALUE + 10L;
        long largeB = (long) Integer.MAX_VALUE + (long) Integer.MAX_VALUE;
        long largeC = Long.MAX_VALUE - 100L;

        // Add to empty range
        RangeSet rangeSet = RangeSet.empty();
        rangeSet.addRange(new Range(0, largeA));
        assertEquals(of(new Range(0, largeA)), rangeSet);

        rangeSet = RangeSet.empty();
        rangeSet.addRange(new Range(largeA, largeA));
        assertEquals(of(new Range(largeA, largeA)), rangeSet);

        rangeSet = RangeSet.empty();
        rangeSet.addRange(new Range(largeA, largeB));
        assertEquals(of(new Range(largeA, largeB)), rangeSet);

        rangeSet = RangeSet.empty();
        rangeSet.addRange((new Range(0, largeC)));
        assertEquals(of(new Range(0, largeC)), rangeSet);

        rangeSet = RangeSet.empty();
        rangeSet.addRange(new Range(largeA, largeC));
        assertEquals(of(new Range(largeA, largeC)), rangeSet);

        // Remove when nothing is present
        rangeSet = RangeSet.empty();
        rangeSet.removeRange(new Range(0, largeA));
        assertEquals(RangeSet.empty(), rangeSet);

        // Remove until nothing is left
        rangeSet = RangeSet.ofRange(0, largeA);
        rangeSet.removeRange(new Range(0, largeA));
        assertEquals(RangeSet.empty(), rangeSet);

        rangeSet = RangeSet.ofRange(1, largeA);
        rangeSet.removeRange(new Range(0, largeA));
        assertEquals(RangeSet.empty(), rangeSet);

        rangeSet = RangeSet.ofRange(largeA, largeC);
        rangeSet.removeRange(new Range(0, largeC));
        assertEquals(RangeSet.empty(), rangeSet);

        // Remove section before/between/after any actual existing element (no effect)
        rangeSet = RangeSet.ofRange(largeA, largeB);
        rangeSet.removeRange(new Range(0, largeA - 2));
        rangeSet.removeRange(new Range(largeB + 1, largeB + 5));
        assertEquals(RangeSet.ofRange(largeA, largeB), rangeSet);

        rangeSet = RangeSet.ofItems(5, largeA, largeB);
        rangeSet.removeRange(new Range(6, largeA - 1));
        rangeSet.removeRange(new Range(largeA + 1, largeB - 1));
        assertEquals(RangeSet.ofItems(5, largeA, largeB), rangeSet);

        rangeSet = RangeSet.ofItems(largeA, largeB);
        rangeSet.removeRange(new Range(0, largeA - 1));
        rangeSet.removeRange(new Range(largeB + 1, largeC));
        assertEquals(RangeSet.ofItems(largeA, largeB), rangeSet);
    }

    @Test
    public void testSubsetForPostions() {
        RangeSet initialRange = RangeSet.ofItems(2, 4, 6, 8);
        assertEquals(RangeSet.ofItems(4, 8), initialRange.subsetForPositions(RangeSet.ofItems(1, 3), false));
        assertEquals(RangeSet.ofItems(4, 8), initialRange.subsetForPositions(RangeSet.ofItems(1, 3, 4), false));
        assertEquals(RangeSet.ofItems(4, 8), initialRange.subsetForPositions(RangeSet.ofItems(1, 3, 5), false));
        assertEquals(initialRange, initialRange.subsetForPositions(RangeSet.ofItems(0, 1, 2, 3, 4, 5, 100), false));
        assertEquals(initialRange, initialRange.subsetForPositions(RangeSet.ofItems(0, 1, 2, 3, 100), false));

        assertEquals(RangeSet.ofItems(4, 6, 8), initialRange.subsetForPositions(RangeSet.ofRange(1, 3), false));
        assertEquals(RangeSet.ofItems(2, 4, 6), initialRange.subsetForPositions(RangeSet.ofRange(0, 2), false));
        assertEquals(initialRange, initialRange.subsetForPositions(RangeSet.ofRange(0, 3), false));
        assertEquals(initialRange, initialRange.subsetForPositions(RangeSet.ofRange(0, 9), false));

        initialRange = RangeSet.ofRange(10, 109);
        assertEquals(RangeSet.ofItems(12, 14), initialRange.subsetForPositions(RangeSet.ofItems(2, 4), false));
        assertEquals(RangeSet.ofItems(12, 14), initialRange.subsetForPositions(RangeSet.ofItems(2, 4, 101), false));

        assertEquals(RangeSet.empty(), RangeSet.empty().subsetForPositions(RangeSet.ofItems(0), false));
        assertEquals(RangeSet.ofItems(99),
                RangeSet.ofRange(0, 99).subsetForPositions(RangeSet.ofRange(100, 104), false));

        initialRange = RangeSet.empty();
        assertEquals(0, initialRange.size());
        initialRange.addRange(new Range(0, 1));
        assertEquals(2, initialRange.size());
        initialRange.addRange(new Range(2, 3));
        assertEquals(4, initialRange.size());
        initialRange.removeRange(new Range(0, 3));
        assertEquals(0, initialRange.size());
        initialRange.addRange(new Range(0, 1));
        assertEquals(2, initialRange.size());

        initialRange = RangeSet.ofItems(1, 4, 5, 6);
        assertEquals(RangeSet.ofItems(1, 4, 5, 6), initialRange.subsetForPositions(RangeSet.ofRange(0, 3), false));
        assertEquals(RangeSet.ofItems(1, 5, 6), initialRange.subsetForPositions(RangeSet.ofItems(0, 2, 3), false));
        assertEquals(RangeSet.ofItems(1, 4, 6), initialRange.subsetForPositions(RangeSet.ofItems(0, 1, 3), false));
        assertEquals(RangeSet.ofItems(1, 4, 5), initialRange.subsetForPositions(RangeSet.ofItems(0, 1, 2), false));
        assertEquals(RangeSet.ofItems(1, 5), initialRange.subsetForPositions(RangeSet.ofItems(0, 2), false));
        assertEquals(RangeSet.ofItems(4, 5), initialRange.subsetForPositions(RangeSet.ofRange(1, 2), false));
        assertEquals(RangeSet.ofItems(4, 5, 6), initialRange.subsetForPositions(RangeSet.ofRange(1, 3), false));
        assertEquals(RangeSet.ofItems(5, 6), initialRange.subsetForPositions(RangeSet.ofRange(2, 3), false));
    }

    @Test
    public void testGet() {
        long[] rows = {0, 1, 4, 5, 7, 9};
        RangeSet initialRange = RangeSet.ofItems(rows);

        for (int i = 0; i < rows.length; i++) {
            assertEquals("i=" + i, rows[i], initialRange.get(i));
        }
        assertEquals(-1, initialRange.get(rows.length));
        assertEquals(-1, initialRange.get(rows.length + 1));
        assertEquals(-1, initialRange.get(rows.length + 100));

        initialRange.removeRange(new Range(0, 1));
    }

    @Test
    public void testShift() {
        RangeSet r = RangeSet.ofRange(0, 2);
        r.applyShifts(new ShiftedRange[] {
                new ShiftedRange(new Range(0, 2), 2)
        });
        assertEquals(RangeSet.ofRange(2, 4), r);
        r.applyShifts(new ShiftedRange[] {
                new ShiftedRange(new Range(2, 6), -2)
        });
        assertEquals(RangeSet.ofRange(0, 2), r);
        r.applyShifts(new ShiftedRange[] {
                new ShiftedRange(new Range(1, 2), 3)
        });
        assertEquals(RangeSet.ofItems(0, 4, 5), r);
        r.applyShifts(new ShiftedRange[] {
                new ShiftedRange(new Range(4, 4), -1)
        });
        assertEquals(RangeSet.ofItems(0, 3, 5), r);

        r = RangeSet.ofItems(0, 3, 4, 5, 6, 10);
        r.applyShifts(new ShiftedRange[] {
                new ShiftedRange(new Range(4, 4), 2),
                new ShiftedRange(new Range(5, 6), 3),
        });
        assertEquals(RangeSet.ofItems(0, 3, 6, 8, 9, 10), r);


        r = RangeSet.fromSortedRanges(new Range[] {
                new Range(0, 1),
                new Range(3, 5),
                new Range(7, 13),
                new Range(15, 19),
        });
        r.applyShifts(new ShiftedRange[] {
                new ShiftedRange(new Range(3, 4), -1),
                new ShiftedRange(new Range(7, 13), -1),
                new ShiftedRange(new Range(15, 17), -2),
        });
        assertEquals(RangeSet.fromSortedRanges(new Range[] {
                new Range(0, 3),
                new Range(5, 15),
                new Range(18, 19),
        }), r);


        r = RangeSet.fromSortedRanges(new Range[] {
                new Range(28972, 28987),
                new Range(28989, 29003),
                new Range(29005, 29011),
                new Range(29013, 29013),
                new Range(29015, 29018),
                new Range(29020, 29020),
                new Range(29022, 29024),
                new Range(29026, 29039),
        });
        r.applyShifts(new ShiftedRange[] {
                new ShiftedRange(new Range(28989, 29011), 2),
                new ShiftedRange(new Range(29013, 29013), 1),
                new ShiftedRange(new Range(29022, 29024), -1),
                new ShiftedRange(new Range(29026, 29026), -2),
        });
    }

    @Test
    public void testInvert() {
        RangeSet r = RangeSet.ofItems(4, 5, 7, 9, 10);

        assertEquals(RangeSet.ofRange(0, 4), r.invert(r));
        assertEquals(RangeSet.empty(), r.invert(RangeSet.empty()));
        assertEquals(RangeSet.ofItems(0, 2, 4), r.invert(RangeSet.ofItems(4, 7, 10)));
        assertEquals(RangeSet.ofItems(1, 2, 3), r.invert(RangeSet.ofItems(5, 7, 9)));

        RangeSet positions = RangeSet.ofItems(18, 37, 83, 88);
        RangeSet keys = RangeSet.fromSortedRanges(new Range[] {
                new Range(1073739467, 1073739511), new Range(1073739568, 1073739639), new Range(1073739700, 1073739767),
                new Range(1073739828, 1073739895), new Range(1073739956, 1073740023), new Range(1073740083, 1073740151),
                new Range(1073740214, 1073740279), new Range(1073740342, 1073740407), new Range(1073740464, 1073740535),
                new Range(1073740597, 1073740663), new Range(1073740725, 1073740791), new Range(1073740851, 1073740924),
                new Range(1073740937, 1073741052), new Range(1073741063, 1073741180), new Range(1073741189, 1073741308),
                new Range(1073741310, 1073741436), new Range(1073741440, 1073741564), new Range(1073741569, 1073741692),
                new Range(1073741696, 1073741948), new Range(1073741968, 1073742065), new Range(1073742096, 1073742184),
                new Range(1073742224, 1073742325), new Range(1073742352, 1073742454), new Range(1073742480, 1073742579),
                new Range(1073742608, 1073742702), new Range(1073742736, 1073742840), new Range(1073742864, 1073742967),
                new Range(1073742992, 1073743084), new Range(1073743120, 1073743218), new Range(1073743248, 1073743348),
                new Range(1073743376, 1073743465), new Range(1073743504, 1073743597), new Range(1073743632, 1073743741),
                new Range(1073743760, 1073743869), new Range(1073743888, 1073743912)
        });
        RangeSet selected = RangeSet.ofItems(1073739485, 1073739504, 1073739606, 1073739611);
        PrimitiveIterator.OfLong selectedIter = selected.indexIterator();
        PrimitiveIterator.OfLong positionsIter = positions.indexIterator();
        while (selectedIter.hasNext()) {
            assert positionsIter.hasNext();
            long s = selectedIter.nextLong();
            long p = positionsIter.nextLong();
            assertEquals(s, keys.get(p));
        }
        assert !positionsIter.hasNext();
        assertEquals(positions, keys.invert(selected));
    }
}
