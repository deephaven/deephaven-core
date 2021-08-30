package io.deephaven.web.shared.data;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RangeTest {

    @Test
    public void testCantMerge() {
        Range rangeA = new Range(0, 1);
        Range rangeB = new Range(3, 4);
        assertNull(rangeA.overlap(rangeB));
        assertNull(rangeB.overlap(rangeA));
    }

    @Test
    public void testOverlapMerge() {
        Range rangeA = new Range(0, 2);
        Range rangeB = new Range(2, 4);
        Range rangeC = new Range(3, 5);
        Range rangeD = new Range(4, 4);

        // share one item
        assertEquals(new Range(0, 4), rangeA.overlap(rangeB));
        assertEquals(new Range(0, 4), rangeB.overlap(rangeA));
        // share more than one item
        assertEquals(new Range(2, 5), rangeB.overlap(rangeC));
        assertEquals(new Range(2, 5), rangeC.overlap(rangeB));
        // share one item, one value is only that item
        assertEquals(new Range(2, 4), rangeB.overlap(rangeD));
        assertEquals(new Range(2, 4), rangeD.overlap(rangeB));
        // share one item, one range entirely within the other
        assertEquals(new Range(3, 5), rangeC.overlap(rangeD));
        assertEquals(new Range(3, 5), rangeD.overlap(rangeC));
    }

    @Test
    public void testAdjacentMerge() {
        Range rangeA = new Range(0, 2);
        Range rangeB = new Range(3, 5);

        assertEquals(new Range(0, 5), rangeA.overlap(rangeB));
        assertEquals(new Range(0, 5), rangeB.overlap(rangeA));
    }

}
