//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SoftRecyclerTest {
    private static final long SECOND = 1_000_000_000L;

    /** A recycler of arrays whose clock is advanced by hand and read on every operation. */
    private static final class Fixture {
        final AtomicInteger constructed = new AtomicInteger();
        long now = 0;
        final SoftRecycler<long[]> recycler;

        Fixture(final int capacity, final int maximumCapacity) {
            this(capacity, maximumCapacity, 0.5);
        }

        Fixture(final int capacity, final int maximumCapacity, final double shrinkFraction) {
            recycler = new SoftRecycler<>(capacity, maximumCapacity, () -> {
                constructed.incrementAndGet();
                return new long[1];
            }, null, () -> now, 1, SECOND, shrinkFraction);
        }

        /** Grow the capacity to {@code count}, from a bin that starts empty. */
        void growTo(final int count) {
            burst(count);
            endWindow();
            burst(count);
            assertEquals(count, recycler.getCapacity());
        }

        /** Borrow {@code count} items and return them all, as an update cycle does with its previous values. */
        List<long[]> burst(final int count) {
            final List<long[]> items = new ArrayList<>();
            for (int ii = 0; ii < count; ++ii) {
                items.add(recycler.borrowItem());
            }
            for (final long[] item : items) {
                recycler.returnItem(item);
            }
            return items;
        }

        /** Advance past the end of the window, and let the next operation see it. */
        void endWindow() {
            now += SECOND;
        }
    }

    @Test
    public void testReusesReturnedItems() {
        final Fixture fixture = new Fixture(4, 4);
        final long[] item = fixture.recycler.borrowItem();
        fixture.recycler.returnItem(item);
        assertSame(item, fixture.recycler.borrowItem());
        assertEquals(1, fixture.constructed.get());
    }

    @Test
    public void testGrowsToTheLargestBurst() {
        final Fixture fixture = new Fixture(10, 1000);
        // fill the bin, so that the first burst below is not short of every item it needs
        fixture.burst(10);
        fixture.endWindow();
        // every burst needs 25 items but the bin keeps only 10: each burst constructs 15 and turns 15 away
        for (int ii = 0; ii < 5; ++ii) {
            fixture.burst(25);
        }
        assertEquals(10, fixture.recycler.getCapacity());
        fixture.endWindow();
        fixture.burst(25);
        // the largest burst, not the window's total of 15 constructed per burst
        assertEquals(25, fixture.recycler.getCapacity());

        // with room for a whole burst, nothing is constructed again
        fixture.burst(25);
        final int constructed = fixture.constructed.get();
        for (int ii = 0; ii < 5; ++ii) {
            fixture.burst(25);
        }
        assertEquals(constructed, fixture.constructed.get());
    }

    @Test
    public void testGrowsToTheLargestBurstFromAnEmptyBin() {
        // from an empty bin, the first burst constructs every item it needs, which is more than the bin was short of
        final Fixture fixture = new Fixture(10, 1000);
        fixture.burst(25);
        fixture.endWindow();
        fixture.burst(25);
        assertEquals(25, fixture.recycler.getCapacity());
    }

    @Test
    public void testItemsABurstKeepsNeedNoRoom() {
        // each burst borrows 5 items it keeps, as storage that stays in use, and 20 it returns
        final Fixture fixture = new Fixture(10, 1000);
        for (int window = 0; window < 3; ++window) {
            for (int ii = 0; ii < 5; ++ii) {
                for (int jj = 0; jj < 5; ++jj) {
                    fixture.recycler.borrowItem();
                }
                fixture.burst(20);
            }
            fixture.endWindow();
        }
        fixture.recycler.borrowItem();
        assertEquals(20, fixture.recycler.getCapacity());
    }

    @Test
    public void testGrowthStopsAtTheMaximum() {
        final Fixture fixture = new Fixture(10, 16);
        fixture.burst(40);
        fixture.endWindow();
        fixture.burst(40);
        assertEquals(16, fixture.recycler.getCapacity());
        fixture.endWindow();
        fixture.burst(40);
        assertEquals(16, fixture.recycler.getCapacity());
    }

    @Test
    public void testConstructionAloneDoesNotGrow() {
        // items borrowed and kept, as for storage that stays in use, are not a shortfall of the bin
        final Fixture fixture = new Fixture(10, 1000);
        for (int ii = 0; ii < 50; ++ii) {
            fixture.recycler.borrowItem();
        }
        fixture.endWindow();
        fixture.recycler.borrowItem();
        assertEquals(10, fixture.recycler.getCapacity());
    }

    @Test
    public void testShrinksTowardTheInitialCapacity() {
        final Fixture fixture = new Fixture(10, 1000);
        fixture.burst(90);
        fixture.endWindow();
        fixture.burst(90);
        assertEquals(90, fixture.recycler.getCapacity());

        // now the bursts need only 10, so 80 items sit unused through each window
        fixture.burst(90);
        fixture.endWindow();
        fixture.burst(10);
        int previous = fixture.recycler.getCapacity();
        for (int window = 0; window < 20; ++window) {
            fixture.endWindow();
            fixture.burst(10);
            final int capacity = fixture.recycler.getCapacity();
            if (previous > 10) {
                assertTrue("capacity " + capacity + " after " + previous, capacity < previous);
            }
            previous = capacity;
        }
        assertEquals(10, fixture.recycler.getCapacity());
    }

    @Test
    public void testShrinkFractionZeroKeepsTheCapacity() {
        final Fixture fixture = new Fixture(10, 1000, 0);
        fixture.growTo(90);
        for (int window = 0; window < 5; ++window) {
            fixture.endWindow();
            fixture.burst(10);
        }
        assertEquals(90, fixture.recycler.getCapacity());
    }

    @Test
    public void testShrinkFractionOneGivesUpEveryUnusedItem() {
        final Fixture fixture = new Fixture(10, 1000, 1);
        fixture.growTo(90);
        // a window that needs 20 of the 90 items leaves 70 unused, which are given up at once
        fixture.burst(90);
        fixture.endWindow();
        fixture.burst(20);
        fixture.endWindow();
        fixture.burst(20);
        assertEquals(20, fixture.recycler.getCapacity());
    }

    @Test
    public void testAdjustsOnlyAtTheEndOfAWindow() {
        final Fixture fixture = new Fixture(10, 1000);
        fixture.burst(10);
        fixture.endWindow();
        for (int ii = 0; ii < 5; ++ii) {
            fixture.burst(25);
            // most of a window is not enough
            fixture.now += SECOND / 10;
        }
        fixture.burst(25);
        assertEquals(10, fixture.recycler.getCapacity());
        fixture.now += SECOND / 2;
        fixture.burst(25);
        assertEquals(25, fixture.recycler.getCapacity());
    }

    @Test
    public void testDefaultMaximumIsUnlimited() {
        final SoftRecycler<long[]> recycler = new SoftRecycler<>(10, () -> new long[1], null);
        assertEquals(10, recycler.getCapacity());
        assertEquals(Integer.MAX_VALUE, recycler.getMaximumCapacity());
    }

    @Test
    public void testRejectsInvalidParameters() {
        assertThrows(IllegalArgumentException.class, () -> new SoftRecycler<>(10, 9, () -> new long[1], null));
        assertThrows(IllegalArgumentException.class, () -> new SoftRecycler<>(-1, 9, () -> new long[1], null));
        assertThrows(IllegalArgumentException.class,
                () -> new SoftRecycler<>(1, 1, () -> new long[1], null, () -> 0, 1, 0, 0.5));
        assertThrows(IllegalArgumentException.class,
                () -> new SoftRecycler<>(1, 1, () -> new long[1], null, () -> 0, 1, SECOND, 1.5));
        assertThrows(IllegalArgumentException.class,
                () -> new SoftRecycler<>(1, 1, () -> new long[1], null, () -> 0, 1, SECOND, Double.NaN));
    }
}
