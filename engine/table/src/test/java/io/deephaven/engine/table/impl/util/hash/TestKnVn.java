//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import io.deephaven.test.types.SerialTest;
import io.deephaven.engine.table.impl.util.hash.NullableLongLongMaps.Shape;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Fills each shape to its capacity ceiling. Serial, because of its appetite: the final array is a long[] near
 * Integer.MAX_VALUE and the last rehash holds the previous one beside it, so the three cases need a 40 GB heap
 * (measured: 51 GB resident at the peak with a 48 GB heap) and run for about five and a half minutes in all on an
 * i9-13900K, each case roughly two minutes for its 900 million puts and the rehashes on the way. Where the heap is
 * smaller the cases skip themselves.
 */
@Category(SerialTest.class)
public class TestKnVn {
    /**
     * Rationale: at its maximum capacity, the hashtable will have an long[Integer.MAX_VALUE] array. When it rehashes,
     * it will need another such array to rehash into. So, number of bytes needed = 2 * sizeof(long) * Integer.MAX_VALUE
     * = 32G. Let's round up and say you need a 40G heap to run this test.
     */
    private static final long MINIMUM_HEAP_SIZE_NEEDED_FOR_TEST = 40L << 30;
    private static final int HASHTABLE_SIZE_LOWER_BOUND_1 = 900_000_000;
    private static final int HASHTABLE_SIZE_LOWER_BOUND_2 = 800_000_000;
    private static final int HASHTABLE_SIZE_LOWER_BOUND_4 = 900_000_000;
    private static final int HASHTABLE_SIZE_UPPER_BOUND = 1_000_000_000;

    @Test
    public void fillK1V1ToTheMax() {
        fillToCapacity(withDefaults(Shape.K1V1), HASHTABLE_SIZE_LOWER_BOUND_1);
    }

    @Test
    public void fillK2V2ToTheMax() {
        fillToCapacity(withDefaults(Shape.K2V2), HASHTABLE_SIZE_LOWER_BOUND_2);
    }

    @Test
    public void fillK4V4ToTheMax() {
        fillToCapacity(withDefaults(Shape.K4V4), HASHTABLE_SIZE_LOWER_BOUND_4);
    }

    private static NullableLongLongMap withDefaults(final Shape shape) {
        return NullableLongLongMaps.of(shape, HashMapBase.DEFAULT_INITIAL_CAPACITY, HashMapBase.DEFAULT_LOAD_FACTOR,
                HashMapBase.DEFAULT_NO_ENTRY_VALUE);
    }

    private static void fillToCapacity(NullableLongLongMap ht, final long lowerSizeBound) {
        final long maxMemory = Runtime.getRuntime().maxMemory();
        if (maxMemory < MINIMUM_HEAP_SIZE_NEEDED_FOR_TEST) {
            final String skipMessage = String.format("Skipping test, because I want %fG of heap, but have only %fG%n",
                    (double) MINIMUM_HEAP_SIZE_NEEDED_FOR_TEST / (1 << 30), (double) maxMemory / (1 << 30));
            Assume.assumeTrue(skipMessage, false);
        }
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(ht);
        long ii = 0;
        try {
            for (; ii < lowerSizeBound; ++ii) {
                if ((ii % 10_000_000) == 0) {
                    System.out.printf("made it to %d%n", ii);
                }
                scalarAccess.put(ii * 11, ii * 17);
            }
        } catch (OutOfMemoryError ooe) {
            throw new RuntimeException(String.format("OOM after %d elements", ii), ooe);
        }

        // Expect the hashtable to reject a put soon
        boolean putFailed = false;
        for (; ii < HASHTABLE_SIZE_UPPER_BOUND; ++ii) {
            try {
                if ((ii % 10_000_000) == 0) {
                    System.out.printf("Made it to %d, and expecting it to hit max capacity soon%n", ii);
                }
                scalarAccess.put(ii * 11, ii * 17);
            } catch (UnsupportedOperationException uoe) {
                putFailed = true;
                break;
            }
        }
        assertTrue(String.format(
                "Expected hashtable to reject a 'put' as it got close to being full, but it accepted %d elements", ii),
                putFailed);

        // resetToNullRetainingCapacity must remember the maximum-capacity sizing, so that the next allocation comes
        // back at that capacity with its nearly-full rehash threshold and a refill of the entries this generation
        // absorbed would not trigger another maximum-sized rehash.
        final long entriesAbsorbed = ii;
        final HashMapBase base = (HashMapBase) ht;
        final int capacityAtMax = ht.capacity();
        ht.resetToNullRetainingCapacity();
        assertEquals(0, ht.capacity());
        // resetToNullRetainingCapacity() is not a cursor operation: reset the invalidated binding.
        scalarAccess.reset(ht);
        scalarAccess.put(0, 0);
        assertEquals(capacityAtMax, ht.capacity());
        assertTrue(
                String.format("rehashThreshold (%d) > entriesAbsorbed (%d)", base.rehashThreshold, entriesAbsorbed),
                base.rehashThreshold > entriesAbsorbed);
    }
}
