package io.deephaven.db.v2.hashing;

import gnu.trove.map.TLongLongMap;
import junit.framework.TestCase;
import org.junit.Assume;
import org.junit.Test;

public class TestKnVn {
    /**
     * Rationale: at its maximum capacity, the hashtable will have an long[Integer.MAX_VALUE] array.
     * When it rehashes, it will need another such array to rehash into. So, number of bytes needed
     * = 2 * sizeof(long) * Integer.MAX_VALUE = 32G. Let's round up and say you need a 40G heap to
     * run this test.
     */
    private static final long MINIMUM_HEAP_SIZE_NEEDED_FOR_TEST = 40L << 30;
    private static final int HASHTABLE_SIZE_LOWER_BOUND_1 = 900_000_000;
    private static final int HASHTABLE_SIZE_LOWER_BOUND_2 = 800_000_000;
    private static final int HASHTABLE_SIZE_LOWER_BOUND_4 = 900_000_000;
    private static final int HASHTABLE_SIZE_UPPER_BOUND = 1_000_000_000;

    /**
     * This is a very long-running test which also needs a big heap. We should figure out how to
     * configure things so this runs off to the side without disrupting other developers.
     */
    @Test
    public void fillK1V1ToTheMax() {
        fillToCapacity(new HashMapLockFreeK1V1(), HASHTABLE_SIZE_LOWER_BOUND_1);
    }

    /**
     * This is a very long-running test which also needs a big heap. We should figure out how to
     * configure things so this runs off to the side without disrupting other developers.
     */
    @Test
    public void fillK2V2ToTheMax() {
        fillToCapacity(new HashMapLockFreeK2V2(), HASHTABLE_SIZE_LOWER_BOUND_2);
    }

    /**
     * This is a very long-running test which also needs a big heap. We should figure out how to
     * configure things so this runs off to the side without disrupting other developers.
     */
    @Test
    public void fillK4V4ToTheMax() {
        fillToCapacity(new HashMapLockFreeK4V4(), HASHTABLE_SIZE_LOWER_BOUND_4);
    }

    private static void fillToCapacity(TLongLongMap ht, final long lowerSizeBound) {
        final long maxMemory = Runtime.getRuntime().maxMemory();
        if (maxMemory < MINIMUM_HEAP_SIZE_NEEDED_FOR_TEST) {
            final String skipMessage =
                String.format("Skipping test, because I want %fG of heap, but have only %fG%n",
                    (double) MINIMUM_HEAP_SIZE_NEEDED_FOR_TEST / (1 << 30),
                    (double) maxMemory / (1 << 30));
            Assume.assumeTrue(skipMessage, false);
        }
        long ii = 0;
        try {
            for (; ii < lowerSizeBound; ++ii) {
                if ((ii % 10_000_000) == 0) {
                    System.out.printf("made it to %d%n", ii);
                }
                ht.put(ii * 11, ii * 17);
            }
        } catch (OutOfMemoryError ooe) {
            throw new RuntimeException(String.format("OOM after %d elements", ii), ooe);
        }

        // Expect the hashtable to reject a put soon
        boolean putFailed = false;
        for (; ii < HASHTABLE_SIZE_UPPER_BOUND; ++ii) {
            try {
                if ((ii % 10_000_000) == 0) {
                    System.out.printf("Made it to %d, and expecting it to hit max capacity soon%n",
                        ii);
                }
                ht.put(ii * 11, ii * 17);
            } catch (UnsupportedOperationException uoe) {
                putFailed = true;
                break;
            }
        }
        TestCase.assertTrue(String.format(
            "Expected hashtable to reject a 'put' as it got close to being full, but it accepted %d elements",
            ii),
            putFailed);
    }
}
