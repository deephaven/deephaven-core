package io.deephaven.util.datastructures;

import io.deephaven.util.annotations.ReferentialIntegrity;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.Assume;
import org.junit.Test;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Unit tests for {@link SegmentedSoftPool}.
 */
public class TestSegmentedSoftPool {

    @Test
    public void testWithoutFactory() {
        final SegmentedSoftPool<Integer> pool = new SegmentedSoftPool<>(10, null, null);

        try {
            pool.take();
            TestCase.fail("Expected exception");
        } catch (UnsupportedOperationException expected) {
        }

        IntStream.range(0, 100).boxed().forEach(pool::give);
        IntStream.range(0, 100).boxed().sorted(Comparator.reverseOrder())
            .forEach(II -> TestCase.assertEquals(II, pool.take()));
        IntStream.range(100, 400).boxed().forEach(pool::give);
        IntStream.range(100, 400).boxed().sorted(Comparator.reverseOrder())
            .forEach(II -> TestCase.assertEquals(II, pool.take()));
    }

    @Test
    public void testWithFactory() {
        final MutableInt counter = new MutableInt(-1);
        final MutableInt sumAllocated = new MutableInt(0);
        final MutableInt sumCleared = new MutableInt(0);

        final SegmentedSoftPool<Integer> pool = new SegmentedSoftPool<>(10,
            () -> {
                counter.increment();
                sumAllocated.add(counter);
                return counter.toInteger();
            },
            sumCleared::add);

        IntStream.range(0, 10).boxed().forEach(
            II -> {
                TestCase.assertEquals((Integer) 0, pool.take());
                pool.give(0);
            });

        IntStream.range(0, 1000).boxed().forEach(II -> TestCase.assertEquals(II, pool.take()));
        IntStream.range(0, 1000).boxed().forEach(pool::give);
        TestCase.assertEquals(sumAllocated, sumCleared);
    }

    private static final BitSet OUTSTANDING_INSTANCES = new BitSet(1_000_000);
    private static final AtomicInteger COUNT = new AtomicInteger();
    private static final ReferenceQueue<TestObject> GCED_INSTANCES = new ReferenceQueue<>();
    @ReferentialIntegrity
    private static final Set<TestRef> OUTSTANDING_REFS = new HashSet<>();

    private static final class TestRef extends WeakReference<TestObject> {

        private final int index;

        private TestRef(@NotNull final TestObject referent) {
            super(referent, GCED_INSTANCES);
            this.index = referent.index;
        }

        private void onInstanceGCed() {
            synchronized (OUTSTANDING_INSTANCES) {
                OUTSTANDING_INSTANCES.clear(index);
            }
            synchronized (OUTSTANDING_REFS) {
                OUTSTANDING_REFS.remove(this);
            }
        }
    }

    private static final class TestObject {

        private final int index;
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final byte[] payload;

        private TestObject() {
            index = COUNT.getAndIncrement();
            payload = new byte[1024 * 1024];
            synchronized (OUTSTANDING_INSTANCES) {
                OUTSTANDING_INSTANCES.set(index);
            }
        }
    }

    @Test
    public void testForGC() {
        // noinspection ConstantConditions
        Assume.assumeTrue(
            "Skipping testForGC, as it is very long under most conditions, requires specific JVM parameters, and isn't really a unit test",
            false);

        // With the following settings, cleanup should begin in 30 seconds and converge on 72 items
        // remaining: -Xmx4g -XX:SoftRefLRUPolicyMSPerMB=10

        final long nanosStart = System.nanoTime();
        final SegmentedSoftPool<TestObject> pool = new SegmentedSoftPool<>(8, null, null);
        IntStream.range(0, 1024).forEach((i) -> {
            final TestObject testObject = new TestObject();
            final TestRef testRef = new TestRef(testObject);
            synchronized (OUTSTANDING_REFS) {
                OUTSTANDING_REFS.add(testRef);
            }
            pool.give(testObject);
        });

        final TestObject[] inUse = new TestObject[65];
        long nanosLastCleanup = 0;
        while (OUTSTANDING_INSTANCES.cardinality() != 72
            || System.nanoTime() - nanosLastCleanup < 10_000_000_000L) {
            // Try to simulate some usage of the pool - we want soft refs that have been followed
            // more recently than others.
            int lengthTaken = 0;
            for (; lengthTaken < inUse.length; ++lengthTaken) {
                try {
                    inUse[lengthTaken] = pool.take();
                } catch (UnsupportedOperationException e) {
                    break;
                }
            }
            // Pause here to verify pool structure with items taken
            if (lengthTaken != inUse.length) {
                throw new IllegalStateException("Pool has been cleaned up more than expected, took "
                    + lengthTaken + " out of " + inUse.length);
            }
            for (int oi = 0; oi < lengthTaken; ++oi) {
                pool.give(inUse[oi]);
                inUse[oi] = null;
            }
            // Pause here to verify pool structure with items returned
            System.gc();
            boolean dequeuedAny = false;
            TestRef gcedInstanceRef;
            while ((gcedInstanceRef = (TestRef) GCED_INSTANCES.poll()) != null) {
                dequeuedAny = true;
                gcedInstanceRef.onInstanceGCed();
            }
            if (dequeuedAny) {
                nanosLastCleanup = System.nanoTime();
                synchronized (OUTSTANDING_INSTANCES) {
                    final long elapsedSeconds = (nanosLastCleanup - nanosStart) / 1_000_000_000L;
                    System.err.println("elapsedSeconds=" + elapsedSeconds + ", firstRemaining="
                        + OUTSTANDING_INSTANCES.nextSetBit(0) + ", remaining="
                        + OUTSTANDING_INSTANCES.cardinality() + ", allocated="
                        + OUTSTANDING_INSTANCES.length());
                }
            }
        }
    }
}
