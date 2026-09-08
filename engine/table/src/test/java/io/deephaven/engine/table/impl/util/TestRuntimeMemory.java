//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.*;

public class TestRuntimeMemory {
    /**
     * Ignored because we can have extra stuff going on and can't get good results in a CI run.
     */
    @Ignore
    @Test
    public void testPooling() throws InterruptedException {
        final RuntimeMemory.PooledSample pooledSample1 = RuntimeMemory.getInstance().readPooledSample();
        final RuntimeMemory.PooledSample pooledSample2 = RuntimeMemory.getInstance().readPooledSample();
        assertNotSame(pooledSample1, pooledSample2);
        RuntimeMemory.getInstance().returnPooledSample(pooledSample1);
        final RuntimeMemory.PooledSample pooledSample3 = RuntimeMemory.getInstance().readPooledSample();
        assertSame(pooledSample1, pooledSample3);
        final long usedMemory3 = pooledSample3.usedHeapMemory();
        RuntimeMemory.getInstance().returnPooledSample(pooledSample3);

        // just something that should be noticeable
        final long[] bigArray = new long[16 * 1024 * 1024];
        int cacheInterval = RuntimeMemory.getInstance().getCacheIntervalMillis();
        Thread.sleep(cacheInterval * 2L);

        final RuntimeMemory.PooledSample pooledSample4 = RuntimeMemory.getInstance().readPooledSample();
        assertSame(pooledSample3, pooledSample4);
        final long usedMemory4 = pooledSample3.usedHeapMemory();

        // we are checking for 8MB just to make sure that the 16MB is accounted for
        assertTrue(usedMemory4 > usedMemory3 + 8 * 1024 * 1024);

        bigArray[0] = 1;

        RuntimeMemory.getInstance().returnPooledSample(pooledSample4);
        RuntimeMemory.getInstance().returnPooledSample(pooledSample1);

        final List<long[]> bigArrays = new ArrayList<>();

        // make sure we have a fresh sample before we allocate the array
        Thread.sleep(cacheInterval * 2L);
        RuntimeMemory.PooledSample freshSample = RuntimeMemory.getInstance().readPooledSample();
        RuntimeMemory.getInstance().returnPooledSample(freshSample);
        long sampleTime = freshSample.nextCheck;
        long expectedMemory = freshSample.usedHeapMemory();
        bigArrays.add(new long[1024 * 1024]);


        int tries = 50;
        while (--tries > 0) {
            final RuntimeMemory.PooledSample checkNoRefetch = RuntimeMemory.getInstance().readPooledSample();
            assertSame(pooledSample1, checkNoRefetch);
            long usedMemory = checkNoRefetch.usedHeapMemory();

            if (System.currentTimeMillis() >= (sampleTime)) {
                RuntimeMemory.getInstance().returnPooledSample(checkNoRefetch);

                // noinspection BusyWait
                Thread.sleep(cacheInterval * 2L);
                final RuntimeMemory.PooledSample tempSample = RuntimeMemory.getInstance().readPooledSample();
                sampleTime = tempSample.nextCheck;
                expectedMemory = tempSample.usedHeapMemory();
                bigArrays.add(new long[1024 * 1024]);
                RuntimeMemory.getInstance().returnPooledSample(tempSample);
                continue;
            }

            assertEquals(expectedMemory, usedMemory);

            RuntimeMemory.getInstance().returnPooledSample(checkNoRefetch);
            break;
        }

        if (tries == 0) {
            System.err.println("Failed to see cached memory usage after 50 tries");
            assertTrue(tries > 0);
        }

        // prevent unused-ness
        System.arraycopy(bigArray, 0, bigArrays.get(0), 0, 1);
    }
}
