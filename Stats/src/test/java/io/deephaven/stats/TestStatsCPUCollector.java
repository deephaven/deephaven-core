/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.stats;

import io.deephaven.base.stats.*;
import junit.framework.TestCase;

import java.io.File;

public class TestStatsCPUCollector extends TestCase {
    public void testProcFD() {
        synchronized (Stats.class) {
            Stats.clearAll();
            StatsCPUCollector SUT = new StatsCPUCollector(1000, true);
            SUT.update();

            Group procGroup = Stats.makeGroup("Proc", "per-process stats");
            Item numFdsItem = procGroup.getItem("NumFDs");
            Item maxFdItem = procGroup.getItem("MaxFD");

            // if this box has /proc, then we should see a non-zero stat
            File procFd = new File("/proc/self/fd");
            if (procFd.isDirectory()) {
                assertNotNull(numFdsItem);
                assertNotNull(maxFdItem);
                long numFds = numFdsItem.getValue().getLast();
                long numFdsUpdates = numFdsItem.getValue().getN();
                long maxFd = maxFdItem.getValue().getLast();
                long maxFdUpdates = maxFdItem.getValue().getN();
                assertEquals(1, numFdsUpdates);
                assertEquals(1, maxFdUpdates);
                System.out.println("TestStatsCPUCollector.testProcFd: " + numFds + " fds, maxFd = " + maxFd);
            } else {
                // if not, we see nothing - but we also don't crash
                assertNull(numFdsItem);
                assertNull(maxFdItem);
            }
        }
    }
}
