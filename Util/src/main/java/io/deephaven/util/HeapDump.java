/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util;

import com.sun.management.HotSpotDiagnosticMXBean;

import javax.management.MBeanServer;
import java.io.IOException;
import java.lang.management.ManagementFactory;

/**
 * A simple method for generating a Heap dump for this JVM.
 */
public class HeapDump {

    public static void heapDump(String filename) throws IOException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final HotSpotDiagnosticMXBean mxBean = ManagementFactory.newPlatformMXBeanProxy(server,
                "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
        mxBean.dumpHeap(filename, true);
    }
}
