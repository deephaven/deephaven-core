/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util;

import io.deephaven.configuration.DataDir;
import com.sun.management.HotSpotDiagnosticMXBean;

import javax.management.MBeanServer;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A simple method for generating a Heap dump for this JVM.
 */
public class HeapDump {
    @SuppressWarnings("WeakerAccess")
    public static void heapDump() throws IOException {
        heapDump(generateHeapDumpPath());
    }

    public static String generateHeapDumpPath() {
        final String name =
                new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System.currentTimeMillis())) + ".hprof";
        final Path path = DataDir.get().resolve("heapDumps").resolve(name);
        return path.toString();
    }

    @SuppressWarnings("WeakerAccess")
    public static void heapDump(String filename) throws IOException {
        Files.createDirectories(Path.of(filename).getParent());
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final HotSpotDiagnosticMXBean mxBean = ManagementFactory.newPlatformMXBeanProxy(server,
                "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
        mxBean.dumpHeap(filename, true);
    }
}
