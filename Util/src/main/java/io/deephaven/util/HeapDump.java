//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.configuration.DataDir;
import com.sun.management.HotSpotDiagnosticMXBean;
import io.deephaven.io.logger.Logger;

import javax.management.MBeanServer;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Predicate;

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

    private static void heapDumpWrapper(final String cause, final RuntimeException failure,
            final Predicate<RuntimeException> ignore, final Logger log) {
        if (ignore != null && ignore.test(failure)) {
            return;
        }
        try {
            final String heapDumpPath = HeapDump.generateHeapDumpPath();
            log.fatal().append(cause + ", generating heap dump to")
                    .append(heapDumpPath).append(": ").append(failure).endl();
            heapDump(heapDumpPath);
        } catch (Exception e) {
            log.info()
                    .append("Exception while trying to dump heap on assertion failure: " + e.getMessage() + ":\n")
                    .append(e)
                    .endl();
        }
    }

    public static void setupHeapDumpWithDefaults(final Configuration configuration,
            final Predicate<RuntimeException> ignore, final Logger log) {
        if (configuration.getBooleanWithDefault("assertion.heapDump", false)) {
            log.info().append("Heap dump on assertion failures enabled.").endl();
            Assert.setOnAssertionCallback(af -> heapDumpWrapper("Assertion failure", af, ignore, log));
        }
        if (configuration.getBooleanWithDefault("require.heapDump", false)) {
            log.info().append("Heap dump on requirement failures enabled.").endl();
            Require.setOnFailureCallback(rf -> heapDumpWrapper("Requirement failure", rf, ignore, log));
        }
    }
}
