//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

/**
 * Code generation for tests of {@link RegionedColumnSource} implementations as well as well as the primary region
 * interfaces for some primitive types.
 */
public class ReplicateRegionAndRegionedSourceTests {

    public static void main(String... args) throws IOException {
        fixupFloatTests(charToAllButBoolean("replicateRegionAndRegionedSourceTests",
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/TestRegionedColumnSourceChar.java"));
        fixupFloatTests(charToAllButBooleanAndByte("replicateRegionAndRegionedSourceTests",
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/TstColumnRegionChar.java"));
        charToAllButBooleanAndFloats("replicateRegionAndRegionedSourceTests",
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/kernel/CharRegionBinarySearchKernelTest.java");
        charToAllButBooleanAndFloats("replicateRegionAndRegionedSourceTests",
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/kernel/CharColumnBinarySearchKernelTest.java");
        floatToAllFloatingPoints("replicateRegionAndRegionedSourceTests",
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/kernel/FloatColumnBinarySearchKernelTest.java");
        floatToAllFloatingPoints("replicateRegionAndRegionedSourceTests",
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/kernel/FloatRegionBinarySearchKernelTest.java");
    }

    /**
     * assertEquals has no exact two-argument form for float or double, so the char sources mark the affected calls with
     * an EXTRA comment and the floating point variants get a delta here. The marker is an ordinary comment in every
     * other variant.
     */
    private static void fixupFloatTests(List<String> paths) throws IOException {
        for (final String path : paths) {
            if (!path.contains("Float") && !path.contains("Double")) {
                continue;
            }
            final File file = new File(path);
            FileUtils.writeLines(file, ReplicationUtils.globalReplacements(
                    FileUtils.readLines(file, Charset.defaultCharset()), "/\\*\\s*EXTRA\\s*\\*/", ", .000001f"));
        }
    }
}
