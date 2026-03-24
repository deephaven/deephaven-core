//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

/**
 * Code generation for tests of {@link RegionedColumnSource} implementations as well as well as the primary region
 * interfaces for some primitive types.
 */
public class ReplicateRegionAndRegionedSourceTests {

    public static void main(String... args) throws IOException {
        charToAllButBoolean("replicateRegionAndRegionedSourceTests",
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/TestRegionedColumnSourceChar.java");
        charToAllButBooleanAndByte("replicateRegionAndRegionedSourceTests",
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/TstColumnRegionChar.java");
        charToAllButBooleanAndFloats("replicateRegionAndRegionedSourceTests",
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/kernel/CharRegionBinarySearchKernelTest.java");
        floatToAllFloatingPoints("replicateRegionAndRegionedSourceTests",
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/kernel/FloatRegionBinarySearchKernelTest.java");
    }
}
