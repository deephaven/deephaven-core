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
    private static final String TASK = "replicateRegionAndRegionedSourceTests";


    public static void main(String... args) throws IOException {
        charToAllButBoolean(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/TestRegionedColumnSourceChar.java");
        charToAllButBooleanAndByte(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/TstColumnRegionChar.java");
        charToAllButBooleanAndFloats(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/kernel/CharRegionBinarySearchKernelTest.java");
        charToAllButBooleanAndFloats(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/kernel/CharColumnBinarySearchKernelTest.java");
        floatToAllFloatingPoints(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/kernel/FloatColumnBinarySearchKernelTest.java");
        floatToAllFloatingPoints(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/kernel/FloatRegionBinarySearchKernelTest.java");
    }
}
