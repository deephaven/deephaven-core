/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBoolean;
import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBooleanAndByte;

/**
 * Code generation for tests of {@link RegionedColumnSource} implementations as well as well as the primary region
 * interfaces for some primitive types.
 */
public class ReplicateRegionAndRegionedSourceTests {

    public static void main(String... args) throws IOException {
        charToAllButBoolean(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/TestRegionedColumnSourceChar.java");
        charToAllButBooleanAndByte(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/regioned/TstColumnRegionChar.java");
    }
}
