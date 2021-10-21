/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.sources.regioned;

import java.io.IOException;

import static io.deephaven.compilertools.ReplicatePrimitiveCode.charToAllButBoolean;
import static io.deephaven.compilertools.ReplicatePrimitiveCode.charToAllButBooleanAndByte;

/**
 * Code generation for tests of {@link RegionedColumnSource} implementations as well as well as the primary region
 * interfaces for some primitive types.
 */
public class ReplicateRegionsAndRegionedSourcesTest {

    public static void main(String... args) throws IOException {
        charToAllButBoolean(
                "DB/src/test/java/io/deephaven/engine/v2/sources/regioned/TestRegionedColumnSourceChar.java");
        charToAllButBooleanAndByte("DB/src/test/java/io/deephaven/engine/v2/sources/regioned/TstColumnRegionChar.java");
    }
}
