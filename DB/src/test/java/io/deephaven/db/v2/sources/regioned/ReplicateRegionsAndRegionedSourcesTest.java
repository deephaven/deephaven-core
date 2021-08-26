/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

/**
 * Code generation for tests of {@link RegionedColumnSource} implementations as well as well as the
 * primary region interfaces for some primitive types.
 */
public class ReplicateRegionsAndRegionedSourcesTest {

    public static void main(String... args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(TestRegionedColumnSourceChar.class,
            ReplicatePrimitiveCode.TEST_SRC);
        ReplicatePrimitiveCode.charToAllButBooleanAndByte(TstColumnRegionChar.class,
            ReplicatePrimitiveCode.TEST_SRC);
    }
}
