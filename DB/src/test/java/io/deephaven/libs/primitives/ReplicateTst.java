/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.libs.primitives;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateTst {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(TestCharPrimitives.class, ReplicatePrimitiveCode.TEST_SRC);
        ReplicatePrimitiveCode.shortToAllIntegralTypes(TestShortNumericPrimitives.class,
                ReplicatePrimitiveCode.TEST_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(TestFloatNumericPrimitives.class,
                ReplicatePrimitiveCode.TEST_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(TestFloatFpPrimitives.class, ReplicatePrimitiveCode.TEST_SRC);
    }
}
