/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicatePrimitiveCode.floatToAllFloatingPoints;

public class ReplicatePrimitiveLibTests {
    public static void main(String[] args) throws IOException {
        charToAllButBoolean("engine/function/src/test/java/io/deephaven/function/TestCharPrimitives.java");
        shortToAllIntegralTypes(
                "engine/function/src/test/java/io/deephaven/function/TestShortNumericPrimitives.java");
        floatToAllFloatingPoints(
                "engine/function/src/test/java/io/deephaven/function/TestFloatNumericPrimitives.java");
        floatToAllFloatingPoints(
                "engine/function/src/test/java/io/deephaven/function/TestFloatFpPrimitives.java");
    }
}
