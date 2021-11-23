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
        charToAllButBoolean("engine/table/src/test/java/io/deephaven/libs/primitives/TestCharPrimitives.java");
        shortToAllIntegralTypes("engine/table/src/main/test/io/deephaven/libs/primitives/TestShortNumericPrimitives.java");
        floatToAllFloatingPoints("engine/table/src/main/test/io/deephaven/libs/primitives/TestFloatNumericPrimitives.java");
        floatToAllFloatingPoints("engine/table/src/main/test/io/deephaven/libs/primitives/TestFloatFpPrimitives.java");
    }
}
