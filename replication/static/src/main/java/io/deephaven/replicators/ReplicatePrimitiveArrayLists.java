//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBoolean;
import static io.deephaven.replication.ReplicatePrimitiveCode.floatToAllFloatingPoints;

public class ReplicatePrimitiveArrayLists {
    private static final String TASK = "replicatePrimitiveArrayLists";

    public static void main(String... args) throws IOException {
        charToAllButBoolean(TASK,
                "Util/src/main/java/io/deephaven/util/datastructures/list/CharArrayList.java");
        charToAllButBoolean(TASK,
                "Util/src/test/java/io/deephaven/util/datastructures/list/TestCharArrayList.java");
        floatToAllFloatingPoints(TASK,
                "Util/src/test/java/io/deephaven/util/datastructures/list/TestFloatArrayListSpecial.java");
    }
}
