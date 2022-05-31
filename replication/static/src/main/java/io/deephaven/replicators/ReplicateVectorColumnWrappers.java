/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBoolean;

public class ReplicateVectorColumnWrappers {

    public static void main(String[] args) throws IOException {
        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/vector/CharVectorColumnWrapper.java");
        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/vector/PrevCharVectorColumnWrapper.java");
        charToAllButBoolean(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/vector/CharVectorColumnWrapperTest.java");
    }
}
