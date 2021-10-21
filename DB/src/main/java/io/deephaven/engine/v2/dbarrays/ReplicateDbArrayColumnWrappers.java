/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.dbarrays;

import java.io.IOException;

import static io.deephaven.compilertools.ReplicatePrimitiveCode.charToAllButBoolean;

public class ReplicateDbArrayColumnWrappers {

    public static void main(String[] args) throws IOException {
        charToAllButBoolean("DB/src/main/java/io/deephaven/engine/v2/dbarrays/DbCharArrayColumnWrapper.java");
        charToAllButBoolean("DB/src/main/java/io/deephaven/engine/v2/dbarrays/DbPrevCharArrayColumnWrapper.java");
    }
}
