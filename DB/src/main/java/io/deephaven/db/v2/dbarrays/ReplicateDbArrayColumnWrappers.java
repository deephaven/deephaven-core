/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.dbarrays;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateDbArrayColumnWrappers {

    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(DbCharArrayColumnWrapper.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(DbPrevCharArrayColumnWrapper.class, ReplicatePrimitiveCode.MAIN_SRC);
    }
}
