/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateTst {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(DbCharArrayTest.class, ReplicatePrimitiveCode.TEST_SRC);
    }
}
