//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicateParquetPushdownHandlers {
    private static final String TASK = "replicateParquetPushdownHandlers";

    private static final String PUSHDOWN_HANDLER_PATH =
            "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/location/";
    private static final String CHAR_PUSHDOWN_HANDLER = PUSHDOWN_HANDLER_PATH + "CharPushdownHandler.java";
    private static final String FLOAT_PUSHDOWN_HANDLER = PUSHDOWN_HANDLER_PATH + "FloatPushdownHandler.java";

    public static void main(String[] args) throws IOException {
        // char -> short, byte, int, long
        charToShortAndByte(TASK, CHAR_PUSHDOWN_HANDLER);
        charToIntegers(TASK, CHAR_PUSHDOWN_HANDLER);
        charToLong(TASK, CHAR_PUSHDOWN_HANDLER);

        // float -> double
        floatToAllFloatingPoints(TASK, FLOAT_PUSHDOWN_HANDLER);
    }
}
