//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToShortAndByte;
import static io.deephaven.replication.ReplicatePrimitiveCode.floatToAllFloatingPoints;
import static io.deephaven.replication.ReplicatePrimitiveCode.replaceAll;

/**
 * Code generation for basic ToPage implementations.
 */
public class ReplicatePageMaterializers {
    private static final String TASK = "replicatePageMaterializers";
    private static final String[] NO_EXCEPTIONS = new String[0];

    public static void main(String... args) throws IOException {
        charToShortAndByte(TASK,
                "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/materializers/CharMaterializer.java",
                NO_EXCEPTIONS);

        // Float -> Double
        floatToAllFloatingPoints(TASK,
                "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/materializers/FloatMaterializer.java",
                NO_EXCEPTIONS);

        // Float -> Int
        String[][] pairs = new String[][] {
                {"readFloat", "readInteger"},
                {"Float", "Int"},
                {"float", "int"}
        };
        replaceAll(TASK,
                "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/materializers/FloatMaterializer.java",
                "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/materializers/IntMaterializer.java",
                null, NO_EXCEPTIONS, pairs);

        // Float -> String
        pairs = new String[][] {
                {"readFloat()", "readBytes().toStringUsingUTF8"},
                {"Float", "String"},
                {"float", "String"},
                {"dataReader, 0, numValues", "dataReader, null, numValues"}
        };
        replaceAll(TASK,
                "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/materializers/FloatMaterializer.java",
                "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/materializers/StringMaterializer.java",
                null, NO_EXCEPTIONS, pairs);

        // LocalTimeFromMicros -> LocalTimeFromNanos
        pairs = new String[][] {
                {"Micros", "Nanos"},
                {"micros", "nanos"},
        };
        replaceAll(TASK,
                "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/materializers/LocalTimeFromMicrosMaterializer.java",
                "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/materializers/LocalTimeFromNanosMaterializer.java",
                null, NO_EXCEPTIONS, pairs);

        // LocalTimeFromMicros -> LocalTimeFromMillis
        pairs = new String[][] {
                {"Micros", "Millis"},
                {"micros", "millis"},
                {"readLong", "readInteger"},
        };
        replaceAll(TASK,
                "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/materializers/LocalTimeFromMicrosMaterializer.java",
                "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/materializers/LocalTimeFromMillisMaterializer.java",
                null, NO_EXCEPTIONS, pairs);

        // TimestampNanosFromMicros -> TimestampNanosFromMillis
        pairs = new String[][] {
                {"Micros", "Millis"},
                {"micros", "millis"}
        };
        replaceAll(TASK,
                "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/materializers/TimestampNanosFromMicrosMaterializer.java",
                "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/materializers/TimestampNanosFromMillisMaterializer.java",
                null, NO_EXCEPTIONS, pairs);
    }
}

