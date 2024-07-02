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

    private static final String MATERIALIZER_DIR =
            "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/materializers/";

    private static final String CHAR_MATERIALIZER_PATH = MATERIALIZER_DIR + "CharMaterializer.java";
    private static final String FLOAT_MATERIALIZER_PATH = MATERIALIZER_DIR + "FloatMaterializer.java";
    private static final String LOCAL_TIME_FROM_MICROS_MATERIALIZER_PATH =
            MATERIALIZER_DIR + "LocalTimeFromMicrosMaterializer.java";
    private static final String LOCAL_DATE_TIME_FROM_MILLIS_MATERIALIZER_PATH =
            MATERIALIZER_DIR + "LocalDateTimeFromMillisMaterializer.java";
    private static final String INSTANT_NANOS_FROM_MICROS_MATERIALIZER_PATH =
            MATERIALIZER_DIR + "InstantNanosFromMicrosMaterializer.java";
    private static final String BIG_DECIMAL_FROM_LONG_MATERIALIZER_PATH =
            MATERIALIZER_DIR + "BigDecimalFromLongMaterializer.java";
    private static final String BIG_DECIMAL_FROM_BYTES_MATERIALIZER_PATH =
            MATERIALIZER_DIR + "BigDecimalFromBytesMaterializer.java";
    private static final String BIG_INTEGER_MATERIALIZER_PATH = MATERIALIZER_DIR + "BigIntegerMaterializer.java";

    public static void main(String... args) throws IOException {
        charToShortAndByte(TASK, CHAR_MATERIALIZER_PATH, NO_EXCEPTIONS);

        // Float -> Double
        floatToAllFloatingPoints(TASK, FLOAT_MATERIALIZER_PATH, NO_EXCEPTIONS);

        // Float -> Int
        String[][] pairs = new String[][] {
                {"readFloat", "readInteger"},
                {"Float", "Int"},
                {"float", "int"}
        };
        replaceAll(TASK, FLOAT_MATERIALIZER_PATH, null, NO_EXCEPTIONS, pairs);

        // LocalTimeFromMicros -> LocalTimeFromMillis
        // We change from Micros to Millis and not the other way since converting from Long to Integer has fewer
        // exceptions than the other way around.
        pairs = new String[][] {
                {"Micros", "Millis"},
                {"micros", "millis"},
                {"readLong", "readInteger"},
        };
        replaceAll(TASK, LOCAL_TIME_FROM_MICROS_MATERIALIZER_PATH, null, NO_EXCEPTIONS, pairs);

        // LocalTimeFromMicros -> LocalTimeFromNanos
        pairs = new String[][] {
                {"Micros", "Nanos"},
                {"micros", "nanos"},
        };
        replaceAll(TASK, LOCAL_TIME_FROM_MICROS_MATERIALIZER_PATH, null, NO_EXCEPTIONS, pairs);

        // LocalDateTimeFromMillis -> LocalDateTimeFromMicros
        pairs = new String[][] {
                {"Millis", "Micros"}
        };
        replaceAll(TASK, LOCAL_DATE_TIME_FROM_MILLIS_MATERIALIZER_PATH, null, NO_EXCEPTIONS, pairs);

        // LocalDateTimeFromMillis -> LocalDateTimeFromNanos
        pairs = new String[][] {
                {"Millis", "Nanos"}
        };
        replaceAll(TASK, LOCAL_DATE_TIME_FROM_MILLIS_MATERIALIZER_PATH, null, NO_EXCEPTIONS, pairs);

        // InstantNanosFromMicros -> InstantNanosFromMillis
        pairs = new String[][] {
                {"Micros", "Millis"},
                {"micros", "millis"}
        };
        replaceAll(TASK, INSTANT_NANOS_FROM_MICROS_MATERIALIZER_PATH, null, NO_EXCEPTIONS, pairs);

        // BigDecimalFromLong -> BigDecimalFromInt
        pairs = new String[][] {
                {"readLong", "readInteger"},
                {"Long", "Int"}
        };
        replaceAll(TASK, BIG_DECIMAL_FROM_LONG_MATERIALIZER_PATH, null, NO_EXCEPTIONS, pairs);

        // BigDecimal -> BigInteger
        pairs = new String[][] {
                {"BigDecimalFromBytes", "BigInteger"},
                {"BigDecimal", "BigInteger"}
        };
        replaceAll(TASK,
                BIG_DECIMAL_FROM_BYTES_MATERIALIZER_PATH,
                BIG_INTEGER_MATERIALIZER_PATH,
                null, NO_EXCEPTIONS, pairs);
    }
}

