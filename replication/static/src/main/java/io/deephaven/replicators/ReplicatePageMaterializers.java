//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToByte;
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
    private static final String SHORT_MATERIALIZER_PATH = MATERIALIZER_DIR + "ShortMaterializer.java";
    private static final String INT_MATERIALIZER_PATH = MATERIALIZER_DIR + "IntMaterializer.java";
    private static final String LONG_MATERIALIZER_BASE_PATH = MATERIALIZER_DIR + "LongMaterializerBase.java";
    private static final String LONG_MATERIALIZER_PATH = MATERIALIZER_DIR + "LongMaterializer.java";
    private static final String LONG_FROM_INT_MATERIALIZER_PATH = MATERIALIZER_DIR + "LongFromIntMaterializer.java";
    private static final String LONG_FROM_UNSIGNED_SHORT_MATERIALIZER_PATH =
            MATERIALIZER_DIR + "LongFromUnsignedShortMaterializer.java";
    private static final String DOUBLE_MATERIALIZER_PATH = MATERIALIZER_DIR + "DoubleMaterializer.java";
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
        charToByte(TASK, CHAR_MATERIALIZER_PATH, NO_EXCEPTIONS);

        // LongBase -> IntBase
        String[][] pairs = new String[][] {
                {"Long", "Int"},
                {"long", "int"},
        };
        replaceAll(TASK, LONG_MATERIALIZER_BASE_PATH, null, NO_EXCEPTIONS, pairs);

        // Long -> Int
        pairs = new String[][] {
                {"readLong", "readInteger"},
                {"Long", "Int"},
                {"long", "int"}
        };
        replaceAll(TASK, LONG_MATERIALIZER_PATH, null, NO_EXCEPTIONS, pairs);

        // Int -> IntFromBoolean
        pairs = new String[][] {
                {"IntMaterializer", "IntFromBooleanMaterializer"},
                {"readInteger\\(\\)", "readBoolean() ? 1 : 0"}
        };
        replaceAll(TASK, INT_MATERIALIZER_PATH, null, new String[] {"IntMaterializerBase"}, pairs);

        // LongBase -> ShortBase
        pairs = new String[][] {
                {"Long", "Short"},
                {"long", "short"},
        };
        replaceAll(TASK, LONG_MATERIALIZER_BASE_PATH, null, NO_EXCEPTIONS, pairs);

        // Long -> Short
        pairs = new String[][] {
                {"dataReader.readLong", "(short) dataReader.readInteger"},
                {"dataReader, 0, numValues", "dataReader, (short) 0, numValues"},
                {"Long", "Short"},
                {"long", "short"}
        };
        replaceAll(TASK, LONG_MATERIALIZER_PATH, null, NO_EXCEPTIONS, pairs);

        // Long -> LongFromInt
        pairs = new String[][] {
                {"LongMaterializer", "LongFromIntMaterializer"},
                {"readLong", "readInteger"},
        };
        replaceAll(TASK, LONG_MATERIALIZER_PATH, LONG_FROM_INT_MATERIALIZER_PATH, null,
                new String[] {"LongMaterializerBase"}, pairs);

        // Long -> LongFromBoolean
        pairs = new String[][] {
                {"LongMaterializer", "LongFromBooleanMaterializer"},
                {"readLong\\(\\)", "readBoolean() ? 1 : 0"},
        };
        replaceAll(TASK, LONG_MATERIALIZER_PATH, null, new String[] {"LongMaterializerBase"}, pairs);

        // LongFromUnsignedShort -> LongFromUnsignedByte
        pairs = new String[][] {
                {"Short", "Byte"},
                {"short", "byte"}
        };
        replaceAll(TASK, LONG_FROM_UNSIGNED_SHORT_MATERIALIZER_PATH, null, NO_EXCEPTIONS, pairs);

        // LongFromUnsignedShort -> LongFromUnsignedInt
        pairs = new String[][] {
                {"Short.toUnsignedLong", "Integer.toUnsignedLong"},
                {"Short", "Int"},
                {"short", "int"}
        };
        replaceAll(TASK, LONG_FROM_UNSIGNED_SHORT_MATERIALIZER_PATH, null, NO_EXCEPTIONS, pairs);

        // LongFromUnsignedShort -> IntFromUnsignedShort
        pairs = new String[][] {
                {"LongFromUnsignedShort", "IntFromUnsignedShort"},
                {"Long", "Int"},
                {"long", "int"}
        };
        replaceAll(TASK, LONG_FROM_UNSIGNED_SHORT_MATERIALIZER_PATH, null, NO_EXCEPTIONS, pairs);

        // LongFromUnsignedShort -> IntFromUnsignedByte
        pairs = new String[][] {
                {"Short", "Byte"},
                {"short", "byte"},
                {"Long", "Int"},
                {"long", "int"}
        };
        replaceAll(TASK, LONG_FROM_UNSIGNED_SHORT_MATERIALIZER_PATH, null, NO_EXCEPTIONS, pairs);

        // LongBase -> DoubleBase
        pairs = new String[][] {
                {"Long", "Double"},
                {"long", "double"},
        };
        replaceAll(TASK, LONG_MATERIALIZER_BASE_PATH, null, NO_EXCEPTIONS, pairs);

        // Long -> Double
        pairs = new String[][] {
                {"dataReader.readLong", "dataReader.readDouble"},
                {"Long", "Double"},
                {"long", "double"}
        };
        replaceAll(TASK, LONG_MATERIALIZER_PATH, null, NO_EXCEPTIONS, pairs);

        // Double -> DoubleFromFloat
        pairs = new String[][] {
                {"DoubleMaterializer", "DoubleFromFloatMaterializer"},
                {"Double", "Float"}
        };
        replaceAll(TASK, DOUBLE_MATERIALIZER_PATH, null, new String[] {"DoubleMaterializerBase"}, pairs);

        // Short -> ShortFromBoolean
        pairs = new String[][] {
                {"ShortMaterializer", "ShortFromBooleanMaterializer"},
                {"dataReader.readInteger\\(\\)", "(dataReader.readBoolean() ? 1 : 0)"}
        };
        replaceAll(TASK, SHORT_MATERIALIZER_PATH, null, new String[] {"ShortMaterializerBase"}, pairs);

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

