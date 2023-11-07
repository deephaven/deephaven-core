package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicateParquetTransferObjects {
    private static final String PARQUET_TRANSFER_DIR =
            "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/transfer/";
    private static final String PARQUET_CHAR_TRANSFER_PATH = PARQUET_TRANSFER_DIR + "CharTransfer.java";
    private static final String PARQUET_CHAR_ARRAY_TRANSFER_PATH = PARQUET_TRANSFER_DIR + "CharArrayTransfer.java";
    private static final String PARQUET_CHAR_VECTOR_TRANSFER_PATH = PARQUET_TRANSFER_DIR + "CharVectorTransfer.java";

    private static final String PARQUET_INT_TRANSFER_PATH = PARQUET_TRANSFER_DIR + "IntTransfer.java";
    private static final String PARQUET_INT_ARRAY_TRANSFER_PATH = PARQUET_TRANSFER_DIR + "IntArrayTransfer.java";
    private static final String PARQUET_INT_VECTOR_TRANSFER_PATH = PARQUET_TRANSFER_DIR + "IntVectorTransfer.java";

    private static final String PARQUET_INSTANT_ARRAY_TRANSFER_PATH =
            PARQUET_TRANSFER_DIR + "InstantArrayTransfer.java";
    private static final String PARQUET_INSTANT_VECTOR_TRANSFER_PATH =
            PARQUET_TRANSFER_DIR + "InstantVectorTransfer.java";

    private static final String PARQUET_LOCAL_DATE_TIME_TRANSFER_PATH =
            PARQUET_TRANSFER_DIR + "LocalDateTimeTransfer.java";
    private static final String PARQUET_LOCAL_DATE_TIME_ARRAY_TRANSFER_PATH =
            PARQUET_TRANSFER_DIR + "LocalDateTimeArrayTransfer.java";
    private static final String PARQUET_LOCAL_DATE_TIME_VECTOR_TRANSFER_PATH =
            PARQUET_TRANSFER_DIR + "LocalDateTimeVectorTransfer.java";

    private static final String PARQUET_DATE_ARRAY_TRANSFER_PATH = PARQUET_TRANSFER_DIR + "DateArrayTransfer.java";
    private static final String PARQUET_DATE_VECTOR_TRANSFER_PATH = PARQUET_TRANSFER_DIR + "DateVectorTransfer.java";

    private static final String PARQUET_TIME_TRANSFER_PATH = PARQUET_TRANSFER_DIR + "TimeTransfer.java";
    private static final String PARQUET_TIME_ARRAY_TRANSFER_PATH = PARQUET_TRANSFER_DIR + "TimeArrayTransfer.java";
    private static final String PARQUET_TIME_VECTOR_TRANSFER_PATH = PARQUET_TRANSFER_DIR + "TimeVectorTransfer.java";

    private static final String[] NO_EXCEPTIONS = new String[0];

    public static void main(String[] args) throws IOException {
        charToShortAndByte(PARQUET_CHAR_TRANSFER_PATH);
        charToShortAndByte(PARQUET_CHAR_ARRAY_TRANSFER_PATH);
        charToShortAndByte(PARQUET_CHAR_VECTOR_TRANSFER_PATH);

        intToLongAndFloatingPoints(PARQUET_INT_TRANSFER_PATH, "int targetPageSizeInBytes", "int targetElementsPerPage",
                "Math.toIntExact");
        intToLongAndFloatingPoints(PARQUET_INT_ARRAY_TRANSFER_PATH, "int targetPageSizeInBytes", "int length",
                "int getSize");
        intToLongAndFloatingPoints(PARQUET_INT_VECTOR_TRANSFER_PATH, "int targetPageSizeInBytes", "int length");

        String[][] pairs = new String[][] {
                {"InstantArrayTransfer", "DateArrayTransfer"},
                {"InstantVectorTransfer", "DateVectorTransfer"},
                {"DateTimeUtils.epochNanos", "DateTimeUtils.epochDaysAsInt"},
                {"Instant", "LocalDate"},
                {"LongBuffer", "IntBuffer"},
                {"Long", "Integer"},
                {"long", "int"},
        };
        replaceAll(PARQUET_INSTANT_ARRAY_TRANSFER_PATH, PARQUET_DATE_ARRAY_TRANSFER_PATH, null, NO_EXCEPTIONS, pairs);
        replaceAll(PARQUET_INSTANT_VECTOR_TRANSFER_PATH, PARQUET_DATE_VECTOR_TRANSFER_PATH, null, NO_EXCEPTIONS, pairs);

        pairs = new String[][] {
                {"InstantArrayTransfer", "TimeArrayTransfer"},
                {"InstantVectorTransfer", "TimeVectorTransfer"},
                {"DateTimeUtils.epochNanos", "DateTimeUtils.nanosOfDay"},
                {"Instant", "LocalTime"}
        };
        replaceAll(PARQUET_INSTANT_ARRAY_TRANSFER_PATH, PARQUET_TIME_ARRAY_TRANSFER_PATH, null, NO_EXCEPTIONS, pairs);
        replaceAll(PARQUET_INSTANT_VECTOR_TRANSFER_PATH, PARQUET_TIME_VECTOR_TRANSFER_PATH, null, NO_EXCEPTIONS, pairs);

        pairs = new String[][] {
                {"InstantArrayTransfer", "LocalDateTimeArrayTransfer"},
                {"InstantVectorTransfer", "LocalDateTimeVectorTransfer"},
                {"DateTimeUtils.epochNanos", "DateTimeUtils.epochNanosUTC"},
                {"Instant", "LocalDateTime"}
        };
        replaceAll(PARQUET_INSTANT_ARRAY_TRANSFER_PATH, PARQUET_LOCAL_DATE_TIME_ARRAY_TRANSFER_PATH, null,
                NO_EXCEPTIONS, pairs);
        replaceAll(PARQUET_INSTANT_VECTOR_TRANSFER_PATH, PARQUET_LOCAL_DATE_TIME_VECTOR_TRANSFER_PATH, null,
                NO_EXCEPTIONS, pairs);

        pairs = new String[][] {
                {"TimeTransfer", "LocalDateTimeTransfer"},
                {"LocalTime", "LocalDateTime"},
                {"DateTimeUtils.nanosOfDay", "DateTimeUtils.epochNanosUTC"}
        };
        replaceAll(PARQUET_TIME_TRANSFER_PATH, PARQUET_LOCAL_DATE_TIME_TRANSFER_PATH, null, NO_EXCEPTIONS, pairs);

        // Additional differences can be generated by Spotless
    }
}
