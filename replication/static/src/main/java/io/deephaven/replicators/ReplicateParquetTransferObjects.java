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

    public static void main(String[] args) throws IOException {
        charToShortAndByte(PARQUET_CHAR_TRANSFER_PATH);
        charToShortAndByte(PARQUET_CHAR_ARRAY_TRANSFER_PATH);
        charToShortAndByte(PARQUET_CHAR_VECTOR_TRANSFER_PATH);

        intToLongAndFloatingPoints(PARQUET_INT_TRANSFER_PATH, "int targetPageSize", "int maxValuesPerPage",
                "Math.toIntExact");
        intToLongAndFloatingPoints(PARQUET_INT_ARRAY_TRANSFER_PATH, "int targetPageSize", "int length", "int getSize");
        intToLongAndFloatingPoints(PARQUET_INT_VECTOR_TRANSFER_PATH, "int targetPageSize", "int length");
    }
}
