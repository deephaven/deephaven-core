package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicateParquetTransferObjects {
    private static final String PARQUET_TRANSFER_DIR =
            "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/transfer/";
    private static final String PARQUET_INT_TRANSFER_PATH = PARQUET_TRANSFER_DIR + "IntTransfer.java";
    private static final String PARQUET_CHAR_TRANSFER_PATH = PARQUET_TRANSFER_DIR + "CharTransfer.java";

    public static void main(String[] args) throws IOException {
        charToShortAndByte(PARQUET_CHAR_TRANSFER_PATH);
        intToLongAndFloatingPoints(PARQUET_INT_TRANSFER_PATH, "int targetSize");
    }
}
