package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicateParquetTransferObjects {
    private static final String PARQUET_INT_TRANSFER_PATH =
            "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/transfer/IntTransfer.java";

    public static void main(String[] args) throws IOException {
        intToChar(PARQUET_INT_TRANSFER_PATH, null,
                "IntBuffer", "IntStatistics", "int targetSize", "int chunkIdx", "int transfer", "int ret");
        intToByte(PARQUET_INT_TRANSFER_PATH, null,
                "IntBuffer", "IntStatistics", "int targetSize", "int chunkIdx", "int transfer", "int ret");
        intToShort(PARQUET_INT_TRANSFER_PATH, null,
                "IntBuffer", "IntStatistics", "int targetSize", "int chunkIdx", "int transfer", "int ret");
        intToLong(PARQUET_INT_TRANSFER_PATH, null,
                "int targetSize", "int chunkIdx", "int transfer", "int ret");
        intToDouble(PARQUET_INT_TRANSFER_PATH, null,
                "int targetSize", "int chunkIdx", "int transfer", "int ret");
        intToFloat(PARQUET_INT_TRANSFER_PATH, null,
                "int targetSize", "int chunkIdx", "int transfer", "int ret");
    }
}
