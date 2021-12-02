package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateParquetChunkedWriters {

    public static void main(String[] args) throws IOException {

        ReplicatePrimitiveCode.intToLongAndFloatingPoints(
                "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/PlainIntChunkedWriter.java",
                "int pageSize", "IntBuffer.allocate(4)", "int originalLimit", "int writeBulk", "int valueCount",
                "int rowCount",
                "int nullCount", "writeInt\\(", "IntBuffer repeatCount", "length != Integer.MIN_VALUE", "int length",
                "int i = 0;", "int targetCapacity", "IntBuffer nullOffsets");
    }

}
