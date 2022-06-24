/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

public class ReplicateParquetChunkedWriters {

    public static void main(String[] args) throws IOException {

        ReplicatePrimitiveCode.intToLongAndFloatingPoints(
                "extensions/parquet/base/src/main/java/io/deephaven/parquet/base/PlainIntChunkedWriter.java",
                "int pageSize",
                "IntBuffer.allocate\\(4\\)",
                "int originalLimit",
                "int writeBulk",
                "int valueCount",
                "int rowCount",
                "int nullCount",
                "writeInt\\(",
                "IntBuffer repeatCount",
                "length != QueryConstants\\.NULL_INT",
                "int length",
                "int i = 0;",
                "int targetCapacity",
                "IntBuffer nullOffsets",
                "// Duplicate for Replication\nimport java.nio.IntBuffer;");
    }

}
