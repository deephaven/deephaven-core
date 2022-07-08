/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static io.deephaven.replication.ReplicationUtils.addImport;
import static java.nio.file.Files.readAllLines;

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
                "// Duplicate for Replication\nimport java.nio.IntBuffer;",
                "int newSize",
                "final int currentCapacity",
                "final int currentPosition",
                "final int targetPageSize",
                "int requiredCapacity",
                "int newCapacity",
                "int MAXIMUM_TOTAL_CAPACITY = Integer.MAX_VALUE")
                .forEach(ReplicateParquetChunkedWriters::injectIntBuffer);
    }

    private static void injectIntBuffer(String s) {
        try {
            List<String> lines = readAllLines(Path.of(s));
            lines = addImport(lines, "import java.nio.IntBuffer;");
            FileUtils.writeLines(new File(s), lines);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
