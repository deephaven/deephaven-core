//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.intToAllButBoolean;
import static io.deephaven.replication.ReplicatePrimitiveCode.replaceAll;

/**
 * Code generation for basic ToPage implementations.
 */
public class ReplicateToPage {
    private static final String TASK = "replicateToPage";
    private static final String[] NO_EXCEPTIONS = new String[0];

    public static void main(String... args) throws IOException {
        intToAllButBoolean(TASK,
                "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/pagestore/topage/ToIntPage.java",
                "interface");

        // LocalDate -> LocalDateTime
        final String sourcePath =
                "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/pagestore/topage/ToLocalDatePage.java";
        String[][] pairs = new String[][] {
                {"LocalDate", "LocalDateTime"}
        };
        replaceAll(TASK, sourcePath, null, NO_EXCEPTIONS, pairs);

        // LocalDate -> LocalTime
        pairs = new String[][] {
                {"LocalDate", "LocalTime"}
        };
        replaceAll(TASK, sourcePath, null, NO_EXCEPTIONS, pairs);
    }
}
