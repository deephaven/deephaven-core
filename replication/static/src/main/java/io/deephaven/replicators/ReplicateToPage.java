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

    private static final String TO_PAGE_DIR =
            "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/pagestore/topage/";

    private static final String TO_INT_PAGE_PATH = TO_PAGE_DIR + "ToIntPage.java";
    private static final String TO_LOCAL_DATE_PAGE_PATH = TO_PAGE_DIR + "ToLocalDatePage.java";
    private static final String TO_BIG_INTEGER_PAGE_PATH = TO_PAGE_DIR + "ToBigIntegerPage.java";

    public static void main(String... args) throws IOException {
        intToAllButBoolean(TASK, TO_INT_PAGE_PATH, "interface");

        // LocalDate -> LocalDateTime
        String[][] pairs = new String[][] {
                {"LocalDate", "LocalDateTime"}
        };
        replaceAll(TASK, TO_LOCAL_DATE_PAGE_PATH, null, NO_EXCEPTIONS, pairs);

        // LocalDate -> LocalTime
        pairs = new String[][] {
                {"LocalDate", "LocalTime"}
        };
        replaceAll(TASK, TO_LOCAL_DATE_PAGE_PATH, null, NO_EXCEPTIONS, pairs);

        // BigInteger -> BigDecimal
        pairs = new String[][] {
                {"BigInteger", "BigDecimal"}
        };
        replaceAll(TASK, TO_BIG_INTEGER_PAGE_PATH, null, NO_EXCEPTIONS, pairs);

    }
}
