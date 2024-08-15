//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;
import java.util.Map;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToByte;
import static io.deephaven.replication.ReplicatePrimitiveCode.charToFloat;
import static io.deephaven.replication.ReplicatePrimitiveCode.replaceAll;

/**
 * Code generation for basic ToPage implementations.
 */
public class ReplicateToPage {
    private static final String TASK = "replicateToPage";
    private static final String[] NO_EXCEPTIONS = new String[0];

    private static final String TO_PAGE_DIR =
            "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/pagestore/topage/";

    private static final String TO_CHAR_PAGE_PATH = TO_PAGE_DIR + "ToCharPage.java";
    private static final String TO_LOCAL_DATE_TIME_PAGE_PATH = TO_PAGE_DIR + "ToLocalDateTimePage.java";
    private static final String TO_BIG_INTEGER_PAGE_PATH = TO_PAGE_DIR + "ToBigIntegerPage.java";

    public static void main(String... args) throws IOException {
        charToFloat(TASK, TO_CHAR_PAGE_PATH, null, "interface");

        // LocalDateTime -> LocalTime
        String[][] pairs = new String[][] {
                {"LocalDateTime", "LocalTime"}
        };
        replaceAll(TASK, TO_LOCAL_DATE_TIME_PAGE_PATH, null, NO_EXCEPTIONS, pairs);

        // BigInteger -> BigDecimal
        pairs = new String[][] {
                {"BigIntegerMaterializer", "BigDecimalFromBytesMaterializer"},
                {"BigInteger", "BigDecimal"}
        };
        replaceAll(TASK, TO_BIG_INTEGER_PAGE_PATH, null, NO_EXCEPTIONS, pairs);
    }
}
