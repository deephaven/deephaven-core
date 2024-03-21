//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToShortAndByte;
import static io.deephaven.replication.ReplicatePrimitiveCode.intToLongAndFloatingPoints;

/**
 * Code generation for basic ToPage implementations.
 */
public class ReplicateToPage {

    public static void main(String... args) throws IOException {
        intToLongAndFloatingPoints("replicateToPage",
                "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/pagestore/topage/ToIntPage.java",
                "interface");
        charToShortAndByte("replicateToPage",
                "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/pagestore/topage/ToCharPageFromInt.java");
    }
}
