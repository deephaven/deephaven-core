package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.compilertools.ReplicatePrimitiveCode.charToShortAndByte;
import static io.deephaven.compilertools.ReplicatePrimitiveCode.intToLongAndFloatingPoints;

/**
 * Code generation for basic ToPage implementations.
 */
public class ReplicateToPage {

    public static void main(String... args) throws IOException {
        intToLongAndFloatingPoints("engine/table/src/main/java/io/deephaven/engine/table/impl/locations/parquet/topage/ToIntPage.java",
                "interface");
        charToShortAndByte("engine/table/src/main/java/io/deephaven/engine/table/impl/locations/parquet/topage/ToCharPageFromInt.java");
    }
}
