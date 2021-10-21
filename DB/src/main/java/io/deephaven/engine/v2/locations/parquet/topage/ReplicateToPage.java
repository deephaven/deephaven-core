package io.deephaven.engine.v2.locations.parquet.topage;

import java.io.IOException;

import static io.deephaven.compilertools.ReplicatePrimitiveCode.charToShortAndByte;
import static io.deephaven.compilertools.ReplicatePrimitiveCode.intToLongAndFloatingPoints;

/**
 * Code generation for basic {@link ToPage} implementations.
 */
public class ReplicateToPage {

    public static void main(String... args) throws IOException {
        intToLongAndFloatingPoints("DB/src/main/java/io/deephaven/engine/v2/locations/parquet/topage/ToIntPage.java",
                "interface");
        charToShortAndByte("DB/src/main/java/io/deephaven/engine/v2/locations/parquet/topage/ToCharPageFromInt.java");
    }
}
