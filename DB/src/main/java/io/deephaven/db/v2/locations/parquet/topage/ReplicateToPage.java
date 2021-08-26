package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

/**
 * Code generation for basic {@link ToPage} implementations.
 */
public class ReplicateToPage {

    public static void main(String... args) throws IOException {
        ReplicatePrimitiveCode.intToLongAndFloatingPoints(ToIntPage.class, ReplicatePrimitiveCode.MAIN_SRC,
                "interface");
        ReplicatePrimitiveCode.charToShortAndByte(ToCharPageFromInt.class, ReplicatePrimitiveCode.MAIN_SRC);
    }
}
