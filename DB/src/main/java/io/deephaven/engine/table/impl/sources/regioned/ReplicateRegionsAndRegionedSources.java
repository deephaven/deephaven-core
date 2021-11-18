package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

/**
 * Code generation for basic {@link RegionedColumnSource} implementations as well as well as the primary region
 * interfaces for some primitive types.
 */
public class ReplicateRegionsAndRegionedSources extends ReplicatePrimitiveCode {

    public static void main(String... args) throws IOException {
        charToAllButBooleanAndByte("DB/src/main/java/io/deephaven/engine/table/impl/sources/regioned/ColumnRegionChar.java");
        charToAllButBooleanAndByte(
                "DB/src/main/java/io/deephaven/engine/table/impl/sources/regioned/DeferredColumnRegionChar.java");
        charToAllButBooleanAndByte(
                "DB/src/main/java/io/deephaven/engine/table/impl/sources/regioned/ParquetColumnRegionChar.java");
        charToAllButBoolean("DB/src/main/java/io/deephaven/engine/table/impl/sources/regioned/RegionedColumnSourceChar.java");
    }
}
