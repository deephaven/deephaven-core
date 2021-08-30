package io.deephaven.db.v2.sources.regioned;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

/**
 * Code generation for basic {@link RegionedColumnSource} implementations as well as well as the
 * primary region interfaces for some primitive types.
 */
public class ReplicateRegionsAndRegionedSources extends ReplicatePrimitiveCode {

    public static void main(String... args) throws IOException {
        charToAllButBooleanAndByte(ColumnRegionChar.class, ReplicatePrimitiveCode.MAIN_SRC);
        charToAllButBooleanAndByte(DeferredColumnRegionChar.class, ReplicatePrimitiveCode.MAIN_SRC);
        charToAllButBooleanAndByte(ParquetColumnRegionChar.class, ReplicatePrimitiveCode.MAIN_SRC);
        charToAllButBoolean(RegionedColumnSourceChar.class, ReplicatePrimitiveCode.MAIN_SRC);
    }
}
