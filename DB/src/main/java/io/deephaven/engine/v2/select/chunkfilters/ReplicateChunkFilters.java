package io.deephaven.engine.v2.select.chunkfilters;

import java.io.IOException;
import java.util.Collections;

import static io.deephaven.compilertools.ReplicatePrimitiveCode.*;

public class ReplicateChunkFilters {
    public static void main(String[] args) throws IOException {
        charToShortAndByte("DB/src/main/java/io/deephaven/engine/v2/select/chunkfilters/CharRangeComparator.java");
        charToInteger("DB/src/main/java/io/deephaven/engine/v2/select/chunkfilters/CharRangeComparator.java",
                Collections.emptyMap());

        charToShortAndByte("DB/src/main/java/io/deephaven/engine/v2/select/CharRangeFilter.java");
        charToInteger("DB/src/main/java/io/deephaven/engine/v2/select/CharRangeFilter.java",
                Collections.emptyMap());
        charToLong("DB/src/main/java/io/deephaven/engine/v2/select/CharRangeFilter.java");

        floatToAllFloatingPoints(
                "DB/src/main/java/io/deephaven/engine/v2/select/chunkfilters/FloatRangeComparator.java");
        floatToAllFloatingPoints("DB/src/main/java/io/deephaven/engine/v2/select/FloatRangeFilter.java");

        charToAllButBoolean(
                "DB/src/main/java/io/deephaven/engine/v2/select/chunkfilters/CharChunkMatchFilterFactory.java");
    }
}
