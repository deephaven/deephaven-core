package io.deephaven.replicators;

import java.io.IOException;
import java.util.Collections;

import static io.deephaven.compilertools.ReplicatePrimitiveCode.*;

public class ReplicateChunkFilters {
    public static void main(String[] args) throws IOException {
        charToShortAndByte("engine/base/src/main/java/io/deephaven/engine/table/impl/chunkfilter/CharRangeComparator.java");
        charToInteger("engine/base/src/main/java/io/deephaven/engine/table/impl/chunkfilter/CharRangeComparator.java",
                Collections.emptyMap());

        charToShortAndByte("DB/src/main/java/io/deephaven/engine/v2/select/CharRangeFilter.java");
        charToInteger("DB/src/main/java/io/deephaven/engine/v2/select/CharRangeFilter.java",
                Collections.emptyMap());
        charToLong("DB/src/main/java/io/deephaven/engine/v2/select/CharRangeFilter.java");

        floatToAllFloatingPoints(
                "engine/base/src/main/java/io/deephaven/engine/table/impl/chunkfilter/FloatRangeComparator.java");
        floatToAllFloatingPoints("DB/src/main/java/io/deephaven/engine/v2/select/FloatRangeFilter.java");

        charToAllButBoolean(
                "engine/base/src/main/java/io/deephaven/engine/table/impl/chunkfilter/CharChunkMatchFilterFactory.java");
    }
}
