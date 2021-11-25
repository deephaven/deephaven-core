package io.deephaven.replicators;

import java.io.IOException;
import java.util.Collections;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicateChunkFilters {
    public static void main(String[] args) throws IOException {
        charToShortAndByte(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfilter/CharRangeComparator.java");
        charToInteger("engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfilter/CharRangeComparator.java",
                Collections.emptyMap());

        charToShortAndByte("engine/table/src/main/java/io/deephaven/engine/table/impl/select/CharRangeFilter.java");
        charToInteger("engine/table/src/main/java/io/deephaven/engine/table/impl/select/CharRangeFilter.java",
                Collections.emptyMap());
        charToLong("engine/table/src/main/java/io/deephaven/engine/table/impl/select/CharRangeFilter.java");

        floatToAllFloatingPoints(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfilter/FloatRangeComparator.java");
        floatToAllFloatingPoints(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/select/FloatRangeFilter.java");

        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfilter/CharChunkMatchFilterFactory.java");
    }
}
