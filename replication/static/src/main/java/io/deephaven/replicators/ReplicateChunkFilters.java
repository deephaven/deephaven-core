//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;
import java.util.Collections;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicateChunkFilters {
    private static final String TASK = "replicateChunkFilters";

    public static void main(String[] args) throws IOException {
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfilter/CharChunkFilter.java");

        charToShortAndByte(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfilter/CharRangeComparator.java");
        charToInteger(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfilter/CharRangeComparator.java",
                Collections.emptyMap());
        charToLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfilter/CharRangeComparator.java");

        charToShortAndByte(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/select/CharRangeFilter.java");
        charToInteger(TASK, "engine/table/src/main/java/io/deephaven/engine/table/impl/select/CharRangeFilter.java",
                Collections.emptyMap());
        charToLong(TASK, "engine/table/src/main/java/io/deephaven/engine/table/impl/select/CharRangeFilter.java");

        floatToAllFloatingPoints(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfilter/FloatRangeComparator.java");
        floatToAllFloatingPoints(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/select/FloatRangeFilter.java");

        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfilter/CharChunkMatchFilterFactory.java");
    }
}
