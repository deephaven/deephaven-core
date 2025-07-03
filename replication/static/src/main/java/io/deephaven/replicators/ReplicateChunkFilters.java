//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicateChunkFilters {
    private static final String TASK = "replicateChunkFilters";

    private static final String CHUNK_FILTER_PATH =
            "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfilter/";
    private static final String CHAR_CHUNK_FILTER = CHUNK_FILTER_PATH + "CharChunkFilter.java";
    private static final String CHAR_RANGE_COMPARATOR = CHUNK_FILTER_PATH + "CharRangeComparator.java";
    private static final String CHAR_CHUNK_MATCH_FILTER_FACTORY =
            CHUNK_FILTER_PATH + "CharChunkMatchFilterFactory.java";

    private static final String RANGE_FILTER_PATH =
            "engine/table/src/main/java/io/deephaven/engine/table/impl/select/";
    private static final String CHAR_RANGE_FILTER = RANGE_FILTER_PATH + "CharRangeFilter.java";
    private static final String FLOAT_RANGE_FILTER = RANGE_FILTER_PATH + "FloatRangeFilter.java";

    public static void main(String[] args) throws IOException {
        // *ChunkFilter.java
        charToAllButBoolean(TASK, CHAR_CHUNK_FILTER);

        // *RangeComparator.java
        charToAllButBoolean(TASK, CHAR_RANGE_COMPARATOR);

        // *ChunkMatchFilterFactory.java
        charToAllButBoolean(TASK, CHAR_CHUNK_MATCH_FILTER_FACTORY);

        // *RangeFilter.java
        charToShortAndByte(TASK, CHAR_RANGE_FILTER);
        charToIntegers(TASK, CHAR_RANGE_FILTER);
        charToLong(TASK, CHAR_RANGE_FILTER);
        floatToAllFloatingPoints(TASK, FLOAT_RANGE_FILTER);
    }
}
