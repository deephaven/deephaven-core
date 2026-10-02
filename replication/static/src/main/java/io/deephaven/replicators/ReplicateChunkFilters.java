//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicateChunkFilters {
    private static final String TASK = "replicateChunkFilters";

    private static final String CHUNK_FILTER_PATH =
            "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfilter/";
    private static final String CHAR_CHUNK_FILTER = CHUNK_FILTER_PATH + "CharChunkFilter.java";
    private static final String FLOAT_CHUNK_FILTER = CHUNK_FILTER_PATH + "FloatChunkFilter.java";
    private static final String CHAR_RANGE_COMPARATOR = CHUNK_FILTER_PATH + "CharRangeComparator.java";
    private static final String CHAR_CHUNK_MATCH_FILTER_FACTORY =
            CHUNK_FILTER_PATH + "CharChunkMatchFilterFactory.java";
    private static final String FLOAT_CHUNK_MATCH_FILTER_FACTORY =
            CHUNK_FILTER_PATH + "FloatChunkMatchFilterFactory.java";

    private static final String FILTER_LOOPS = "filterLoops";

    private static final String RANGE_FILTER_PATH =
            "engine/table/src/main/java/io/deephaven/engine/table/impl/select/";
    private static final String CHAR_RANGE_FILTER = RANGE_FILTER_PATH + "CharRangeFilter.java";
    private static final String FLOAT_RANGE_FILTER = RANGE_FILTER_PATH + "FloatRangeFilter.java";

    public static void main(String[] args) throws IOException {
        // *ChunkFilter.java
        charToAllButBoolean(TASK, CHAR_CHUNK_FILTER);

        // Give the cheap leaf filters their own copy of the loops, before the leaf templates are replicated
        copyFilterLoops(CHAR_CHUNK_FILTER, CHAR_RANGE_COMPARATOR);
        copyFilterLoops(CHAR_CHUNK_FILTER, CHAR_CHUNK_MATCH_FILTER_FACTORY);
        copyFilterLoops(FLOAT_CHUNK_FILTER, FLOAT_CHUNK_MATCH_FILTER_FACTORY);

        // *RangeComparator.java
        charToAllButBoolean(TASK, CHAR_RANGE_COMPARATOR);

        // *ChunkMatchFilterFactory.java
        charToAllButBooleanAndFloats(TASK, CHAR_CHUNK_MATCH_FILTER_FACTORY);
        floatToAllFloatingPoints(TASK, FLOAT_CHUNK_MATCH_FILTER_FACTORY);

        final File objectFile = new File(CHUNK_FILTER_PATH + "DoubleChunkMatchFilterFactory.java");
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = ReplicationUtils.replaceRegion(lines, "getBits", List.of("" +
                "    public static long getBits(double value) {\n" +
                "        return Double.doubleToLongBits(value == 0.0d ? 0.0d : value);\n" +
                "    }\n"));
        lines = ReplicationUtils.globalReplacements(lines,
                "int valueBits", "long valueBits",
                "doubleToIntBits", "doubleToLongBits",
                "IntOpenHashSet", "LongOpenHashSet",
                "IntSet", "LongSet",
                "it\\.unimi\\.dsi\\.fastutil\\.ints", "it.unimi.dsi.fastutil.longs",
                "0\\.0f", "0.0d");
        FileUtils.writeLines(objectFile, lines);

        // *RangeFilter.java
        charToShortAndByte(TASK, CHAR_RANGE_FILTER);
        charToIntegers(TASK, CHAR_RANGE_FILTER);
        charToLong(TASK, CHAR_RANGE_FILTER);
        floatToAllFloatingPoints(TASK, FLOAT_RANGE_FILTER);
    }

    /**
     * Replaces every {@code filterLoops} region in {@code leafPath}, each inside a nested leaf filter class, with the
     * {@code filterLoops} region of the {@code *ChunkFilter} base class at {@code chunkFilterPath}. A leaf with its own
     * copy of the loops calls {@code matches} on only its own class, so the JIT can inline the call no matter how many
     * other filters have run.
     */
    private static void copyFilterLoops(final String chunkFilterPath, final String leafPath) throws IOException {
        final List<String> loops = new ArrayList<>();
        ReplicationUtils.replaceRegion(
                FileUtils.readLines(new File(chunkFilterPath), Charset.defaultCharset()), FILTER_LOOPS, region -> {
                    loops.addAll(region);
                    return region;
                });
        if (loops.isEmpty()) {
            throw new IllegalStateException("No " + FILTER_LOOPS + " region in " + chunkFilterPath);
        }
        // the leaves are nested classes, one level deeper than the base class
        final List<String> indented = new ArrayList<>();
        for (final String line : loops) {
            indented.add(line.isEmpty() ? line : "    " + line);
        }

        final File leafFile = new File(leafPath);
        final List<String> lines = FileUtils.readLines(leafFile, Charset.defaultCharset());
        FileUtils.writeLines(leafFile, ReplicationUtils.replaceRegion(lines, FILTER_LOOPS, indented));
    }
}
