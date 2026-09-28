//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import io.deephaven.replication.ReplicationUtils;
import io.deephaven.util.type.ArrayTypeUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.shortToAllIntegralTypes;
import static io.deephaven.replication.ReplicationUtils.globalReplacements;

public class ReplicateColumnStats {
    public static void main(String[] args) throws IOException {
        final List<String> paths = shortToAllIntegralTypes("replicateColumnStats",
                "server/src/main/java/io/deephaven/server/table/stats/ShortChunkedNumericalStats.java");
        final String intPath =
                paths.stream().filter(p -> p.contains("Integer")).findFirst().orElseThrow(FileNotFoundException::new);
        fixupIntegerChunkName(intPath);

        ReplicatePrimitiveCode.floatToAllFloatingPoints("replicateColumnStats",
                "server/src/main/java/io/deephaven/server/table/stats/FloatChunkedNumericalStats.java");

        final String objectPath = ReplicatePrimitiveCode.charToObject("replicateColumnStats",
                "server/src/main/java/io/deephaven/server/table/stats/CharacterChunkedStats.java");
        fixupObjectChunk(objectPath);
    }

    private static void fixupIntegerChunkName(final String intPath) throws IOException {
        final File objectFile = new File(intPath);
        final List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        FileUtils.writeLines(objectFile, globalReplacements(lines, "chunk.IntegerChunk", "chunk.IntChunk",
                "final IntegerChunk", "final IntChunk", "asIntegerChunk", "asIntChunk"));
    }

    private static void fixupObjectChunk(final String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = ReplicationUtils.removeImport(lines,
                "import io.deephaven.util.QueryConstants;");
        lines = ReplicationUtils.addImport(lines,
                ArrayTypeUtils.class);
        lines = globalReplacements(lines,
                "QueryConstants.NULL_OBJECT", "null",
                "\\? extends Attributes.Values", "?, ? extends Attributes.Values",
                "ObjectChunk<[?] ", "ObjectChunk<?, ? ",
                // charToObject capitalizes the leading C in the package "chars" to "Objects"; fix back to "objects".
                "it\\.unimi\\.dsi\\.fastutil\\.Objects\\.", "it.unimi.dsi.fastutil.objects.",
                "Object2LongOpenHashMap countValues", "Object2LongOpenHashMap<Object> countValues",
                "new Object2LongOpenHashMap\\(\\)", "new Object2LongOpenHashMap<>()",
                "ObjectSet uniqueValues", "ObjectSet<Object> uniqueValues",
                "new ObjectOpenHashSet\\(\\)", "new ObjectOpenHashSet<>()",
                "new ChunkedObjectColumnIterator", "new ChunkedObjectColumnIterator<>",
                "\\(ObjectColumnIterator", "(ObjectColumnIterator<Object>",
                "nextObject\\(\\)", "next()");
        lines = ReplicationUtils.replaceRegion(lines, "add_entries", List.of("" +
                "        if (columnSource.getType().isArray()) {\n" +
                "            Object2LongMaps.fastForEach(countValues, entry -> sorted\n" +
                "                    .add(Map.entry(ArrayTypeUtils.toString(entry.getKey()), entry.getLongValue())));\n"
                +
                "        } else {\n" +
                "            Object2LongMaps.fastForEach(countValues, entry -> sorted\n" +
                "                    .add(Map.entry(Objects.toString(entry.getKey()), entry.getLongValue())));\n" +
                "        }"));
        FileUtils.writeLines(objectFile, lines);
    }
}
