package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.deephaven.replication.ReplicatePrimitiveCode.shortToAllIntegralTypes;
import static io.deephaven.replication.ReplicationUtils.globalReplacements;

public class ReplicateColumnStats {
    public static void main(String[] args) throws IOException {
        final List<String> paths = shortToAllIntegralTypes(
                "server/src/main/java/io/deephaven/server/table/stats/ShortChunkedNumericalStats.java");
        final String intPath =
                paths.stream().filter(p -> p.contains("Integer")).findFirst().orElseThrow(FileNotFoundException::new);
        fixupIntegerChunkName(intPath);

        ReplicatePrimitiveCode.floatToAllFloatingPoints(
                "server/src/main/java/io/deephaven/server/table/stats/FloatChunkedNumericalStats.java");

        final String objectPath = ReplicatePrimitiveCode.charToObject(
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
                "import gnu.trove.set.TObjectSet;",
                "import gnu.trove.set.hash.TObjectHashSet;",
                "import io.deephaven.util.QueryConstants;");
        lines = ReplicationUtils.addImport(lines,
                Set.class,
                HashSet.class);
        lines = globalReplacements(lines,
                "QueryConstants.NULL_OBJECT", "null",
                "\\? extends Attributes.Values", "?, ? extends Attributes.Values",
                "ObjectChunk<[?] ", "ObjectChunk<?, ? ",
                " TObjectLongMap", " TObjectLongMap<Object>",
                " TObjectLongHashMap", " TObjectLongHashMap<>",
                " TObjectSet", " Set<Object>",
                " TObjectHashSet", " HashSet<>",
                "new ChunkedObjectColumnIterator", "new ChunkedObjectColumnIterator<>",
                "\\(ObjectColumnIterator", "(ObjectColumnIterator<Object>",
                "nextObject\\(\\)", "next()");
        FileUtils.writeLines(objectFile, lines);
    }
}
