package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateSegmentedSortedMultisetTests {
    public static void main(String[] args) throws IOException {
        ReplicateSegmentedSortedMultiset.main(args);

        charToAllButBooleanAndFloats(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestCharSegmentedSortedMultiset.java");
        fixupFloatTests(
                charToFloat(
                        "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestCharSegmentedSortedMultiset.java",
                        null));
        fixupFloatTests(charToDouble(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestCharSegmentedSortedMultiset.java",
                null));
        final String objectSsaTest =
                charToObject(
                        "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestCharSegmentedSortedMultiset.java");
        fixupObjectSsaTest(objectSsaTest);
    }

    private static void fixupFloatTests(String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = globalReplacements(lines, "/\\*EXTRA\\*/", ", .000001f");
        FileUtils.writeLines(file, lines);
    }

    private static void fixupObjectSsaTest(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = globalReplacements(lines, "NULL_OBJECT", "null",
                "new ObjectSegmentedSortedMultiset\\(nodeSize\\)",
                "new ObjectSegmentedSortedMultiset(nodeSize, Object.class)",
                "new ObjectSegmentedSortedMultiset\\(desc.nodeSize\\(\\)\\)",
                "new ObjectSegmentedSortedMultiset(desc.nodeSize(), Object.class)");
        lines = removeImport(lines, "\\s*import static.*QueryConstants.*;");
        lines = removeRegion(lines, "SortFixupSanityCheck");
        FileUtils.writeLines(objectFile, ReplicationUtils.fixupChunkAttributes(lines));
    }
}
