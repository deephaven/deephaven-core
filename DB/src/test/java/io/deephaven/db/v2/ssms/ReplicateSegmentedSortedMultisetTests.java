package io.deephaven.db.v2.ssms;

import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.compilertools.ReplicateUtilities;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.compilertools.ReplicateUtilities.*;

public class ReplicateSegmentedSortedMultisetTests {
    public static void main(String[] args) throws IOException {
        ReplicateSegmentedSortedMultiset.main(args);

        ReplicatePrimitiveCode.charToAllButBooleanAndFloats(TestCharSegmentedSortedMultiset.class,
                ReplicatePrimitiveCode.TEST_SRC);
        fixupFloatTests(ReplicatePrimitiveCode.charToFloat(TestCharSegmentedSortedMultiset.class,
                ReplicatePrimitiveCode.TEST_SRC, null));
        fixupFloatTests(ReplicatePrimitiveCode.charToDouble(TestCharSegmentedSortedMultiset.class,
                ReplicatePrimitiveCode.TEST_SRC, null));
        final String objectSsaTest = ReplicatePrimitiveCode.charToObject(TestCharSegmentedSortedMultiset.class,
                ReplicatePrimitiveCode.TEST_SRC);
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
        FileUtils.writeLines(objectFile, ReplicateUtilities.fixupChunkAttributes(lines));
    }
}
