//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
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

        // Replicate TestFloatSegmentedSortedMultisetSpecialValues -> TestDoubleSegmentedSortedMultisetSpecialValues
        floatToAllFloatingPoints("replicateSegmentedSortedMultisetTests",
                "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestFloatSegmentedSortedMultisetSpecialValues.java");
        final File doubleSpecialValuesTest = new File(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestDoubleSegmentedSortedMultisetSpecialValues.java");
        List<String> doubleSpecialValuesLines = FileUtils.readLines(doubleSpecialValuesTest, Charset.defaultCharset());
        doubleSpecialValuesLines = globalReplacements(doubleSpecialValuesLines,
                "Double\\.intBitsToDouble\\(0x7fc12345\\)", "Double.longBitsToDouble(0x7ff8000000000001L)",
                "Double\\.doubleToRawIntBits", "Double.doubleToRawLongBits",
                "0\\.0f", "0.0d",
                "canonical 0x7fc00000", "canonical 0x7ff8000000000000L");
        FileUtils.writeLines(doubleSpecialValuesTest, doubleSpecialValuesLines);

        charToAllButBooleanAndFloats("replicateSegmentedSortedMultisetTests",
                "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestCharSegmentedSortedMultiset.java");
        fixupFloatTests(
                charToFloat("replicateSegmentedSortedMultisetTests",
                        "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestCharSegmentedSortedMultiset.java",
                        null));
        fixupFloatTests(charToDouble("replicateSegmentedSortedMultisetTests",
                "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestCharSegmentedSortedMultiset.java",
                null));
        final String objectSsaTest =
                charToObject("replicateSegmentedSortedMultisetTests",
                        "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestCharSegmentedSortedMultiset.java");
        fixupObjectSsaTest(objectSsaTest);

        final String compactModificationsTest =
                "engine/table/src/test/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/compactmodifications/TestCharCompactModifications.java";
        charToAllButBooleanAndFloats("replicateSegmentedSortedMultisetTests", compactModificationsTest);
        fixupFloatTests(charToFloat("replicateSegmentedSortedMultisetTests", compactModificationsTest, null));
        fixupFloatTests(charToDouble("replicateSegmentedSortedMultisetTests", compactModificationsTest, null));
        fixupObjectCompactModificationsTest(
                charToObject("replicateSegmentedSortedMultisetTests", compactModificationsTest));
    }

    private static void fixupObjectCompactModificationsTest(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = globalReplacements(lines, "NULL_OBJECT", "null");
        lines = removeImport(lines, "\\s*import static.*QueryConstants.*;");
        FileUtils.writeLines(objectFile, ReplicationUtils.fixupChunkAttributes(lines));
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
        // the null-sentinel equality semantics are specific to the primitive types
        lines = removeRegion(lines, "NullEquals");
        // CharVectorDirect replicates to ObjectVectorDirect, which is already imported explicitly; drop the duplicate
        // (removeImport removes a single occurrence per pattern, leaving exactly one)
        lines = removeImport(lines, "\\s*import io.deephaven.vector.ObjectVectorDirect;");
        FileUtils.writeLines(objectFile, ReplicationUtils.fixupChunkAttributes(lines));
    }
}
