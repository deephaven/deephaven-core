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
    private static final String TASK = "replicateSegmentedSortedMultisetTests";

    public static void main(String[] args) throws IOException {
        redirectOutputToReplicated(TASK);

        ReplicateSegmentedSortedMultiset.main(args);

        // Replicate TestFloatSegmentedSortedMultisetSpecialValues -> TestDoubleSegmentedSortedMultisetSpecialValues
        floatToAllFloatingPoints(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestFloatSegmentedSortedMultisetSpecialValues.java");
        final File doubleSpecialValuesTest = new File(replicatedOutputPath(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestDoubleSegmentedSortedMultisetSpecialValues.java"));
        List<String> doubleSpecialValuesLines = FileUtils.readLines(doubleSpecialValuesTest, Charset.defaultCharset());
        doubleSpecialValuesLines = globalReplacements(doubleSpecialValuesLines,
                "Double\\.intBitsToDouble\\(0x7fc12345\\)", "Double.longBitsToDouble(0x7ff8000000000001L)",
                "Double\\.doubleToRawIntBits", "Double.doubleToRawLongBits",
                "0\\.0f", "0.0d",
                "canonical 0x7fc00000", "canonical 0x7ff8000000000000L");
        FileUtils.writeLines(doubleSpecialValuesTest, doubleSpecialValuesLines);

        charToAllButBooleanAndFloats(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestCharSegmentedSortedMultiset.java");
        fixupFloatTests(
                charToFloat(TASK,
                        "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestCharSegmentedSortedMultiset.java",
                        null));
        fixupFloatTests(charToDouble(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestCharSegmentedSortedMultiset.java",
                null));
        final String objectSsmTest =
                charToObject(TASK,
                        "engine/table/src/test/java/io/deephaven/engine/table/impl/ssms/TestCharSegmentedSortedMultiset.java");
        fixupObjectSsmTest(objectSsmTest);
        ReplicateSegmentedSortedArray.equalsConsistentObjectCopy(TASK,
                "TestCharSegmentedSortedMultiset", objectSsmTest);

        final String compactModificationsTest =
                "engine/table/src/test/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/compactmodifications/TestCharCompactModifications.java";
        charToAllButBooleanAndFloats(TASK, compactModificationsTest);
        fixupFloatTests(charToFloat(TASK, compactModificationsTest, null));
        fixupFloatTests(charToDouble(TASK, compactModificationsTest, null));
        fixupObjectCompactModificationsTest(
                charToObject(TASK, compactModificationsTest));
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

    private static void fixupObjectSsmTest(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = globalReplacements(lines, "NULL_OBJECT", "null",
                "new ObjectSegmentedSortedMultiset\\(nodeSize\\)",
                "new ObjectSegmentedSortedMultiset(nodeSize, Object.class)",
                "new ObjectSegmentedSortedMultiset\\(desc.nodeSize\\(\\)\\)",
                "new ObjectSegmentedSortedMultiset(desc.nodeSize(), Object.class)");
        // there is no ValueIteratorOfObject; the Object SSM iterates as a generic ValueIterator<Object>, whose
        // element accessor is Iterator.next() rather than the primitive variants' nextObject()
        lines = globalReplacements(lines,
                "import io\\.deephaven\\.engine\\.primitive\\.value\\.iterator\\.ValueIteratorOfObject;",
                "import io.deephaven.engine.primitive.value.iterator.ValueIterator;",
                "ValueIteratorOfObject", "ValueIterator<Object>",
                "\\.nextObject\\(\\)", ".next()");
        lines = removeImport(lines, "\\s*import static.*QueryConstants.*;");
        lines = removeRegion(lines, "SortFixupSanityCheck");
        // the null-sentinel equality semantics are specific to the primitive types; that region holds the only
        // HashMap use, so its import goes with it
        lines = removeRegion(lines, "NullEquals");
        lines = removeImport(lines, "\\s*import java\\.util\\.HashMap;");
        // an Object SSM *is* an ObjectVector, so a boxed Vector of its values is equal -- the assertions just above,
        // which the primitive-vector case replicates into, already cover it
        lines = removeRegion(lines, "BoxedEquals");
        FileUtils.writeLines(objectFile, ReplicationUtils.fixupChunkAttributes(lines));
    }
}
