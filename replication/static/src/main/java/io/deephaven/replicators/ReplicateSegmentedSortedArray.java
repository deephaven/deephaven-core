//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBoolean;
import static io.deephaven.replication.ReplicatePrimitiveCode.charToObject;
import static io.deephaven.replication.ReplicationUtils.globalReplacements;
import static io.deephaven.replication.ReplicationUtils.simpleFixup;

public class ReplicateSegmentedSortedArray {
    private static final String TASK = "replicateSegmentedSortedArray";

    public static void main(String[] args) throws IOException {
        final String charSsaPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssa/CharSegmentedSortedArray.java";
        final List<String> ssas = charToAllButBoolean(TASK, charSsaPath);
        ssas.add(charSsaPath);

        final String objectSsa = charToObject(TASK, charSsaPath);
        fixupObjectSsa(objectSsa, true);

        ssas.add(objectSsa);
        for (String ssa : ssas) {
            final String ssaReverse = descendingPath(ssa);
            invertSense(ssa, ssaReverse);
        }

        final String charChunkSsaStampPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssa/CharChunkSsaStamp.java";
        final List<String> chunkSsaStamps = charToAllButBoolean(TASK, charChunkSsaStampPath);
        chunkSsaStamps.add(charChunkSsaStampPath);

        final String objectSsaStamp = charToObject(TASK, charChunkSsaStampPath);
        fixupObjectSsa(objectSsaStamp, true);
        chunkSsaStamps.add(objectSsaStamp);

        for (String chunkSsaStamp : chunkSsaStamps) {
            final String chunkSsaStampReverse = descendingPath(chunkSsaStamp);
            invertSense(chunkSsaStamp, chunkSsaStampReverse);
        }

        final String charSsaSsaStampPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssa/CharSsaSsaStamp.java";
        final List<String> ssaSsaStamps = charToAllButBoolean(TASK, charSsaSsaStampPath);
        ssaSsaStamps.add(charSsaSsaStampPath);

        final String objectSsaSsaStamp = charToObject(TASK, charSsaSsaStampPath);
        fixupObjectSsa(objectSsaSsaStamp, true);
        ssaSsaStamps.add(objectSsaSsaStamp);

        for (String ssaSsaStamp : ssaSsaStamps) {
            final String ssaSsaStampReverse = descendingPath(ssaSsaStamp);
            invertSense(ssaSsaStamp, ssaSsaStampReverse);
        }

        // the checkers exist only to validate an SSA's contents from a test, so they live in the test source set
        final String charSsaCheckerPath =
                "engine/table/src/test/java/io/deephaven/engine/table/impl/ssa/CharSsaChecker.java";
        final List<String> ssaCheckers = charToAllButBoolean(TASK, charSsaCheckerPath);
        ssaCheckers.add(charSsaCheckerPath);

        final String objectSsaChecker = charToObject(TASK, charSsaCheckerPath);
        fixupObjectSsa(objectSsaChecker, true);
        ssaCheckers.add(objectSsaChecker);

        for (String ssaChecker : ssaCheckers) {
            final String ssaCheckerReverse = descendingPath(ssaChecker);
            invertSense(ssaChecker, ssaCheckerReverse);
        }

        for (final String objectPath : List.of(objectSsa, objectSsaStamp, objectSsaSsaStamp, objectSsaChecker)) {
            final String charClassName = ReplicationUtils.className(objectPath).replaceFirst("^Object", "Char");
            equalsConsistentObjectCopy(TASK, charClassName, objectPath);
            equalsConsistentObjectCopy(TASK, charClassName, descendingPath(objectPath));
        }
    }

    /**
     * Matches the Object SSA, stamp, checker, dup compact, compact, compact modifications and SSM class names (and the
     * SSA and SSM test class names), capturing the optional Test prefix.
     */
    private static final String OBJECT_CLASS_PATTERN =
            "\\b(Test)?Object(?=(Reverse)?(SegmentedSortedArray|ChunkSsaStamp|SsaSsaStamp|SsaChecker|DupCompactKernel|CompactKernel|CompactModifications|SegmentedSortedMultiset)\\b)";
    private static final String OBJECT_CLASS_REPLACEMENT = "$1EqualsConsistentObject";

    /**
     * Write the EqualsConsistentObject counterpart of a generated Object class, next to it. The Object class tests
     * equality with {@code ObjectComparisons.compareEquals}, which is correct for any Comparable; the counterpart tests
     * equality with {@code ObjectComparisons.eq}, which is correct only for data types whose natural ordering is
     * consistent with equals. References to the other Object SSA, stamp, checker, dup compact, compact, compact
     * modifications and SSM classes become references to their EqualsConsistentObject counterparts.
     *
     * @param task the gradle task that regenerates the copy
     * @param sourceClassName the name of the class to edit to change the copy
     * @param objectPath the path of the generated Object class
     * @return the path of the EqualsConsistentObject class
     */
    static String equalsConsistentObjectCopy(final String task, final String sourceClassName,
            final String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        final String copyPath = new File(objectFile.getParentFile(),
                objectFile.getName().replaceAll(OBJECT_CLASS_PATTERN, OBJECT_CLASS_REPLACEMENT)).getPath();
        if (copyPath.equals(objectPath)) {
            throw new IllegalArgumentException(
                    objectPath
                            + " is not an Object SSA, stamp, checker, dup compact, compact, compact modifications or SSM class");
        }

        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = Stream.concat(
                // the generated classes put the package line directly after the header, with no blank line
                ReplicationUtils.fileHeaderStream(task, sourceClassName).filter(line -> !line.isEmpty()),
                lines.stream().dropWhile(line -> line.startsWith("//") || line.isEmpty()))
                .collect(Collectors.toList());
        lines = globalReplacements(lines, OBJECT_CLASS_PATTERN, OBJECT_CLASS_REPLACEMENT);

        if (lines.stream().anyMatch(line -> line.contains("region equality function"))) {
            lines = simpleFixup(lines, "equality function", "ObjectComparisons\\.compareEquals\\(lhs, rhs\\)",
                    "ObjectComparisons.eq(lhs, rhs)");
            if (lines.stream().noneMatch(line -> line.contains("ObjectComparisons.eq(lhs, rhs)"))) {
                throw new IllegalStateException(
                        objectPath + ": equality function region does not use ObjectComparisons.compareEquals");
            }
        }

        System.out.println("Generating equals consistent file " + copyPath);
        FileUtils.writeLines(new File(copyPath), lines);
        return copyPath;
    }

    private static void invertSense(String path, String descendingPath) throws IOException {
        final File file = new File(path);

        List<String> lines = ascendingNameToDescendingName(path, FileUtils.readLines(file, Charset.defaultCharset()));

        // Skip, re-add file header
        lines = Stream.concat(
                ReplicationUtils.fileHeaderStream(TASK, ReplicationUtils.className(path)),
                lines.stream().dropWhile(line -> line.startsWith("//"))).collect(Collectors.toList());

        if (path.contains("ChunkSsaStamp") || path.contains("SsaSsaStamp") || path.contains("SsaChecker")) {
            lines = globalReplacements(lines, "\\BSegmentedSortedArray", "ReverseSegmentedSortedArray");
        }

        if (path.contains("SegmentedSortedArray")) {
            lines = globalReplacements(lines, "\\BSsaChecker", "ReverseSsaChecker");
        }


        lines = simpleFixup(lines, "isReversed", "false", "true");

        if (path.contains("Object")) {
            lines = ReplicateSortKernel.fixupObjectComparisons(lines, false, true);
        } else {
            lines = ReplicateSortKernel.invertComparisons(lines);
        }

        System.out.println("Generating descending file " + descendingPath);
        FileUtils.writeLines(new File(descendingPath), lines);
    }

    @NotNull
    private static List<String> ascendingNameToDescendingName(String path, List<String> lines) {
        final String className = new File(path).getName().replaceAll(".java$", "");
        final String newName = descendingPath(className);

        // Skip, re-add file header
        lines = Stream.concat(
                ReplicationUtils.fileHeaderStream(TASK, ReplicationUtils.className(path)),
                lines.stream().dropWhile(line -> line.startsWith("//"))).collect(Collectors.toList());

        return globalReplacements(lines, className, newName);
    }

    @NotNull
    private static String descendingPath(String className) {
        return className.replace("SegmentedSortedArray", "ReverseSegmentedSortedArray")
                .replace("SsaSsaStamp", "ReverseSsaSsaStamp")
                .replace("ChunkSsaStamp", "ReverseChunkSsaStamp")
                .replace("SsaChecker", "ReverseSsaChecker");
    }


    private static void fixupObjectSsa(String objectPath, boolean ascending) throws IOException {
        final File objectFile = new File(objectPath);
        final List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        FileUtils.writeLines(objectFile, ReplicationUtils.replaceRegion(ReplicationUtils.simpleFixup(
                ReplicateSortKernel.fixupObjectComparisons(ReplicationUtils.fixupChunkAttributes(lines), ascending,
                        true),
                "fillValue", "Object.MIN_VALUE", "null"),
                "clearValues", List.of("        Arrays.fill(values, from, to, null);")));
    }
}
