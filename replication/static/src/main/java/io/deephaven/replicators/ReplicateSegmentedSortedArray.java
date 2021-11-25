package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBoolean;
import static io.deephaven.replication.ReplicatePrimitiveCode.charToObject;
import static io.deephaven.replication.ReplicationUtils.globalReplacements;
import static io.deephaven.replication.ReplicationUtils.simpleFixup;

public class ReplicateSegmentedSortedArray {
    public static void main(String[] args) throws IOException {
        final String charSsaPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssa/CharSegmentedSortedArray.java";
        final List<String> ssas = charToAllButBoolean(charSsaPath);
        ssas.add(charSsaPath);

        invertSense(charSsaPath, descendingPath(charSsaPath));

        final String charNullSsaPath = ReplicateDupCompactKernel.fixupCharNullComparisons(charSsaPath);
        invertSense(charNullSsaPath, descendingPath(charNullSsaPath));

        final String objectSsa = charToObject(charSsaPath);
        fixupObjectSsa(objectSsa, true);

        ssas.add(objectSsa);
        for (String ssa : ssas) {
            final String ssaReverse = descendingPath(ssa);
            invertSense(ssa, ssaReverse);

            if (ssa.contains("Double")) {
                ReplicateDupCompactKernel.nanFixup(ssa, "Double", true);
                ReplicateDupCompactKernel.nanFixup(ssaReverse, "Double", false);
            } else if (ssa.contains("Float")) {
                ReplicateDupCompactKernel.nanFixup(ssa, "Float", true);
                ReplicateDupCompactKernel.nanFixup(ssaReverse, "Float", false);
            }
        }

        final String charChunkSsaStampPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssa/CharChunkSsaStamp.java";
        final List<String> chunkSsaStamps = charToAllButBoolean(charChunkSsaStampPath);
        chunkSsaStamps.add(charChunkSsaStampPath);

        invertSense(charChunkSsaStampPath, descendingPath(charChunkSsaStampPath));

        final String charNullChunkSsaStampPath =
                ReplicateDupCompactKernel.fixupCharNullComparisons(charChunkSsaStampPath);
        final String descendingCharNullChunkSsaStampPath = descendingPath(charNullChunkSsaStampPath);
        invertSense(charNullChunkSsaStampPath, descendingCharNullChunkSsaStampPath);
        fixupSsaName(charNullChunkSsaStampPath, "CharSegmentedSortedArray", "NullAwareCharSegmentedSortedArray");
        fixupSsaName(descendingCharNullChunkSsaStampPath, "CharReverseSegmentedSortedArray",
                "NullAwareCharReverseSegmentedSortedArray");

        final String objectSsaStamp = charToObject(charChunkSsaStampPath);
        fixupObjectSsa(objectSsaStamp, true);
        chunkSsaStamps.add(objectSsaStamp);

        for (String chunkSsaStamp : chunkSsaStamps) {
            final String chunkSsaStampReverse = descendingPath(chunkSsaStamp);
            invertSense(chunkSsaStamp, chunkSsaStampReverse);

            if (chunkSsaStamp.contains("Double")) {
                ReplicateDupCompactKernel.nanFixup(chunkSsaStamp, "Double", true);
                ReplicateDupCompactKernel.nanFixup(chunkSsaStampReverse, "Double", false);
            } else if (chunkSsaStamp.contains("Float")) {
                ReplicateDupCompactKernel.nanFixup(chunkSsaStamp, "Float", true);
                ReplicateDupCompactKernel.nanFixup(chunkSsaStampReverse, "Float", false);
            }
        }

        final String charSsaSsaStampPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssa/CharSsaSsaStamp.java";
        final List<String> ssaSsaStamps = charToAllButBoolean(charSsaSsaStampPath);
        ssaSsaStamps.add(charSsaSsaStampPath);

        invertSense(charSsaSsaStampPath, descendingPath(charSsaSsaStampPath));

        final String charNullSsaSsaStampPath = ReplicateDupCompactKernel.fixupCharNullComparisons(charSsaSsaStampPath);
        final String descendingCharNullSsaSsaStampPath = descendingPath(charNullSsaSsaStampPath);
        invertSense(charNullSsaSsaStampPath, descendingCharNullSsaSsaStampPath);
        fixupSsaName(charNullSsaSsaStampPath, "CharSegmentedSortedArray", "NullAwareCharSegmentedSortedArray");
        fixupSsaName(descendingCharNullSsaSsaStampPath, "CharReverseSegmentedSortedArray",
                "NullAwareCharReverseSegmentedSortedArray");

        final String objectSsaSsaStamp = charToObject(charSsaSsaStampPath);
        fixupObjectSsa(objectSsaSsaStamp, true);
        ssaSsaStamps.add(objectSsaSsaStamp);

        for (String ssaSsaStamp : ssaSsaStamps) {
            final String ssaSsaStampReverse = descendingPath(ssaSsaStamp);
            invertSense(ssaSsaStamp, ssaSsaStampReverse);

            if (ssaSsaStamp.contains("Double")) {
                ReplicateDupCompactKernel.nanFixup(ssaSsaStamp, "Double", true);
                ReplicateDupCompactKernel.nanFixup(ssaSsaStampReverse, "Double", false);
            } else if (ssaSsaStamp.contains("Float")) {
                ReplicateDupCompactKernel.nanFixup(ssaSsaStamp, "Float", true);
                ReplicateDupCompactKernel.nanFixup(ssaSsaStampReverse, "Float", false);
            }
        }

        final String charSsaCheckerPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssa/CharSsaChecker.java";
        final List<String> ssaCheckers = charToAllButBoolean(charSsaCheckerPath);
        ssaCheckers.add(charSsaCheckerPath);

        invertSense(charSsaCheckerPath, descendingPath(charSsaCheckerPath));

        final String objectSsaChecker = charToObject(charSsaCheckerPath);
        fixupObjectSsa(objectSsaChecker, true);
        ssaCheckers.add(objectSsaChecker);

        for (String ssaChecker : ssaCheckers) {
            final String ssaCheckerReverse = descendingPath(ssaChecker);
            invertSense(ssaChecker, ssaCheckerReverse);

            if (ssaChecker.contains("Double")) {
                ReplicateDupCompactKernel.nanFixup(ssaChecker, "Double", true);
                ReplicateDupCompactKernel.nanFixup(ssaCheckerReverse, "Double", false);
            } else if (ssaChecker.contains("Float")) {
                ReplicateDupCompactKernel.nanFixup(ssaChecker, "Float", true);
                ReplicateDupCompactKernel.nanFixup(ssaCheckerReverse, "Float", false);
            }
        }
    }

    private static void invertSense(String path, String descendingPath) throws IOException {
        final File file = new File(path);

        List<String> lines = ascendingNameToDescendingName(path, FileUtils.readLines(file, Charset.defaultCharset()));

        if (path.contains("ChunkSsaStamp") || path.contains("SsaSsaStamp") || path.contains("SsaChecker")) {
            lines = globalReplacements(3, lines, "\\BSegmentedSortedArray", "ReverseSegmentedSortedArray");
        }

        if (path.contains("SegmentedSortedArray")) {
            lines = globalReplacements(3, lines, "\\BSsaChecker", "ReverseSsaChecker");
        }


        lines = simpleFixup(lines, "isReversed", "false", "true");

        if (path.contains("Object")) {
            lines = ReplicateSortKernel.fixupObjectComparisons(lines, false);
        } else {
            lines = ReplicateSortKernel.invertComparisons(lines);
        }

        System.out.println("Generating descending file " + descendingPath);
        FileUtils.writeLines(new File(descendingPath), lines);
    }

    private static void fixupSsaName(String path, String oldName, String newName) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = globalReplacements(3, lines, oldName, newName);
        FileUtils.writeLines(new File(path), lines);
    }

    @NotNull
    private static List<String> ascendingNameToDescendingName(String path, List<String> lines) {
        final String className = new File(path).getName().replaceAll(".java$", "");
        final String newName = descendingPath(className);
        // we should skip the replicate header
        return globalReplacements(3, lines, className, newName);
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
        FileUtils.writeLines(objectFile, ReplicationUtils.simpleFixup(
                ReplicateSortKernel.fixupObjectComparisons(ReplicationUtils.fixupChunkAttributes(lines), ascending),
                "fillValue", "Object.MIN_VALUE", "null"));
    }
}
