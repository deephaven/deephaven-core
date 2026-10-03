//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateDupCompactKernel {
    public static void main(String[] args) throws IOException {
        final String charJavaPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/join/dupcompact/CharDupCompactKernel.java";
        final List<String> kernelsToInvert =
                ReplicatePrimitiveCode.charToAllButBoolean("replicateDupCompactKernel", charJavaPath);
        final String objectDupCompact = ReplicatePrimitiveCode.charToObject("replicateDupCompactKernel", charJavaPath);
        fixupObjectDupCompact(objectDupCompact);

        kernelsToInvert.add(charJavaPath);
        kernelsToInvert.add(objectDupCompact);
        for (String kernel : kernelsToInvert) {
            final String dupCompactReversePath = kernel.replaceAll("DupCompactKernel", "ReverseDupCompactKernel");
            invertSense(kernel, dupCompactReversePath);
        }

        ReplicateSegmentedSortedArray.equalsConsistentObjectCopy("replicateDupCompactKernel", "CharDupCompactKernel",
                objectDupCompact);
        ReplicateSegmentedSortedArray.equalsConsistentObjectCopy("replicateDupCompactKernel", "CharDupCompactKernel",
                objectDupCompact.replaceAll("DupCompactKernel", "ReverseDupCompactKernel"));
    }

    private static void invertSense(String path, String descendingPath) throws IOException {
        final File file = new File(path);

        List<String> lines =
                simpleFixup(ascendingNameToDescendingName(path, FileUtils.readLines(file, Charset.defaultCharset())),
                        "initialize last", "MIN_VALUE", "MAX_VALUE");

        if (path.contains("Object")) {
            lines = ReplicateSortKernel.fixupObjectComparisons(lines, false, true);
        } else {
            lines = ReplicateSortKernel.invertComparisons(lines);
        }

        FileUtils.writeLines(new File(descendingPath), lines);
    }

    @NotNull
    private static List<String> ascendingNameToDescendingName(String path, List<String> lines) {
        final String className = new File(path).getName().replaceAll(".java$", "");
        final String newName = className.replace("DupCompactKernel", "ReverseDupCompactKernel");

        lines = globalReplacements(
                lines.stream().dropWhile(line -> line.startsWith("//")).collect(Collectors.toList()),
                className, newName);

        // the header names the Char class that every variant is replicated from, and follows the class name
        // replacements because their patterns also match the source class name
        final String charClassName = className.replaceFirst("^(Byte|Short|Int|Long|Float|Double|Object)", "Char");
        return Stream.concat(ReplicationUtils.fileHeaderStream("replicateDupCompactKernel", charClassName),
                lines.stream()).collect(Collectors.toList());
    }

    private static void fixupObjectDupCompact(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        final List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        FileUtils.writeLines(objectFile,
                ReplicateSortKernel.fixupObjectComparisons(fixupChunkAttributes(lines), true, true));
    }
}
