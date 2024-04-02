//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBoolean;
import static io.deephaven.replication.ReplicatePrimitiveCode.charToObject;
import static io.deephaven.replication.ReplicationUtils.globalReplacements;
import static io.deephaven.replication.ReplicationUtils.simpleFixup;

public class ReplicateSortCheck {
    public static void main(String[] args) throws IOException {
        final List<String> invertList = new ArrayList<>();

        final String charSortCheckPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sortcheck/CharSortCheck.java";
        invertList.add(charSortCheckPath);
        invertList.addAll(charToAllButBoolean("replicateSortCheck", charSortCheckPath));
        final String objectPath = charToObject("replicateSortCheck", charSortCheckPath);
        invertList.add(objectPath);
        ReplicationUtils.fixupChunkAttributes(objectPath);

        for (final String kernel : invertList) {
            invertSense(kernel, ascendingNameToDescendingName(kernel));
        }
    }

    private static void invertSense(String path, String descendingPath) throws IOException {
        final File file = new File(path);

        List<String> lines =
                simpleFixup(ascendingNameToDescendingName(path, FileUtils.readLines(file, Charset.defaultCharset())),
                        "initialize last", "MIN_VALUE", "MAX_VALUE");

        if (path.contains("Object")) {
            lines = ReplicateSortKernel.fixupObjectComparisons(lines, false);
        } else {
            lines = ReplicateSortKernel.invertComparisons(lines);
        }

        FileUtils.writeLines(new File(descendingPath), lines);
    }

    @NotNull
    private static List<String> ascendingNameToDescendingName(String path, List<String> lines) {
        final String className = new File(path).getName().replaceAll(".java$", "");
        final String newName = ascendingNameToDescendingName(className);

        // Skip, re-add file header
        lines = Stream.concat(
                ReplicationUtils.fileHeaderStream("replicateSortCheck", ReplicationUtils.className(path)),
                lines.stream().dropWhile(line -> line.startsWith("//"))).collect(Collectors.toList());

        return globalReplacements(lines, className, newName);
    }

    @NotNull
    private static String ascendingNameToDescendingName(String className) {
        return className.replace("SortCheck", "ReverseSortCheck");
    }
}
