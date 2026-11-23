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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBoolean;
import static io.deephaven.replication.ReplicatePrimitiveCode.charToObject;
import static io.deephaven.replication.ReplicationUtils.globalReplacements;
import static io.deephaven.replication.ReplicationUtils.replicatedOutputPath;
import static io.deephaven.replication.ReplicationUtils.simpleFixup;

public class ReplicateSortCheck {
    private static final String TASK = "replicateSortCheck";

    public static void main(String[] args) throws IOException {
        final List<String> invertList = new ArrayList<>();

        final String charSortCheckPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sortcheck/CharSortCheck.java";
        invertList.add(charSortCheckPath);
        invertList.addAll(charToAllButBoolean(TASK, charSortCheckPath));
        final String objectPath = charToObject(TASK, charSortCheckPath);
        invertList.add(objectPath);
        ReplicationUtils.fixupChunkAttributes(objectPath);

        for (final String kernel : invertList) {
            final File kernelFile = new File(kernel);
            invertSense(kernel, replicatedOutputPath(TASK,
                    new File(kernelFile.getParent(), ascendingNameToDescendingName(kernelFile.getName())).getPath()));
        }
    }

    private static void invertSense(String path, String descendingPath) throws IOException {
        final File file = new File(path);

        List<String> lines =
                simpleFixup(ascendingNameToDescendingName(path, FileUtils.readLines(file, Charset.defaultCharset())),
                        "initialize last", "MIN_VALUE", "MAX_VALUE");

        lines = ReplicateSortKernel.invertComparisons(lines);

        FileUtils.writeLines(new File(descendingPath), lines);
    }

    @NotNull
    private static List<String> ascendingNameToDescendingName(String path, List<String> lines) {
        final String className = new File(path).getName().replaceAll(".java$", "");
        final String newName = ascendingNameToDescendingName(className);

        lines = globalReplacements(
                lines.stream().dropWhile(line -> line.startsWith("//")).collect(Collectors.toList()),
                className, newName);

        // the header names the Char class that every variant is replicated from, and follows the class name
        // replacements because their patterns also match the source class name
        final String charClassName = className.replaceFirst("^(Byte|Short|Int|Long|Float|Double|Object)", "Char");
        return Stream.concat(ReplicationUtils.fileHeaderStream(TASK, charClassName),
                lines.stream()).collect(Collectors.toList());
    }

    @NotNull
    private static String ascendingNameToDescendingName(String className) {
        return className.replace("SortCheck", "ReverseSortCheck");
    }
}
