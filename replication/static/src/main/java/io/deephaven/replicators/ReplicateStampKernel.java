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

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.globalReplacements;

public class ReplicateStampKernel {
    private static final String TASK = "replicateStampKernel";

    public static void main(String[] args) throws IOException {
        final String charStampPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/join/stamp/CharStampKernel.java";
        final String charNoExactStampPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/join/stamp/CharNoExactStampKernel.java";
        final List<String> stampKernels = charToAllButBoolean(TASK, charStampPath);
        final List<String> noExactStampKernels = charToAllButBoolean(TASK, charNoExactStampPath);

        stampKernels.addAll(noExactStampKernels);
        stampKernels.add(charStampPath);
        stampKernels.add(charNoExactStampPath);

        final String objectStamp = charToObject(TASK, charStampPath);
        fixupObjectStamp(objectStamp);
        final String objectNoExactStamp = charToObject(TASK, charNoExactStampPath);
        fixupObjectStamp(objectNoExactStamp);

        stampKernels.add(objectStamp);
        stampKernels.add(objectNoExactStamp);

        for (String stampKernel : stampKernels) {
            final String stampReversePath = stampKernel.replaceAll("StampKernel", "ReverseStampKernel");
            invertSense(stampKernel, stampReversePath);
        }
    }

    private static void invertSense(String path, String descendingPath) throws IOException {
        final File file = new File(path);

        List<String> lines = ascendingNameToDescendingName(path, FileUtils.readLines(file, Charset.defaultCharset()));

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
        final String newName = className.replace("StampKernel", "ReverseStampKernel");

        lines = globalReplacements(
                lines.stream().dropWhile(line -> line.startsWith("//")).collect(Collectors.toList()),
                className, newName);

        // the header names the Char class that every variant is replicated from, and follows the class name
        // replacements because their patterns also match the source class name
        final String charClassName = className.replaceFirst("^(Byte|Short|Int|Long|Float|Double|Object)", "Char");
        return Stream.concat(ReplicationUtils.fileHeaderStream(TASK, charClassName), lines.stream())
                .collect(Collectors.toList());
    }

    private static void fixupObjectStamp(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        final List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        FileUtils.writeLines(objectFile,
                ReplicateSortKernel.fixupObjectComparisons(fixupChunkAttributes(lines), true, true));
    }

    @NotNull
    private static List<String> fixupChunkAttributes(List<String> lines) {
        lines = lines.stream().map(x -> x.replaceAll("ObjectChunk<([^>]*)>", "ObjectChunk<Object, $1>"))
                .collect(Collectors.toList());
        return lines;
    }
}
