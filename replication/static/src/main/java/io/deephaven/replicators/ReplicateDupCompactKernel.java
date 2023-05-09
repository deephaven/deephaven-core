/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import io.deephaven.replication.ReplicationUtils;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.QueryConstants;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.replication.ReplicatePrimitiveCode.className;
import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateDupCompactKernel {
    public static void main(String[] args) throws IOException {
        final String charJavaPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/join/dupcompact/CharDupCompactKernel.java";
        final List<String> kernelsToInvert = ReplicatePrimitiveCode.charToAllButBoolean(charJavaPath);
        final String objectDupCompact = ReplicatePrimitiveCode.charToObject(charJavaPath);
        fixupObjectDupCompact(objectDupCompact);

        kernelsToInvert.add(charJavaPath);
        kernelsToInvert.add(objectDupCompact);
        for (String kernel : kernelsToInvert) {
            final String dupCompactReversePath = kernel.replaceAll("DupCompactKernel", "ReverseDupCompactKernel");
            invertSense(kernel, dupCompactReversePath);

            if (kernel.contains("Char")) {
                final String nullAwarePath = kernel.replace("CharDupCompactKernel", "NullAwareCharDupCompactKernel");
                fixupCharNullComparisons(kernel, nullAwarePath, "CharDupCompactKernel",
                        "NullAwareCharDupCompactKernel", true);

                final String nullAwareDescendingPath =
                        nullAwarePath.replaceAll("NullAwareCharDupCompact", "NullAwareCharReverseDupCompact");
                fixupCharNullComparisons(kernel, nullAwareDescendingPath,
                        "CharDupCompactKernel", "NullAwareCharReverseDupCompactKernel", false);
            } else if (kernel.contains("Float")) {
                nanFixup(kernel, "Float", true);
                nanFixup(dupCompactReversePath, "Float", false);
            } else if (kernel.contains("Double")) {
                nanFixup(kernel, "Double", true);
                nanFixup(dupCompactReversePath, "Double", false);
            }
        }
    }

    public static String fixupCharNullComparisons(String sourceClassJavaPath) throws IOException {
        final String sourceClassName = className(sourceClassJavaPath);
        final String nullAwarePath = sourceClassJavaPath.replace("Char", "NullAwareChar");
        return fixupCharNullComparisons(sourceClassJavaPath, nullAwarePath, sourceClassName,
                sourceClassName.replace("Char", "NullAwareChar"), true);
    }

    private static String fixupCharNullComparisons(String path, String newPath, String oldName, String newName,
            boolean ascending) throws IOException {
        final File file = new File(path);

        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());

        lines = ReplicationUtils.addImport(lines, QueryConstants.class, CharComparisons.class);

        // we always replicate ascending then invert
        lines = globalReplacements(ReplicateSortKernel.fixupCharNullComparisons(lines, true), oldName, newName);

        if (!ascending) {
            lines = ReplicateSortKernel.invertComparisons(lines);
        }

        // preserve the first comment of the file; typically the copyright
        int insertionPoint = 0;
        if (lines.size() > 0 && lines.get(0).startsWith("/*")) {
            for (int ii = 0; ii < lines.size(); ++ii) {
                final int offset = lines.get(ii).indexOf("*/");
                if (offset != -1) {
                    insertionPoint = ii + 1;
                    break;
                }
            }
        }

        lines.addAll(insertionPoint, Arrays.asList(
                "/* ---------------------------------------------------------------------------------------------------------------------",
                " * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit " + oldName
                        + " and regenerate",
                " * ------------------------------------------------------------------------------------------------------------------ */"));

        FileUtils.writeLines(new File(newPath), lines);

        return newPath;
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

    public static void nanFixup(String path, String type, boolean ascending) throws IOException {
        final File file = new File(path);

        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());

        lines = ReplicateSortKernel.fixupNanComparisons(lines, type, ascending);

        lines = simpleFixup(lines, "equality", "lhs == rhs", type + "Comparisons.eq(lhs, rhs)");

        FileUtils.writeLines(file, lines);
    }

    @NotNull
    private static List<String> ascendingNameToDescendingName(String path, List<String> lines) {
        final String className = new File(path).getName().replaceAll(".java$", "");
        final String newName = className.replace("DupCompactKernel", "ReverseDupCompactKernel");
        // we should skip the replicate header
        return globalReplacements(3, lines, className, newName);
    }

    private static void fixupObjectDupCompact(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        final List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        FileUtils.writeLines(objectFile, ReplicateSortKernel.fixupObjectComparisons(fixupChunkAttributes(lines)));
    }
}
