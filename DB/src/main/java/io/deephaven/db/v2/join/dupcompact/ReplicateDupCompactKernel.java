package io.deephaven.db.v2.join.dupcompact;

import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.compilertools.ReplicateUtilities;
import io.deephaven.db.util.DhCharComparisons;
import io.deephaven.db.v2.sort.ReplicateSortKernel;
import io.deephaven.util.QueryConstants;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.compilertools.ReplicateUtilities.*;

public class ReplicateDupCompactKernel {
    public static void main(String[] args) throws IOException {
        final List<String> kernelsToInvert =
                ReplicatePrimitiveCode.charToAllButBoolean(CharDupCompactKernel.class, ReplicatePrimitiveCode.MAIN_SRC);
        final String objectDupCompact =
                ReplicatePrimitiveCode.charToObject(CharDupCompactKernel.class, ReplicatePrimitiveCode.MAIN_SRC);
        fixupObjectDupCompact(objectDupCompact);

        kernelsToInvert
                .add(ReplicatePrimitiveCode.pathForClass(CharDupCompactKernel.class, ReplicatePrimitiveCode.MAIN_SRC));
        kernelsToInvert.add(objectDupCompact);
        for (String kernel : kernelsToInvert) {
            final String dupCompactReversePath = kernel.replaceAll("DupCompactKernel", "ReverseDupCompactKernel");
            invertSense(kernel, dupCompactReversePath);

            if (kernel.contains("Char")) {
                final String nullAwarePath = kernel.replace("CharDupCompactKernel", "NullAwareCharDupCompactKernel");
                fixupCharNullComparisons(CharDupCompactKernel.class, kernel, nullAwarePath, "CharDupCompactKernel",
                        "NullAwareCharDupCompactKernel", true);

                final String nullAwareDescendingPath =
                        nullAwarePath.replaceAll("NullAwareCharDupCompact", "NullAwareCharReverseDupCompact");
                fixupCharNullComparisons(CharDupCompactKernel.class, kernel, nullAwareDescendingPath,
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

    public static String fixupCharNullComparisons(Class sourceClass, String kernel) throws IOException {
        final String nullAwarePath = kernel.replace("Char", "NullAwareChar");
        return fixupCharNullComparisons(sourceClass, kernel, nullAwarePath, sourceClass.getSimpleName(),
                sourceClass.getSimpleName().replace("Char", "NullAwareChar"), true);
    }

    private static String fixupCharNullComparisons(Class sourceClass, String path, String newPath, String oldName,
            String newName, boolean ascending) throws IOException {
        final File file = new File(path);

        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());

        lines = ReplicateUtilities.addImport(lines, QueryConstants.class, DhCharComparisons.class);

        // we always replicate ascending then invert
        lines = globalReplacements(ReplicateSortKernel.fixupCharNullComparisons(lines, true), oldName, newName);

        if (!ascending) {
            lines = ReplicateSortKernel.invertComparisons(lines);
        }

        lines.addAll(0, Arrays.asList(
                "/* ---------------------------------------------------------------------------------------------------------------------",
                " * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit " + sourceClass.getSimpleName()
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

        lines = simpleFixup(lines, "eq", "lhs == rhs", "Dh" + type + "Comparisons.eq(lhs, rhs)");

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

    @NotNull
    private static List<String> fixupChunkAttributes(List<String> lines) {
        lines = lines.stream().map(x -> x.replaceAll("ObjectChunk<([^>]*)>", "ObjectChunk<Object, $1>"))
                .collect(Collectors.toList());
        return lines;
    }
}
