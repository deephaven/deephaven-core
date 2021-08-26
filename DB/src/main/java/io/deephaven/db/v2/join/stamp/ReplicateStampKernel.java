package io.deephaven.db.v2.join.stamp;

import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.db.v2.join.dupcompact.ReplicateDupCompactKernel;
import io.deephaven.db.v2.sort.ReplicateSortKernel;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.compilertools.ReplicateUtilities.globalReplacements;

public class ReplicateStampKernel {
    public static void main(String[] args) throws IOException {
        final List<String> stampKernels =
                ReplicatePrimitiveCode.charToAllButBoolean(CharStampKernel.class, ReplicatePrimitiveCode.MAIN_SRC);
        final List<String> noExactStampKernels = ReplicatePrimitiveCode
                .charToAllButBoolean(CharNoExactStampKernel.class, ReplicatePrimitiveCode.MAIN_SRC);

        stampKernels.addAll(noExactStampKernels);
        final String charStampPath =
                ReplicatePrimitiveCode.pathForClass(CharStampKernel.class, ReplicatePrimitiveCode.MAIN_SRC);
        stampKernels.add(charStampPath);
        final String charNoExactStampPath =
                ReplicatePrimitiveCode.pathForClass(CharNoExactStampKernel.class, ReplicatePrimitiveCode.MAIN_SRC);
        stampKernels.add(charNoExactStampPath);

        final String objectStamp =
                ReplicatePrimitiveCode.charToObject(CharStampKernel.class, ReplicatePrimitiveCode.MAIN_SRC);
        fixupObjectStamp(objectStamp);
        final String objectNoExactStamp =
                ReplicatePrimitiveCode.charToObject(CharNoExactStampKernel.class, ReplicatePrimitiveCode.MAIN_SRC);
        fixupObjectStamp(objectNoExactStamp);

        stampKernels.add(objectStamp);
        stampKernels.add(objectNoExactStamp);

        stampKernels.add(ReplicateDupCompactKernel.fixupCharNullComparisons(CharStampKernel.class, charStampPath));
        stampKernels.add(
                ReplicateDupCompactKernel.fixupCharNullComparisons(CharNoExactStampKernel.class, charNoExactStampPath));

        for (String stampKernel : stampKernels) {
            final String stampReversePath = stampKernel.replaceAll("StampKernel", "ReverseStampKernel");
            invertSense(stampKernel, stampReversePath);

            if (stampKernel.contains("Double")) {
                ReplicateDupCompactKernel.nanFixup(stampKernel, "Double", true);
                ReplicateDupCompactKernel.nanFixup(stampReversePath, "Double", false);
            } else if (stampKernel.contains("Float")) {
                ReplicateDupCompactKernel.nanFixup(stampKernel, "Float", true);
                ReplicateDupCompactKernel.nanFixup(stampReversePath, "Float", false);
            }
        }
    }

    private static void invertSense(String path, String descendingPath) throws IOException {
        final File file = new File(path);

        List<String> lines = ascendingNameToDescendingName(path, FileUtils.readLines(file, Charset.defaultCharset()));

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
        final String newName = className.replace("StampKernel", "ReverseStampKernel");
        // we should skip the replicate header
        return globalReplacements(3, lines, className, newName);
    }

    private static void fixupObjectStamp(String objectPath) throws IOException {
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
