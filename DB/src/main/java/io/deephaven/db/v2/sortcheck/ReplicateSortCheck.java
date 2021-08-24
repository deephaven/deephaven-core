package io.deephaven.db.v2.sortcheck;

import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.compilertools.ReplicateUtilities;
import io.deephaven.db.v2.sort.ReplicateSortKernel;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static io.deephaven.compilertools.ReplicateUtilities.globalReplacements;
import static io.deephaven.compilertools.ReplicateUtilities.simpleFixup;

public class ReplicateSortCheck {
    public static void main(String[] args) throws IOException {
        final List<String> invertList = new ArrayList<>();

        invertList.add(ReplicatePrimitiveCode.pathForClass(CharSortCheck.class,
            ReplicatePrimitiveCode.MAIN_SRC));
        invertList.addAll(ReplicatePrimitiveCode.charToAllButBoolean(CharSortCheck.class,
            ReplicatePrimitiveCode.MAIN_SRC));
        final String objectPath = ReplicatePrimitiveCode.charToObject(CharSortCheck.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        invertList.add(objectPath);
        ReplicateUtilities.fixupChunkAttributes(objectPath);

        for (final String kernel : invertList) {
            invertSense(kernel, ascendingNameToDescendingName(kernel));
        }
    }

    private static void invertSense(String path, String descendingPath) throws IOException {
        final File file = new File(path);

        List<String> lines = simpleFixup(
            ascendingNameToDescendingName(path,
                FileUtils.readLines(file, Charset.defaultCharset())),
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
        // we should skip the replicate header
        return globalReplacements(3, lines, className, newName);
    }

    @NotNull
    private static String ascendingNameToDescendingName(String className) {
        return className.replace("SortCheck", "ReverseSortCheck");
    }
}
