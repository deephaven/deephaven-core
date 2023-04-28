/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.CharComparisons;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateSortKernel {
    public static void main(String[] args) throws IOException {
        replicateLongToInt();
        replicateLongToByte();
        doCharReplication(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/timsort/CharLongTimsortKernel.java");
        doCharReplication(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/timsort/CharIntTimsortKernel.java");
        doCharReplication(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/timsort/CharByteTimsortKernel.java");

        doCharMegaMergeReplication(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/megamerge/CharLongMegaMergeKernel.java");

        charToAllButBooleanAndFloats(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/findruns/CharFindRunsKernel.java");
        final String doubleRunPath = charToDouble(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/findruns/CharFindRunsKernel.java",
                Collections.emptyMap());
        fixupDoubleRuns(doubleRunPath);
        final String floatRunPath = charToFloat(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/findruns/CharFindRunsKernel.java",
                Collections.emptyMap());
        fixupFloatRuns(floatRunPath);
        final String objectRunPath = charToObject(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/findruns/CharFindRunsKernel.java");
        fixupObjectRuns(objectRunPath);

        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/partition/CharPartitionKernel.java");
        final String objectPartitionPath = charToObject(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/partition/CharPartitionKernel.java");
        fixupObjectPartition(objectPartitionPath);

        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/permute/CharPermuteKernel.java");
        fixupObjectPermute(charToObject(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/permute/CharPermuteKernel.java"));
    }

    private static void doCharReplication(@NotNull final String sourceClassJavaPath) throws IOException {
        // replicate char to each of the other types
        final List<String> timsortPaths =
                charToAllButBoolean(sourceClassJavaPath);
        final String objectSortPath = charToObject(sourceClassJavaPath);
        timsortPaths.add(sourceClassJavaPath);
        timsortPaths.add(objectSortPath);

        // now replicate each type to a descending kernel, and swap the sense of gt, lt, geq, and leq
        for (final String path : timsortPaths) {
            final String descendingPath = path.replace("TimsortKernel", "TimsortDescendingKernel");

            if (path.contains("Double") || path.contains("Float")) {
                FileUtils.copyFile(new File(path), new File(descendingPath));

                // first we need to figure out what to do with the NaNs in our ascending kernel
                fixupNanComparisons(path, true);

                // we still need a descending kernel
                System.out.println("Descending FP Path: " + descendingPath);
                // we are going to fix it up ascending, then follow it up with a sense inversion
                fixupNanComparisons(descendingPath, true);
                invertSense(path, descendingPath);
            } else if (path.contains("Char")) {
                final String sourceClassName = className(sourceClassJavaPath);
                final String nullAwareAscendingName = "NullAware" + sourceClassName;
                final String nullAwarePath = path.replace(sourceClassName, nullAwareAscendingName);
                final String nullAwareDescendingPath =
                        nullAwarePath.replaceAll("TimsortKernel", "TimsortDescendingKernel");

                fixupCharNullComparisons(sourceClassJavaPath, path, nullAwarePath, sourceClassName,
                        nullAwareAscendingName, true);
                // we are going to fix it up ascending, then follow it up with a sense inversion
                fixupCharNullComparisons(sourceClassJavaPath, path, nullAwareDescendingPath, sourceClassName,
                        nullAwareAscendingName, true);
                invertSense(nullAwareDescendingPath, nullAwareDescendingPath);
            } else if (path.contains("Object")) {
                FileUtils.copyFile(new File(path), new File(descendingPath));

                fixupObjectTimSort(path, true);
                System.out.println("Descending Object Path: " + descendingPath);
                fixupObjectTimSort(descendingPath, false);
            } else {
                System.out.println("Descending Path: " + descendingPath);
                invertSense(path, descendingPath);
            }
        }
    }

    private static void doCharMegaMergeReplication(String sourceClassJavaPath) throws IOException {
        // replicate char to each of the other types
        final List<String> megaMergePaths = charToAllButBoolean(sourceClassJavaPath);
        final String objectSortPath = charToObject(sourceClassJavaPath);
        megaMergePaths.add(sourceClassJavaPath);
        megaMergePaths.add(objectSortPath);

        // now replicate each type to a descending kernel, and swap the sense of gt, lt, geq, and leq
        for (final String path : megaMergePaths) {
            final String descendingPath = path.replace("LongMegaMergeKernel", "LongMegaMergeDescendingKernel");
            if (path.contains("Object")) {
                FileUtils.copyFile(new File(path), new File(descendingPath));
                fixupObjectMegaMerge(objectSortPath, true);
                fixupObjectMegaMerge(descendingPath, false);
            } else {
                invertSense(path, descendingPath);
            }
        }
    }

    private static void replicateLongToInt() throws IOException {
        final String intSortKernelPath = longToInt(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/LongSortKernel.java");
        fixupIntSortKernel(intSortKernelPath);
        longToInt(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/timsort/CharLongTimsortKernel.java");
        longToInt(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/radix/BooleanLongRadixSortKernel.java");
    }

    private static void replicateLongToByte() throws IOException {
        final String byteSortKernelPath = longToByte(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/LongSortKernel.java");
        fixupByteSortKernel(byteSortKernelPath);
        longToByte("engine/table/src/main/java/io/deephaven/engine/table/impl/sort/timsort/CharLongTimsortKernel.java");
        longToByte(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sort/radix/BooleanLongRadixSortKernel.java");
    }

    private static void fixupIntSortKernel(String intSortKernelPath) throws IOException {
        final List<String> longCase = Arrays.asList("case Long:",
                "if (order == SortingOrder.Ascending) {",
                "    return LongIntTimsortKernel.createContext(size);",
                "} else {",
                "    return LongIntTimsortDescendingKernel.createContext(size);",
                "}");

        final File file = new File(intSortKernelPath);
        final List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        FileUtils.writeLines(file, replaceRegion(lines, "lngcase", indent(longCase, 16)));
    }

    private static void fixupByteSortKernel(String byteSortKernelPath) throws IOException {
        final List<String> longCase = Arrays.asList("case Long:",
                "if (order == SortingOrder.Ascending) {",
                "    return LongByteTimsortKernel.createContext(size);",
                "} else {",
                "    return LongByteTimsortDescendingKernel.createContext(size);",
                "}");

        final File file = new File(byteSortKernelPath);
        final List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        FileUtils.writeLines(file, replaceRegion(lines, "lngcase", indent(longCase, 16)));
    }

    private static void invertSense(String path, String descendingPath) throws IOException {
        final File file = new File(path);

        final List<String> lines = ascendingNameToDescendingName(FileUtils.readLines(file, Charset.defaultCharset()));

        FileUtils.writeLines(new File(descendingPath), invertComparisons(lines));
    }

    @NotNull
    private static List<String> ascendingNameToDescendingName(List<String> lines) {
        // we should skip the replicate header
        return globalReplacements(3, lines, "TimsortKernel", "TimsortDescendingKernel", "\\BLongMegaMergeKernel",
                "LongMegaMergeDescendingKernel");
    }

    private static void fixupObjectTimSort(String objectPath, boolean ascending) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());

        if (!ascending) {
            lines = ascendingNameToDescendingName(lines);
        }

        lines = fixupChunkAttributes(lines);

        FileUtils.writeLines(objectFile, fixupObjectComparisons(lines, ascending));
    }

    private static void fixupObjectMegaMerge(String objectPath, boolean ascending) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());

        if (!ascending) {
            lines = ascendingNameToDescendingName(lines);
            lines = invertComparisons(lines);
        }

        lines = fixupChunkAttributes(lines);

        FileUtils.writeLines(objectFile, fixupColumnSourceGetObject(lines));
    }

    private static List<String> fixupColumnSourceGetObject(List<String> lines) {
        return globalReplacements(lines, "getObject\\(", "get\\(");
    }

    private static void fixupObjectPermute(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());

        lines = fixupTypedChunkAttributes(lines);
        lines = lines.stream()
                .map(x -> x.replaceAll("asObjectChunk\\(\\)", "<Object>asObjectChunk()"))
                .map(x -> x.replaceAll("asWritableObjectChunk\\(\\)", "<Object>asWritableObjectChunk()"))
                .collect(Collectors.toList());

        FileUtils.writeLines(objectFile, lines);
    }

    @NotNull
    private static List<String> fixupTypedChunkAttributes(List<String> lines) {
        lines = lines.stream()
                .map(x -> x.replaceAll("static <T extends Any>", "static<TYPE_T, T extends Any>"))
                .map(x -> x.replaceAll("ObjectChunk<([^>]*)>", "ObjectChunk<TYPE_T, $1>"))
                .collect(Collectors.toList());
        return lines;
    }

    private static void fixupObjectPartition(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        final List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());

        final List<String> complines = fixupChunkAttributes(fixupObjectComparisons(lines));

        FileUtils.writeLines(objectFile, complines);
    }

    private static void fixupDoubleRuns(@NotNull final String doublePath) throws IOException {
        final File doubleFile = new File(doublePath);
        List<String> doubleLines = FileUtils.readLines(doubleFile, Charset.defaultCharset());
        doubleLines = ReplicationUtils.simpleFixup(doubleLines, "neq",
                "next != last",
                "Double.doubleToLongBits(next) != Double.doubleToLongBits(last)");
        FileUtils.writeLines(doubleFile, doubleLines);
    }

    private static void fixupFloatRuns(@NotNull final String floatPath) throws IOException {
        final File floatFile = new File(floatPath);
        List<String> floatLines = FileUtils.readLines(floatFile, Charset.defaultCharset());
        floatLines = simpleFixup(floatLines, "neq",
                "next != last",
                "Float.floatToIntBits(next) != Float.floatToIntBits(last)");
        FileUtils.writeLines(floatFile, floatLines);
    }

    private static void fixupObjectRuns(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());

        lines = fixupObjectNeq(fixupChunkAttributes(addImport(lines, "import java.util.Objects;")));

        FileUtils.writeLines(objectFile, lines);
    }

    private static void fixupLongInt(String path) throws IOException {
        final File file = new File(path);

        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());

        lines = removeImport(lines, "io.deephaven.engine.table.impl.sort.LongSortKernel");

        lines = lines.stream().map(x -> x.replaceAll("LongTimsortKernel", "LongIntTimsortKernel"))
                .map(x -> x.replaceAll("LongSortKernelContext", "LongIntSortKernelContext"))
                .map(x -> x.replaceAll(
                        "static class LongIntSortKernelContext<ATTR extends Any, KEY_INDICES extends Keys> implements SortKernel<ATTR, KEY_INDICES>",
                        "static class LongIntSortKernelContext<ATTR extends Any, KEY_INDICES extends Indices> implements AutoCloseable"))
                .map(x -> x.replaceAll("IntChunk<RowKeys>", "IntChunk"))
                .collect(Collectors.toList());

        lines = applyFixup(lines, "Context", "\\s+@Override", (m) -> Collections.singletonList(""));

        FileUtils.writeLines(new File(path), lines);
    }

    private static void fixupIntInt(String path) throws IOException {
        final File file = new File(path);

        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());

        lines = removeImport(lines, "io.deephaven.engine.table.impl.sort.LongSortKernel");

        lines = lines.stream().map(x -> x.replaceAll("IntTimsortKernel", "IntIntTimsortKernel"))
                .map(x -> x.replaceAll("IntSortKernelContext", "IntIntSortKernelContext"))
                .map(x -> x.replaceAll(
                        "static class IntIntSortKernelContext<ATTR extends Any, KEY_INDICES extends Keys> implements SortKernel<ATTR, KEY_INDICES>",
                        "static class IntIntSortKernelContext<ATTR extends Any, KEY_INDICES extends Indices> implements AutoCloseable"))
                .map(x -> x.replaceAll("IntChunk<RowKeys>", "IntChunk"))
                .collect(Collectors.toList());

        lines = applyFixup(lines, "Context", "\\s+@Override", (m) -> Collections.singletonList(""));

        FileUtils.writeLines(new File(path), lines);
    }


    public static void fixupNanComparisons(String path, boolean ascending) throws IOException {
        final File file = new File(path);

        final List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());

        FileUtils.writeLines(new File(path),
                fixupNanComparisons(lines, path.contains("Double") ? "Double" : "Float", ascending));
    }

    public static List<String> fixupNanComparisons(List<String> lines, String type, boolean ascending) {
        final String lcType = type.toLowerCase();

        lines = ReplicationUtils.addImport(lines, "import io.deephaven.util.compare." + type + "Comparisons;");

        lines = replaceRegion(lines, "comparison functions",
                Arrays.asList("    private static int doComparison(" + lcType + " lhs, " + lcType + " rhs) {",
                        "        return " + (ascending ? "" : "-1 * ") + type + "Comparisons.compare(lhs, rhs);",
                        "    }"));
        return lines;
    }

    @SuppressWarnings("SameParameterValue")
    private static void fixupCharNullComparisons(String sourceClassJavaPath, String path, String newPath,
            String oldName,
            String newName, boolean ascending) throws IOException {
        final File file = new File(path);

        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());

        lines = ReplicationUtils.addImport(lines, QueryConstants.class, CharComparisons.class);

        lines = globalReplacements(fixupCharNullComparisons(lines, ascending), oldName, newName);

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
    }

    public static List<String> fixupCharNullComparisons(List<String> lines, boolean ascending) {
        lines = replaceRegion(lines, "comparison functions",
                Arrays.asList("    private static int doComparison(char lhs, char rhs) {",
                        "        return " + (ascending ? "" : "-1 * ") + "CharComparisons.compare(lhs, rhs);",
                        "    }"));
        return lines;
    }

    public static List<String> fixupObjectComparisons(List<String> lines) {
        return fixupObjectComparisons(lines, true);
    }

    public static List<String> fixupObjectComparisons(List<String> lines, boolean ascending) {
        final List<String> ascendingComparision = Arrays.asList(
                "    // ascending comparison",
                "    private static int doComparison(Object lhs, Object rhs) {",
                "       if (lhs == rhs) {",
                "            return 0;",
                "        }",
                "        if (lhs == null) {",
                "            return -1;",
                "        }",
                "        if (rhs == null) {",
                "            return 1;",
                "        }",
                "        //noinspection unchecked,rawtypes",
                "        return ((Comparable)lhs).compareTo(rhs);",
                "    }",
                "");
        final List<String> descendingComparision = Arrays.asList(
                "    // descending comparison",
                "    private static int doComparison(Object lhs, Object rhs) {",
                "        if (lhs == rhs) {",
                "            return 0;",
                "        }",
                "        if (lhs == null) {",
                "            return 1;",
                "        }",
                "        if (rhs == null) {",
                "            return -1;",
                "        }",
                "        //noinspection unchecked,rawtypes",
                "        return ((Comparable)rhs).compareTo(lhs);",
                "    }");

        return addImport(simpleFixup(
                replaceRegion(lines, "comparison functions", ascending ? ascendingComparision : descendingComparision),
                "equality function", "lhs == rhs", "Objects.equals(lhs, rhs)"), "import java.util.Objects;");
    }

    public static List<String> invertComparisons(List<String> lines) {
        final List<String> descendingComment = Collections.singletonList(
                "    // note that this is a descending kernel, thus the comparisons here are backwards (e.g., the lt function is in terms of the sort direction, so is implemented by gt)");
        return insertRegion(
                applyFixup(lines, "comparison functions", "(\\s+return )(.*compare.*;)",
                        m -> Collections.singletonList(m.group(1) + "-1 * " + m.group(2))),
                "comparison functions", descendingComment);
    }

    private static List<String> fixupObjectNeq(List<String> lines) {
        return applyFixup(lines, "neq", "\\s+return next != last;",
                m -> Collections.singletonList("        return !Objects.equals(next, last);"));
    }

}
