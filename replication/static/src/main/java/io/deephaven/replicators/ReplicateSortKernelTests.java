package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBoolean;
import static io.deephaven.replication.ReplicatePrimitiveCode.charToObject;

public class ReplicateSortKernelTests {
    public static void main(String[] args) throws IOException {
        ReplicateSortKernel.main(args);

        charToAllButBoolean(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sort/timsort/TestCharTimSortKernel.java");
        charToAllButBoolean(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sort/timsort/BaseTestCharTimSortKernel.java");
        charToAllButBoolean(
                "engine/benchmark/src/benchmark/java/io/deephaven/benchmark/engine/sort/timsort/CharSortKernelBenchmark.java");
        charToAllButBoolean(
                "engine/benchmark/src/benchmark/java/io/deephaven/benchmark/engine/partition/CharPartitionKernelBenchmark.java");
        charToAllButBoolean(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sort/permute/TestCharPermuteKernel.java");

        charToAllButBoolean(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sort/megamerge/TestCharLongMegaMerge.java");

        final String baseTestPath =
                charToObject(
                        "engine/table/src/test/java/io/deephaven/engine/table/impl/sort/timsort/BaseTestCharTimSortKernel.java");
        fixupObject(baseTestPath);
        charToObject(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sort/timsort/TestCharTimSortKernel.java");
        charToObject(
                "engine/benchmark/src/benchmark/java/io/deephaven/benchmark/engine/sort/timsort/CharSortKernelBenchmark.java");

        final String objectMegaMergePath = charToObject(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sort/megamerge/TestCharLongMegaMerge.java");
        fixupObjectMegaMerge(objectMegaMergePath);
    }

    private static void fixupObject(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());

        final int packageIndex = lines.indexOf("package io.deephaven.engine.table.impl.sort;");

        lines.add(packageIndex + 2, "import java.util.Objects;");

        lines = lines.stream().map(x -> x.replaceAll("ObjectChunk<Any>", "ObjectChunk<Object, Any>"))
                .collect(Collectors.toList());

        lines = fixupTupleColumnSource(ReplicateSortKernel
                .fixupObjectComparisons(fixupMergesort(fixupGetJavaMultiComparator(fixupGetJavaComparator(lines)))));

        FileUtils.writeLines(objectFile, lines);
    }

    private static void fixupObjectMegaMerge(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());

        lines = ReplicationUtils.globalReplacements(lines, "ObjectArraySource\\(\\)",
                "ObjectArraySource(String.class)", "ObjectChunk<Values>", "ObjectChunk<Object, Values>");

        FileUtils.writeLines(objectFile, lines);
    }

    @NotNull
    private static List<String> fixupGetJavaComparator(List<String> lines) {
        return ReplicationUtils.applyFixup(lines, "getJavaComparator",
                "(.*)Comparator.comparing\\(ObjectLongTuple::getFirstElement\\)(.*)",
                m -> Arrays.asList("        // noinspection unchecked",
                        m.group(1) + "Comparator.comparing(x -> (Comparable)x.getFirstElement())" + m.group(2)));
    }

    @NotNull
    private static List<String> fixupGetJavaMultiComparator(List<String> lines) {
        return ReplicationUtils.applyFixup(lines, "getJavaMultiComparator",
                "(.*)Comparator.comparing\\(ObjectLongLongTuple::getFirstElement\\).thenComparing\\(ObjectLongLongTuple::getSecondElement\\)(.*)",
                m -> Arrays.asList("        // noinspection unchecked",
                        m.group(1)
                                + "Comparator.comparing(x -> (Comparable)((ObjectLongLongTuple)x).getFirstElement()).thenComparing(x -> ((ObjectLongLongTuple)x).getSecondElement())"
                                + m.group(2)));
    }

    @NotNull
    private static List<String> fixupMergesort(List<String> lines) {
        return ReplicationUtils.applyFixup(lines, "mergesort", "(.*)Object.compare\\((.*), (.*)\\)\\)(.*)",
                m -> Arrays.asList("            // noinspection unchecked",
                        m.group(1) + "Objects.compare((Comparable)" + m.group(2) + ", (Comparable)" + m.group(3)
                                + ", Comparator.naturalOrder()))" + m.group(4)));
    }

    @NotNull
    private static List<String> fixupTupleColumnSource(List<String> lines) {
        return ReplicationUtils.replaceRegion(lines, "tuple column source", Arrays.asList(
                "                @Override",
                "                public Object get(long rowSet) {",
                "                    return javaTuples.get(((int)rowSet) / 10).getFirstElement();",
                "                }"));
    }


}
