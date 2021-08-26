package io.deephaven.db.v2.sort.timsort;

import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.compilertools.ReplicateUtilities;
import io.deephaven.db.v2.sort.megamerge.TestCharLongMegaMerge;
import io.deephaven.db.v2.sort.partition.CharPartitionKernelBenchmark;
import io.deephaven.db.v2.sort.ReplicateSortKernel;
import io.deephaven.db.v2.sort.permute.TestCharPermuteKernel;
import io.deephaven.db.v2.sources.ObjectArraySource;

import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ReplicateSortKernelTests {
    public static void main(String[] args) throws IOException {
        ReplicateSortKernel.main(args);

        ReplicatePrimitiveCode.charToAllButBoolean(TestCharTimSortKernel.class, ReplicatePrimitiveCode.TEST_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(BaseTestCharTimSortKernel.class, ReplicatePrimitiveCode.TEST_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(CharSortKernelBenchmark.class, ReplicatePrimitiveCode.BENCHMARK_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(CharPartitionKernelBenchmark.class,
                ReplicatePrimitiveCode.BENCHMARK_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(TestCharPermuteKernel.class, ReplicatePrimitiveCode.TEST_SRC);


        ReplicatePrimitiveCode.charToAllButBoolean(TestCharLongMegaMerge.class, ReplicatePrimitiveCode.TEST_SRC);

        final String baseTestPath =
                ReplicatePrimitiveCode.charToObject(BaseTestCharTimSortKernel.class, ReplicatePrimitiveCode.TEST_SRC);
        fixupObject(baseTestPath);
        ReplicatePrimitiveCode.charToObject(TestCharTimSortKernel.class, ReplicatePrimitiveCode.TEST_SRC);
        ReplicatePrimitiveCode.charToObject(CharSortKernelBenchmark.class, ReplicatePrimitiveCode.BENCHMARK_SRC);

        final String objectMegaMergePath =
                ReplicatePrimitiveCode.charToObject(TestCharLongMegaMerge.class, ReplicatePrimitiveCode.TEST_SRC);
        fixupObjectMegaMerge(objectMegaMergePath);
    }

    private static void fixupObject(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());

        final int packageIndex = lines.indexOf("package io.deephaven.db.v2.sort;");

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

        lines = ReplicateUtilities.globalReplacements(lines, "ObjectArraySource\\(\\)",
                "ObjectArraySource(String.class)", "ObjectChunk<Values>", "ObjectChunk<Object, Values>");

        FileUtils.writeLines(objectFile, lines);
    }

    @NotNull
    private static List<String> fixupGetJavaComparator(List<String> lines) {
        return ReplicateUtilities.applyFixup(lines, "getJavaComparator",
                "(.*)Comparator.comparing\\(ObjectLongTuple::getFirstElement\\)(.*)",
                m -> Arrays.asList("        // noinspection unchecked",
                        m.group(1) + "Comparator.comparing(x -> (Comparable)x.getFirstElement())" + m.group(2)));
    }

    @NotNull
    private static List<String> fixupGetJavaMultiComparator(List<String> lines) {
        return ReplicateUtilities.applyFixup(lines, "getJavaMultiComparator",
                "(.*)Comparator.comparing\\(ObjectLongLongTuple::getFirstElement\\).thenComparing\\(ObjectLongLongTuple::getSecondElement\\)(.*)",
                m -> Arrays.asList("        // noinspection unchecked",
                        m.group(1)
                                + "Comparator.comparing(x -> (Comparable)((ObjectLongLongTuple)x).getFirstElement()).thenComparing(x -> ((ObjectLongLongTuple)x).getSecondElement())"
                                + m.group(2)));
    }

    @NotNull
    private static List<String> fixupMergesort(List<String> lines) {
        return ReplicateUtilities.applyFixup(lines, "mergesort", "(.*)Object.compare\\((.*), (.*)\\)\\)(.*)",
                m -> Arrays.asList("            // noinspection unchecked",
                        m.group(1) + "Objects.compare((Comparable)" + m.group(2) + ", (Comparable)" + m.group(3)
                                + ", Comparator.naturalOrder()))" + m.group(4)));
    }

    @NotNull
    private static List<String> fixupTupleColumnSource(List<String> lines) {
        return ReplicateUtilities.replaceRegion(lines, "tuple column source", Arrays.asList(
                "                @Override",
                "                public Object get(long index) {",
                "                    return javaTuples.get(((int)index) / 10).getFirstElement();",
                "                }"));
    }


}
