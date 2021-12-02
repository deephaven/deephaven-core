package io.deephaven.replicators;

import gnu.trove.set.hash.THashSet;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.*;

//
public class ReplicateSegmentedSortedMultiset {
    public static void main(String[] args) throws IOException {
        charToAllButBooleanAndLong(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssms/CharSegmentedSortedMultiset.java");
        insertDateTimeExtensions(charToLong(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssms/CharSegmentedSortedMultiset.java"));

        String objectSsm = charToObject(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssms/CharSegmentedSortedMultiset.java");
        fixupObjectSsm(objectSsm, ReplicateSegmentedSortedMultiset::fixupNulls,
                ReplicateSegmentedSortedMultiset::fixupTHashes,
                ReplicateSegmentedSortedMultiset::fixupSsmConstructor,
                ReplicateSegmentedSortedMultiset::fixupObjectCompare);

        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmminmax/CharSetResult.java");
        fixupObjectSsm(
                charToObject(
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmminmax/CharSetResult.java"),
                ReplicateSegmentedSortedMultiset::fixupNulls);

        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmpercentile/CharPercentileTypeHelper.java");
        fixupObjectSsm(
                charToObject(
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmpercentile/CharPercentileTypeHelper.java"),
                ReplicateSegmentedSortedMultiset::fixupNulls);

        charToIntegers(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmpercentile/CharPercentileTypeMedianHelper.java");
        floatToAllFloatingPoints(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmpercentile/FloatPercentileTypeMedianHelper.java");

        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/CharSsmBackedSource.java");
        objectSsm = charToObject(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/CharSsmBackedSource.java");
        fixupObjectSsm(objectSsm,
                ReplicateSegmentedSortedMultiset::fixupSourceConstructor,
                (l) -> replaceRegion(l, "CreateNew", Collections.singletonList(
                        "            underlying.set(key, ssm = new ObjectSegmentedSortedMultiset(DistinctOperatorFactory.NODE_SIZE, Object.class));")));

        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/count/CharChunkedCountDistinctOperator.java");
        fixupObjectKernelOperator(
                charToObject(
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/count/CharChunkedCountDistinctOperator.java"),
                "ssms");

        charToAllButBooleanAndLong(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/distinct/CharChunkedDistinctOperator.java");
        fixupLongKernelOperator(
                charToLong(
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/distinct/CharChunkedDistinctOperator.java"),
                "    externalResult = new DateTimeSsmSourceWrapper(internalResult);");
        fixupObjectKernelOperator(
                charToObject(
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/distinct/CharChunkedDistinctOperator.java"),
                "internalResult");

        charToAllButBooleanAndLong(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/unique/CharChunkedUniqueOperator.java");
        fixupLongKernelOperator(
                charToLong(
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/unique/CharChunkedUniqueOperator.java"),
                "    externalResult = new BoxedColumnSource.OfDateTime(internalResult);");
        fixupObjectKernelOperator(
                charToObject(
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/unique/CharChunkedUniqueOperator.java"),
                "ssms");

        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/count/CharRollupCountDistinctOperator.java");
        fixupObjectKernelOperator(charToObject(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/count/CharRollupCountDistinctOperator.java"),
                "ssms");

        charToAllButBooleanAndLong(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/distinct/CharRollupDistinctOperator.java");
        fixupLongKernelOperator(
                charToLong(
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/distinct/CharRollupDistinctOperator.java"),
                "    externalResult = new DateTimeSsmSourceWrapper(internalResult);");
        fixupObjectKernelOperator(
                charToObject(
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/distinct/CharRollupDistinctOperator.java"),
                "internalResult");

        charToAllButBooleanAndLong(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/unique/CharRollupUniqueOperator.java");
        fixupLongKernelOperator(
                charToLong(
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/unique/CharRollupUniqueOperator.java"),
                "    externalResult = new BoxedColumnSource.OfDateTime(internalResult);");
        fixupObjectKernelOperator(
                charToObject(
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/unique/CharRollupUniqueOperator.java"),
                "ssms");
    }

    private static void fixupLongKernelOperator(String longPath, String externalResultSetter) throws IOException {
        final File longFile = new File(longPath);
        List<String> lines = FileUtils.readLines(longFile, Charset.defaultCharset());
        lines = addImport(lines,
                "import io.deephaven.engine.table.impl.sources.BoxedColumnSource;",
                "import io.deephaven.time.DateTime;",
                "import io.deephaven.engine.table.impl.by.ssmcountdistinct.DateTimeSsmSourceWrapper;");
        lines = replaceRegion(lines, "Constructor",
                indent(Collections.singletonList("Class<?> type,"), 12));
        lines = replaceRegion(lines, "ResultAssignment",
                indent(Arrays.asList(
                        "if(type == DateTime.class) {",
                        externalResultSetter,
                        "} else {",
                        "    externalResult = internalResult;",
                        "}"), 8));

        FileUtils.writeLines(longFile, lines);
    }

    private static void fixupObjectKernelOperator(String objectPath, String ssmVarName) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = replaceRegion(lines, "Constructor",
                indent(Collections.singletonList("Class<?> type,"), 12));
        lines = replaceRegion(lines, "SsmCreation",
                indent(Collections.singletonList("this." + ssmVarName + " = new ObjectSsmBackedSource(type);"), 8));
        lines = replaceRegion(lines, "ResultCreation",
                indent(Collections.singletonList("this.internalResult = new ObjectArraySource(type);"), 8));
        lines = globalReplacements(lines, "\\(WritableObjectChunk<\\? extends Values>\\)",
                "(WritableObjectChunk<?, ? extends Values>)");

        FileUtils.writeLines(objectFile, lines);
    }

    private static void fixupObjectSsm(String objectPath, Function<List<String>, List<String>>... mutators)
            throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = fixupChunkAttributes(lines);
        lines = ReplicateSortKernel.fixupObjectComparisons(lines);
        lines = replaceRegion(lines, "averageMedian",
                indent(Collections.singletonList("throw new UnsupportedOperationException();"), 16));

        if (mutators != null) {
            for (int i = 0; i < mutators.length; i++) {
                lines = mutators[i].apply(lines);
            }
        }

        FileUtils.writeLines(objectFile, lines);
    }

    private static List<String> fixupNulls(List<String> lines) {
        lines = globalReplacements(lines, "NULL_OBJECT", "null");
        return removeImport(lines, "\\s*import static.*QueryConstants.*;");
    }

    private static List<String> fixupTHashes(List<String> lines) {
        lines = removeImport(lines, "\\s*import gnu.trove.*;");
        lines = addImport(lines, THashSet.class);
        return globalReplacements(lines, "TObjectHashSet", "THashSet");
    }

    private static List<String> fixupSsmConstructor(List<String> lines) {
        return replaceRegion(lines, "Constructor",
                Collections.singletonList("    private final Class componentType;\n" +
                        "\n" +
                        "    /**\n" +
                        "     * Create a ObjectSegmentedSortedArray with the given leafSize.\n" +
                        "     *\n" +
                        "     * @param leafSize the maximumSize for any leaf\n" +
                        "     * @param componentType the type of the underlying Object\n" +
                        "     */\n" +
                        "    public ObjectSegmentedSortedMultiset(int leafSize, Class<?> componentType) {\n" +
                        "        this.leafSize = leafSize;\n" +
                        "        this.componentType = componentType;\n" +
                        "        leafCount = 0;\n" +
                        "        size = 0;\n" +
                        "    }\n" +
                        "\n" +
                        "    @Override\n" +
                        "    public Class getComponentType() {\n" +
                        "        return componentType;\n" +
                        "    }"));
    }

    private static List<String> fixupSourceConstructor(List<String> lines) {
        return replaceRegion(lines, "Constructor",
                Collections.singletonList("    public ObjectSsmBackedSource(Class type) {\n" +
                        "        super(ObjectVector.class, type);\n" +
                        "        underlying = new ObjectArraySource<>(ObjectSegmentedSortedMultiset.class, type);\n" +
                        "    }"));
    }

    private static List<String> fixupObjectCompare(List<String> lines) {
        lines = removeRegion(lines, "VectorEquals");
        lines = replaceRegion(lines, "EqualsArrayTypeCheck", Collections.singletonList(
                "        if(o.getComponentType() != o.getComponentType()) {\n" +
                        "            return false;\n" +
                        "        }"));
        lines = replaceRegion(lines, "DirObjectEquals",
                Collections.singletonList(
                        "                if(!Objects.equals(directoryValues[ii], that.directoryValues[ii])) {\n" +
                                "                    return false;\n" +
                                "                }"));
        return replaceRegion(lines, "LeafObjectEquals",
                Collections.singletonList(
                        "                if(!Objects.equals(leafValues[li][ai], that.leafValues[otherLeaf][otherLeafIdx++])) {\n"
                                +
                                "                    return false;\n" +
                                "                }"));
    }

    private static void insertDateTimeExtensions(String longPath) throws IOException {
        final File longFile = new File(longPath);
        List<String> lines = FileUtils.readLines(longFile, Charset.defaultCharset());

        lines = addImport(lines,
                "import io.deephaven.time.DateTime;",
                "import io.deephaven.vector.ObjectVectorDirect;",
                "import io.deephaven.time.DateTimeUtils;");
        lines = insertRegion(lines, "Extensions",
                Arrays.asList(
                        "    public DateTime getAsDate(long i) {",
                        "        return DateTimeUtils.nanosToTime(get(i));",
                        "    }",
                        "",
                        "    public ObjectVector<DateTime> subArrayAsDate(long fromIndexInclusive, long toIndexExclusive) {",
                        "        return new ObjectVectorDirect<>(keyArrayAsDate(fromIndexInclusive, toIndexExclusive));",
                        "    }",
                        "",
                        "    public ObjectVector<DateTime> subArrayByPositionsAsDates(long[] positions) {",
                        "        final DateTime[] keyArray = new DateTime[positions.length];",
                        "        int writePos = 0;",
                        "        for (long position : positions) {",
                        "            keyArray[writePos++] = getAsDate(position);",
                        "        }",
                        "",
                        "        return new ObjectVectorDirect<>(keyArray);",
                        "    }",
                        "",
                        "    public DateTime[] toDateArray() {",
                        "        return keyArrayAsDate();",
                        "    }",
                        "",
                        "    public Chunk<Values> toDateChunk() {",
                        "        return ObjectChunk.chunkWrap(toDateArray());",
                        "    }",
                        "",
                        "    public void fillDateChunk(WritableChunk destChunk) {",
                        "        if(isEmpty()) {",
                        "            return ;",
                        "        }",
                        "",
                        "        //noinspection unchecked",
                        "        WritableObjectChunk<DateTime, Values> writable = destChunk.asWritableObjectChunk();",
                        "        if (leafCount == 1) {",
                        "            for(int ii = 0; ii < size(); ii++) {",
                        "                writable.set(ii, DateTimeUtils.nanosToTime(directoryValues[ii]));",
                        "            }",
                        "        } else if (leafCount > 0) {",
                        "            int offset = 0;",
                        "            for (int li = 0; li < leafCount; ++li) {",
                        "                for(int jj = 0; jj < leafSizes[li]; jj++) {",
                        "                    writable.set(jj + offset, DateTimeUtils.nanosToTime(leafValues[li][jj]));",
                        "                }",
                        "                offset += leafSizes[li];",
                        "            }",
                        "        }",
                        "    }",
                        "",
                        "",
                        "    public ObjectVector<DateTime> getDirectAsDate() {",
                        "        return new ObjectVectorDirect<>(keyArrayAsDate());",
                        "    }",
                        "",
                        "    private DateTime[] keyArrayAsDate() {",
                        "        return keyArrayAsDate(0, size()-1);",
                        "    }",
                        "",
                        "    /**",
                        "     * Create an array of the current keys beginning with the first (inclusive) and ending with the last (inclusive)",
                        "     * @param first",
                        "     * @param last",
                        "     * @return",
                        "     */",
                        "    private DateTime[] keyArrayAsDate(long first, long last) {",
                        "        if(isEmpty()) {",
                        "            return DateTimeUtils.ZERO_LENGTH_DATETIME_ARRAY;",
                        "        }",
                        "",
                        "        final int totalSize = (int)(last - first + 1);",
                        "        final DateTime[] keyArray = new DateTime[intSize()];",
                        "        if (leafCount == 1) {",
                        "            for(int ii = 0; ii < totalSize; ii++) {",
                        "                keyArray[ii] = DateTimeUtils.nanosToTime(directoryValues[ii + (int)first]);",
                        "            }",
                        "        } else if (leafCount > 0) {",
                        "            int offset = 0;",
                        "            int copied = 0;",
                        "            int skipped = 0;",
                        "            for (int li = 0; li < leafCount; ++li) {",
                        "                if(skipped < first) {",
                        "                    final int toSkip = (int)first - skipped;",
                        "                    if(toSkip < leafSizes[li]) {",
                        "                        final int nToCopy = Math.min(leafSizes[li] - toSkip, totalSize);",
                        "                        for(int jj = 0; jj < nToCopy; jj++) {",
                        "                            keyArray[jj] = DateTimeUtils.nanosToTime(leafValues[li][jj + toSkip]);",
                        "                        }",
                        "                        copied = nToCopy;",
                        "                        offset = copied;",
                        "                        skipped = (int)first;",
                        "                    } else {",
                        "                        skipped += leafSizes[li];",
                        "                    }",
                        "                } else {",
                        "                    int nToCopy = Math.min(leafSizes[li], totalSize - copied);",
                        "                    for(int jj = 0; jj < nToCopy; jj++) {",
                        "                        keyArray[jj + offset] = DateTimeUtils.nanosToTime(leafValues[li][jj]);",
                        "                    }",
                        "                    offset += leafSizes[li];",
                        "                    copied += nToCopy;",
                        "                }",
                        "            }",
                        "        }",
                        "        return keyArray;",
                        "    }",
                        "",
                        "    public String toDateString() {",
                        "        final StringBuilder arrAsString = new StringBuilder(\"[\");",
                        "        if (leafCount == 1) {",
                        "            for(int ii = 0; ii < intSize(); ii++) {",
                        "                arrAsString.append(DateTimeUtils.nanosToTime(directoryValues[ii])).append(\", \");",
                        "            }",
                        "            ",
                        "            arrAsString.replace(arrAsString.length() - 2, arrAsString.length(), \"]\");",
                        "            return arrAsString.toString();",
                        "        } else if (leafCount > 0) {",
                        "            for (int li = 0; li < leafCount; ++li) {",
                        "                for(int ai = 0; ai < leafSizes[li]; ai++) {",
                        "                    arrAsString.append(DateTimeUtils.nanosToTime(leafValues[li][ai])).append(\", \");",
                        "                }",
                        "            }",
                        "",
                        "            arrAsString.replace(arrAsString.length() - 2, arrAsString.length(), \"]\");",
                        "            return arrAsString.toString();",
                        "        }",
                        "",
                        "        return \"[]\";",
                        "    }"));

        FileUtils.writeLines(longFile, lines);
    }
}
