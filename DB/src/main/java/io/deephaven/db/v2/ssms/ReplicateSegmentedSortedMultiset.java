package io.deephaven.db.v2.ssms;

import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.dbarrays.DbArrayDirect;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.by.ssmcountdistinct.CharSsmBackedSource;
import io.deephaven.db.v2.by.ssmcountdistinct.DbDateTimeSsmSourceWrapper;
import io.deephaven.db.v2.by.ssmcountdistinct.count.CharChunkedCountDistinctOperator;
import io.deephaven.db.v2.by.ssmcountdistinct.count.CharRollupCountDistinctOperator;
import io.deephaven.db.v2.by.ssmcountdistinct.distinct.CharChunkedDistinctOperator;
import io.deephaven.db.v2.by.ssmcountdistinct.distinct.CharRollupDistinctOperator;
import io.deephaven.db.v2.by.ssmcountdistinct.unique.CharChunkedUniqueOperator;
import io.deephaven.db.v2.by.ssmcountdistinct.unique.CharRollupUniqueOperator;
import io.deephaven.db.v2.by.ssmpercentile.CharPercentileTypeHelper;
import io.deephaven.db.v2.by.ssmpercentile.CharPercentileTypeMedianHelper;
import io.deephaven.db.v2.by.ssmpercentile.FloatPercentileTypeMedianHelper;
import io.deephaven.db.v2.sort.ReplicateSortKernel;
import io.deephaven.db.v2.sources.BoxedColumnSource;
import gnu.trove.set.hash.THashSet;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static io.deephaven.compilertools.ReplicateUtilities.*;

//
public class ReplicateSegmentedSortedMultiset {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBooleanAndLong(CharSegmentedSortedMultiset.class,
                ReplicatePrimitiveCode.MAIN_SRC);
        insertDbDateTimeExtensions(
                ReplicatePrimitiveCode.charToLong(CharSegmentedSortedMultiset.class, ReplicatePrimitiveCode.MAIN_SRC));

        String objectSsm =
                ReplicatePrimitiveCode.charToObject(CharSegmentedSortedMultiset.class, ReplicatePrimitiveCode.MAIN_SRC);
        fixupObjectSsm(objectSsm, ReplicateSegmentedSortedMultiset::fixupNulls,
                ReplicateSegmentedSortedMultiset::fixupDbArrays, ReplicateSegmentedSortedMultiset::fixupTHashes,
                ReplicateSegmentedSortedMultiset::fixupSsmConstructor,
                ReplicateSegmentedSortedMultiset::fixupObjectCompare);

        ReplicatePrimitiveCode.charToAllButBoolean(io.deephaven.db.v2.by.ssmminmax.CharSetResult.class,
                ReplicatePrimitiveCode.MAIN_SRC);
        fixupObjectSsm(ReplicatePrimitiveCode.charToObject(io.deephaven.db.v2.by.ssmminmax.CharSetResult.class,
                ReplicatePrimitiveCode.MAIN_SRC), ReplicateSegmentedSortedMultiset::fixupNulls);

        ReplicatePrimitiveCode.charToAllButBoolean(CharPercentileTypeHelper.class, ReplicatePrimitiveCode.MAIN_SRC);
        fixupObjectSsm(
                ReplicatePrimitiveCode.charToObject(CharPercentileTypeHelper.class, ReplicatePrimitiveCode.MAIN_SRC),
                ReplicateSegmentedSortedMultiset::fixupNulls);

        ReplicatePrimitiveCode.charToIntegers(CharPercentileTypeMedianHelper.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(FloatPercentileTypeMedianHelper.class,
                ReplicatePrimitiveCode.MAIN_SRC);

        ReplicatePrimitiveCode.charToAllButBoolean(CharSsmBackedSource.class, ReplicatePrimitiveCode.MAIN_SRC);
        objectSsm = ReplicatePrimitiveCode.charToObject(CharSsmBackedSource.class, ReplicatePrimitiveCode.MAIN_SRC);
        fixupObjectSsm(objectSsm, ReplicateSegmentedSortedMultiset::fixupDbArrays,
                ReplicateSegmentedSortedMultiset::fixupSourceConstructor,
                (l) -> replaceRegion(l, "CreateNew", Collections.singletonList(
                        "            underlying.set(key, ssm = new ObjectSegmentedSortedMultiset(DistinctOperatorFactory.NODE_SIZE, Object.class));")));

        ReplicatePrimitiveCode.charToAllButBoolean(CharChunkedCountDistinctOperator.class,
                ReplicatePrimitiveCode.MAIN_SRC);
        fixupObjectKernelOperator(ReplicatePrimitiveCode.charToObject(CharChunkedCountDistinctOperator.class,
                ReplicatePrimitiveCode.MAIN_SRC), "ssms");

        ReplicatePrimitiveCode.charToAllButBooleanAndLong(CharChunkedDistinctOperator.class,
                ReplicatePrimitiveCode.MAIN_SRC);
        fixupLongKernelOperator(
                ReplicatePrimitiveCode.charToLong(CharChunkedDistinctOperator.class, ReplicatePrimitiveCode.MAIN_SRC),
                "    externalResult = new DbDateTimeSsmSourceWrapper(internalResult);");
        fixupObjectKernelOperator(
                ReplicatePrimitiveCode.charToObject(CharChunkedDistinctOperator.class, ReplicatePrimitiveCode.MAIN_SRC),
                "internalResult");

        ReplicatePrimitiveCode.charToAllButBooleanAndLong(CharChunkedUniqueOperator.class,
                ReplicatePrimitiveCode.MAIN_SRC);
        fixupLongKernelOperator(
                ReplicatePrimitiveCode.charToLong(CharChunkedUniqueOperator.class, ReplicatePrimitiveCode.MAIN_SRC),
                "    externalResult = new BoxedColumnSource.OfDateTime(internalResult);");
        fixupObjectKernelOperator(
                ReplicatePrimitiveCode.charToObject(CharChunkedUniqueOperator.class, ReplicatePrimitiveCode.MAIN_SRC),
                "ssms");

        ReplicatePrimitiveCode.charToAllButBoolean(CharRollupCountDistinctOperator.class,
                ReplicatePrimitiveCode.MAIN_SRC);
        fixupObjectKernelOperator(ReplicatePrimitiveCode.charToObject(CharRollupCountDistinctOperator.class,
                ReplicatePrimitiveCode.MAIN_SRC), "ssms");

        ReplicatePrimitiveCode.charToAllButBooleanAndLong(CharRollupDistinctOperator.class,
                ReplicatePrimitiveCode.MAIN_SRC);
        fixupLongKernelOperator(
                ReplicatePrimitiveCode.charToLong(CharRollupDistinctOperator.class, ReplicatePrimitiveCode.MAIN_SRC),
                "    externalResult = new DbDateTimeSsmSourceWrapper(internalResult);");
        fixupObjectKernelOperator(
                ReplicatePrimitiveCode.charToObject(CharRollupDistinctOperator.class, ReplicatePrimitiveCode.MAIN_SRC),
                "internalResult");

        ReplicatePrimitiveCode.charToAllButBooleanAndLong(CharRollupUniqueOperator.class,
                ReplicatePrimitiveCode.MAIN_SRC);
        fixupLongKernelOperator(
                ReplicatePrimitiveCode.charToLong(CharRollupUniqueOperator.class, ReplicatePrimitiveCode.MAIN_SRC),
                "    externalResult = new BoxedColumnSource.OfDateTime(internalResult);");
        fixupObjectKernelOperator(
                ReplicatePrimitiveCode.charToObject(CharRollupUniqueOperator.class, ReplicatePrimitiveCode.MAIN_SRC),
                "ssms");
    }

    private static void fixupLongKernelOperator(String longPath, String externalResultSetter) throws IOException {
        final File longFile = new File(longPath);
        List<String> lines = FileUtils.readLines(longFile, Charset.defaultCharset());
        lines = addImport(lines, BoxedColumnSource.class, DBDateTime.class, DbDateTimeSsmSourceWrapper.class);
        lines = replaceRegion(lines, "Constructor",
                indent(Collections.singletonList("Class<?> type,"), 12));
        lines = replaceRegion(lines, "ResultAssignment",
                indent(Arrays.asList(
                        "if(type == DBDateTime.class) {",
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

    private static List<String> fixupDbArrays(List<String> lines) {
        lines = removeAnyImports(lines, "\\s*import .*DbArray.+Wrapper;",
                "\\s*import .*Db.+Array;",
                "\\s*import .*Db.+Direct;");

        lines = addImport(lines, DbArray.class, DbArrayDirect.class);

        return globalReplacements(lines, "DbObjectArray>", "DbArray>",
                "DbObjectArray\\s+", "DbArray ",
                "DbObjectArray\\.", "DbArray.",
                "DbObjectArrayDirect", "DbArrayDirect<>",
                "new DbArrayObjectWrapper\\(this\\)", "this");
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
                        "        super(DbArray.class, type);\n" +
                        "        underlying = new ObjectArraySource<>(ObjectSegmentedSortedMultiset.class, type);\n" +
                        "    }"));
    }

    private static List<String> fixupObjectCompare(List<String> lines) {
        lines = removeRegion(lines, "DbArrayEquals");
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

    private static void insertDbDateTimeExtensions(String longPath) throws IOException {
        final File longFile = new File(longPath);
        List<String> lines = FileUtils.readLines(longFile, Charset.defaultCharset());

        lines = addImport(lines, DBDateTime.class, DbArrayDirect.class, DBTimeUtils.class);
        lines = insertRegion(lines, "Extensions",
                Arrays.asList(
                        "    public DBDateTime getAsDate(long i) {",
                        "        return DBTimeUtils.nanosToTime(get(i));",
                        "    }",
                        "",
                        "    public DbArray<DBDateTime> subArrayAsDate(long fromIndexInclusive, long toIndexExclusive) {",
                        "        return new DbArrayDirect<>(keyArrayAsDate(fromIndexInclusive, toIndexExclusive));",
                        "    }",
                        "",
                        "    public DbArray<DBDateTime> subArrayByPositionsAsDates(long[] positions) {",
                        "        final DBDateTime[] keyArray = new DBDateTime[positions.length];",
                        "        int writePos = 0;",
                        "        for (long position : positions) {",
                        "            keyArray[writePos++] = getAsDate(position);",
                        "        }",
                        "",
                        "        return new DbArrayDirect<>(keyArray);",
                        "    }",
                        "",
                        "",
                        "    public DBDateTime[] toDateArray() {",
                        "        return keyArrayAsDate();",
                        "    }",
                        "",
                        "    public DBDateTime getPrevAsDate(long offset) {",
                        "        return DBTimeUtils.nanosToTime(getPrev(offset));",
                        "    }",
                        "",
                        "",
                        "    public Chunk<Attributes.Values> toDateChunk() {",
                        "        return ObjectChunk.chunkWrap(toDateArray());",
                        "    }",
                        "",
                        "    public void fillDateChunk(WritableChunk destChunk) {",
                        "        if(isEmpty()) {",
                        "            return ;",
                        "        }",
                        "",
                        "        //noinspection unchecked",
                        "        WritableObjectChunk<DBDateTime, Attributes.Values> writable = destChunk.asWritableObjectChunk();",
                        "        if (leafCount == 1) {",
                        "            for(int ii = 0; ii < size(); ii++) {",
                        "                writable.set(ii, DBTimeUtils.nanosToTime(directoryValues[ii]));",
                        "            }",
                        "        } else if (leafCount > 0) {",
                        "            int offset = 0;",
                        "            for (int li = 0; li < leafCount; ++li) {",
                        "                for(int jj = 0; jj < leafSizes[li]; jj++) {",
                        "                    writable.set(jj + offset, DBTimeUtils.nanosToTime(leafValues[li][jj]));",
                        "                }",
                        "                offset += leafSizes[li];",
                        "            }",
                        "        }",
                        "    }",
                        "",
                        "",
                        "    public DbArray<DBDateTime> getDirectAsDate() {",
                        "        return new DbArrayDirect<>(keyArrayAsDate());",
                        "    }",
                        "",
                        "    private DBDateTime[] keyArrayAsDate() {",
                        "        return keyArrayAsDate(0, size()-1);",
                        "    }",
                        "",
                        "    /**",
                        "     * Create an array of the current keys beginning with the first (inclusive) and ending with the last (inclusive)",
                        "     * @param first",
                        "     * @param last",
                        "     * @return",
                        "     */",
                        "    private DBDateTime[] keyArrayAsDate(long first, long last) {",
                        "        if(isEmpty()) {",
                        "            return ArrayUtils.EMPTY_DATETIME_ARRAY;",
                        "        }",
                        "",
                        "        final int totalSize = (int)(last - first + 1);",
                        "        final DBDateTime[] keyArray = new DBDateTime[intSize()];",
                        "        if (leafCount == 1) {",
                        "            for(int ii = 0; ii < totalSize; ii++) {",
                        "                keyArray[ii] = DBTimeUtils.nanosToTime(directoryValues[ii + (int)first]);",
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
                        "                            keyArray[jj] = DBTimeUtils.nanosToTime(leafValues[li][jj + toSkip]);",
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
                        "                        keyArray[jj + offset] = DBTimeUtils.nanosToTime(leafValues[li][jj]);",
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
                        "                arrAsString.append(DBTimeUtils.nanosToTime(directoryValues[ii])).append(\", \");",
                        "            }",
                        "            ",
                        "            arrAsString.replace(arrAsString.length() - 2, arrAsString.length(), \"]\");",
                        "            return arrAsString.toString();",
                        "        } else if (leafCount > 0) {",
                        "            for (int li = 0; li < leafCount; ++li) {",
                        "                for(int ai = 0; ai < leafSizes[li]; ai++) {",
                        "                    arrAsString.append(DBTimeUtils.nanosToTime(leafValues[li][ai])).append(\", \");",
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
