//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import it.unimi.dsi.fastutil.doubles.DoubleOpenCustomHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenCustomHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateSegmentedSortedMultiset {
    private static final String TASK = "replicateSegmentedSortedMultiset";

    public static void main(String[] args) throws IOException {
        // Replicate FloatCompareOpenHashSet -> DoubleCompareOpenHashSet
        floatToAllFloatingPoints(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssms/FloatCompareOpenHashSet.java");
        final File doubleCompareHashSetFile = new File(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssms/DoubleCompareOpenHashSet.java");
        List<String> doubleCompareLines = FileUtils.readLines(doubleCompareHashSetFile, Charset.defaultCharset());
        doubleCompareLines = globalReplacements(doubleCompareLines,
                "0\\.0f", "0.0d",
                "0x7fc00000", "0x7ff8000000000000L",
                // DoubleOpenHashSet's load-factor parameter is float, not double -- keep it float
                "final int expected, final double f\\)", "final int expected, final float f)");
        FileUtils.writeLines(doubleCompareHashSetFile, doubleCompareLines);

        final List<String> generatedSsms = charToAllButBooleanAndLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssms/CharSegmentedSortedMultiset.java");

        // Float/Double SSMs must use FloatComparisons / DoubleComparisons equality in their delta-tracking hash
        // sets — otherwise -0.0 vs +0.0 (and any two NaN bit patterns) would be treated as distinct values, which
        // disagrees with the SSM's own leaf-storage equality.
        for (final String generated : generatedSsms) {
            if (generated.contains("Float")) {
                useCompareOpenHashSet(generated, "Float");
            } else if (generated.contains("Double")) {
                useCompareOpenHashSet(generated, "Double");
            }
        }

        insertInstantExtensions(charToLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssms/CharSegmentedSortedMultiset.java"));

        String objectSsm = charToObject(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/ssms/CharSegmentedSortedMultiset.java");
        fixupObjectSsm(objectSsm, ReplicateSegmentedSortedMultiset::fixupNulls,
                ReplicateSegmentedSortedMultiset::fixupObjectGeneric,
                ReplicateSegmentedSortedMultiset::fixupObjectHashes,
                ReplicateSegmentedSortedMultiset::fixupSsmConstructor,
                ReplicateSegmentedSortedMultiset::fixupObjectCompare,
                ReplicateSegmentedSortedMultiset::fixupKeyArrayAllocation);

        final List<String> files = charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmminmax/CharSetResult.java");
        for (String file : files) {
            if (file.contains("Float")) {
                final File updatedFile = new File(file);
                List<String> lines = FileUtils.readLines(updatedFile, Charset.defaultCharset());
                lines = ReplicationUtils.replaceRegion(lines, "nan handling", List.of("" +
                        "            if (minimum) {\n" +
                        "                newResult = Float.isNaN(floatSsm.getMaxFloat()) ? Float.NaN : floatSsm.getMinFloat();\n"
                        +
                        "            } else {\n" +
                        "                newResult = floatSsm.getMaxFloat(); // NaN sorts to max\n" +
                        "            }"));
                FileUtils.writeLines(updatedFile, lines);
            } else if (file.contains("Double")) {
                final File updatedFile = new File(file);
                List<String> lines = FileUtils.readLines(updatedFile, Charset.defaultCharset());
                lines = ReplicationUtils.replaceRegion(lines, "nan handling", List.of("" +
                        "            if (minimum) {\n" +
                        "                newResult = Double.isNaN(doubleSsm.getMaxDouble()) ? Double.NaN : doubleSsm.getMinDouble();\n"
                        +
                        "            } else {\n" +
                        "                newResult = doubleSsm.getMaxDouble(); // NaN sorts to max\n" +
                        "            }"));
                FileUtils.writeLines(updatedFile, lines);
            }
        }
        fixupObjectSsm(
                charToObject(TASK,
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmminmax/CharSetResult.java"),
                ReplicateSegmentedSortedMultiset::fixupNulls);

        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmpercentile/CharPercentileTypeHelper.java");
        updateFloatPercentileHelper(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmpercentile/FloatPercentileTypeHelper.java");
        updateDoublePercentileHelper(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmpercentile/DoublePercentileTypeHelper.java");
        fixupObjectSsm(
                charToObject(TASK,
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmpercentile/CharPercentileTypeHelper.java"),
                ReplicateSegmentedSortedMultiset::fixupNulls);

        charToIntegers(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmpercentile/CharPercentileTypeMedianHelper.java");
        floatToAllFloatingPoints(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmpercentile/FloatPercentileTypeMedianHelper.java");
        updateDoublePercentileHelper(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmpercentile/DoublePercentileTypeMedianHelper.java");

        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/CharSsmBackedSource.java");
        objectSsm = charToObject(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/CharSsmBackedSource.java");
        fixupObjectSsm(objectSsm,
                ReplicateSegmentedSortedMultiset::fixupSourceConstructor,
                (l) -> replaceRegion(l, "CreateNew", Collections.singletonList(
                        "            underlying.set(key, ssm = new ObjectSegmentedSortedMultiset(SsmDistinctContext.NODE_SIZE, componentType));")));

        final String compactModificationsPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/compactmodifications/CharCompactModifications.java";
        final List<String> compactModifications = charToAllButBoolean(TASK, compactModificationsPath);
        for (final String compactModification : compactModifications) {
            if (compactModification.contains("Float")) {
                fixupFloatCompactModifications(compactModification, "Float");
            } else if (compactModification.contains("Double")) {
                fixupFloatCompactModifications(compactModification, "Double");
            }
        }
        fixupObjectCompactModifications(charToObject(TASK, compactModificationsPath));

        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/count/CharChunkedCountDistinctOperator.java");
        fixupObjectKernelOperator(
                charToObject(TASK,
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/count/CharChunkedCountDistinctOperator.java"),
                "ssms");

        charToAllButBooleanAndLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/distinct/CharChunkedDistinctOperator.java");
        fixupLongKernelOperator(
                charToLong(TASK,
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/distinct/CharChunkedDistinctOperator.java"),
                "    externalResult = new InstantSsmSourceWrapper(internalResult);");
        fixupObjectKernelOperator(
                charToObject(TASK,
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/distinct/CharChunkedDistinctOperator.java"),
                "internalResult");

        charToAllButBooleanAndLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/unique/CharChunkedUniqueOperator.java");
        fixupLongKernelOperator(
                charToLong(TASK,
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/unique/CharChunkedUniqueOperator.java"),
                "    externalResult = new LongAsInstantColumnSource(internalResult);");
        fixupObjectKernelOperator(
                charToObject(TASK,
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/unique/CharChunkedUniqueOperator.java"),
                "ssms");

        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/count/CharRollupCountDistinctOperator.java");
        fixupObjectKernelOperator(charToObject(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/count/CharRollupCountDistinctOperator.java"),
                "ssms");

        charToAllButBooleanAndLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/distinct/CharRollupDistinctOperator.java");
        fixupLongKernelOperator(
                charToLong(TASK,
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/distinct/CharRollupDistinctOperator.java"),
                "    externalResult = new InstantSsmSourceWrapper(internalResult);");
        fixupObjectKernelOperator(
                charToObject(TASK,
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/distinct/CharRollupDistinctOperator.java"),
                "internalResult");

        charToAllButBooleanAndLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/unique/CharRollupUniqueOperator.java");
        fixupLongKernelOperator(
                charToLong(TASK,
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/unique/CharRollupUniqueOperator.java"),
                "    externalResult = new LongAsInstantColumnSource(internalResult);");
        fixupObjectKernelOperator(
                charToObject(TASK,
                        "engine/table/src/main/java/io/deephaven/engine/table/impl/by/ssmcountdistinct/unique/CharRollupUniqueOperator.java"),
                "ssms");
    }

    private static void updateFloatPercentileHelper(String file) throws IOException {
        final File updatedFile = new File(file);
        List<String> lines = FileUtils.readLines(updatedFile, Charset.defaultCharset());
        lines = ReplicationUtils.replaceRegion(lines, "maybeHandleNaN", List.of("" +
                "            final FloatSegmentedSortedMultiset floatSsmLo = (FloatSegmentedSortedMultiset) ssmLo;\n" +
                "            final FloatSegmentedSortedMultiset floatSsmHi = (FloatSegmentedSortedMultiset) ssmHi;\n" +
                "            if ((hiSize > 0 && Float.isNaN(floatSsmHi.getMax())) || (loSize > 0 && Float.isNaN(floatSsmLo.getMax()))) {\n"
                +
                "                // No need to pivot while we have NaN values present\n" +
                "                return setResult(destination, Float.NaN);\n" +
                "            }"));
        FileUtils.writeLines(updatedFile, lines);
    }

    private static void updateDoublePercentileHelper(String file) throws IOException {
        final File updatedFile = new File(file);
        List<String> lines = FileUtils.readLines(updatedFile, Charset.defaultCharset());
        lines = ReplicationUtils.replaceRegion(lines, "maybeHandleNaN", List.of("" +
                "            final DoubleSegmentedSortedMultiset doubleSsmLo = (DoubleSegmentedSortedMultiset) ssmLo;\n"
                +
                "            final DoubleSegmentedSortedMultiset doubleSsmHi = (DoubleSegmentedSortedMultiset) ssmHi;\n"
                +
                "            if ((hiSize > 0 && Double.isNaN(doubleSsmHi.getMax())) || (loSize > 0 && Double.isNaN(doubleSsmLo.getMax()))) {\n"
                +
                "                // No need to pivot while we have NaN values present\n" +
                "                return setResult(destination, Double.NaN);\n" +
                "            }"));
        FileUtils.writeLines(updatedFile, lines);
    }

    private static void fixupLongKernelOperator(String longPath, String externalResultSetter) throws IOException {
        final File longFile = new File(longPath);
        List<String> lines = FileUtils.readLines(longFile, Charset.defaultCharset());
        lines = addImport(lines,
                "import io.deephaven.engine.table.impl.sources.LongAsInstantColumnSource;",
                "import io.deephaven.engine.table.impl.by.ssmcountdistinct.InstantSsmSourceWrapper;");
        lines = addImport(lines, Instant.class);
        lines = replaceRegion(lines, "Constructor",
                indent(Collections.singletonList("Class<?> type,"), 12));
        lines = replaceRegion(lines, "ResultAssignment",
                indent(Arrays.asList(
                        "if(type == Instant.class) {",
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
                "(WritableObjectChunk<Object, ? extends Values>)");
        // give the typed chunk locals (e.g. the cast-once valueCopy) the two-argument WritableObjectChunk form
        lines = fixupChunkAttributes(lines);

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

    private static List<String> fixupKeyArrayAllocation(List<String> lines) {
        lines = replaceRegion(lines, "KeyArrayAllocation",
                indent(Collections.singletonList(
                        "final Object[] keyArray = (Object[]) Array.newInstance(getComponentType(), totalSize);"), 8));
        lines = replaceRegion(lines, "EmptyKeyArrayAllocation",
                indent(Collections.singletonList(
                        "return (Object[]) Array.newInstance(getComponentType(), 0);"), 12));
        return addImport(lines, Array.class);
    }

    /**
     * Swap the FloatOpenHashSet / DoubleOpenHashSet used by the delta-tracking hash sets in the generated Float and
     * Double SSMs for our {@code }io.deephaven.engine.table.impl.ssms.FloatCompareOpenHashSet} /
     * {@code io.deephaven.engine.table.impl.ssms.DoubleCompareOpenHashSet}, so signed-zero and NaN comparisons in the
     * delta tracker line up with the SSM's leaf-storage equality (FloatComparisons / DoubleComparisons).
     */
    private static void useCompareOpenHashSet(final String generatedPath, final String typeName) throws IOException {
        final File file = new File(generatedPath);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "new " + typeName + "OpenHashSet\\(", "new " + typeName + "CompareOpenHashSet(",
                typeName + "Set added", typeName + "CompareOpenHashSet added",
                typeName + "Set removed", typeName + "CompareOpenHashSet removed");
        lines = removeImport(lines, typeName.equals("Float") ? FloatOpenHashSet.class : DoubleOpenHashSet.class);
        FileUtils.writeLines(file, lines);
    }

    private static List<String> fixupNulls(List<String> lines) {
        lines = globalReplacements(lines, "NULL_OBJECT", "null");
        return removeImport(lines, "\\s*import static.*QueryConstants.*;");
    }

    private static void fixupFloatCompactModifications(String path, String typeOfFloat) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = replaceRegion(lines, "maybeIgnoreNaN", Collections.singletonList("" +
                "        if (!countNaN && " + typeOfFloat + ".isNaN(value)) {\n" +
                "            return true;\n" +
                "        }"));
        FileUtils.writeLines(file, lines);
    }

    private static void fixupObjectCompactModifications(String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = fixupChunkAttributes(lines, "T");
        lines = globalReplacements(lines,
                "public static void compactAndCountModifications",
                "public static <T> void compactAndCountModifications",
                "private static int countRun", "private static <T> int countRun",
                "final Object removedValue", "final T removedValue",
                "final Object addedValue", "final T addedValue",
                "final Object value", "final T value");
        lines = fixupNulls(lines);
        FileUtils.writeLines(file, lines);
    }

    private static List<String> fixupObjectHashes(List<String> lines) {
        // charToObject capitalizes the leading C in the package "chars" to "Objects"; fix back to "objects".
        // Also fixes the leaky toCharArray API that doesn't exist on ObjectCollection.
        return globalReplacements(lines,
                "it\\.unimi\\.dsi\\.fastutil\\.Objects\\.", "it.unimi.dsi.fastutil.objects.",
                "ObjectSet added", "ObjectSet<Object> added",
                "ObjectSet removed", "ObjectSet<Object> removed",
                "new ObjectOpenHashSet\\(", "new ObjectOpenHashSet<>(",
                // ObjectCollection.toArray() returns Object[] already; the typed toCharArray() rename doesn't exist.
                "\\.toObjectArray\\(", ".toArray(");
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

    private static List<String> fixupObjectGeneric(List<String> lines) {
        return globalReplacements(lines, "ObjectVector \\{", "ObjectVector<Object> \\{");
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
        // the primitive iterator is only used by the (now removed) primitive-vector equalsArray overload
        lines = removeImport(lines, "\\s*import .*CloseablePrimitiveIteratorOfObject;");
        lines = replaceRegion(lines, "EqualsArrayTypeCheck", Collections.singletonList(
                "        if(getComponentType() != o.getComponentType()) {\n" +
                        "            return false;\n" +
                        "        }"));
        lines = replaceRegion(lines, "DirObjectEquals",
                Collections.singletonList(
                        "                if(!Objects.equals(directoryValues[ii], that.directoryValues[ii])) {\n" +
                                "                    return false;\n" +
                                "                }"));
        lines = replaceRegion(lines, "SingletonEquals",
                Collections.singletonList(
                        "            return Objects.equals(get(0), that.get(0));"));
        return replaceRegion(lines, "LeafObjectEquals",
                Collections.singletonList(
                        "                if(!Objects.equals(leafValues[li][ai], that.leafValues[otherLeaf][otherLeafIdx++])) {\n"
                                +
                                "                    return false;\n" +
                                "                }"));
    }

    private static void insertInstantExtensions(String longPath) throws IOException {
        final File longFile = new File(longPath);
        List<String> lines = FileUtils.readLines(longFile, Charset.defaultCharset());

        lines = addImport(lines,
                "import io.deephaven.vector.ObjectVectorDirect;",
                "import io.deephaven.time.DateTimeUtils;");
        lines = addImport(lines, Instant.class);
        lines = insertRegion(lines, "Extensions",
                Arrays.asList(
                        "    public Instant getAsInstant(long i) {",
                        "        return DateTimeUtils.epochNanosToInstant(get(i));",
                        "    }",
                        "",
                        "    public ObjectVector<Instant> subArrayAsInstants(long fromIndexInclusive, long toIndexExclusive) {",
                        "        return new ObjectVectorDirect<>(keyArrayAsInstants(fromIndexInclusive, toIndexExclusive));",
                        "    }",
                        "",
                        "    public ObjectVector<Instant> subArrayByPositionsAsInstants(long[] positions) {",
                        "        final Instant[] keyArray = new Instant[positions.length];",
                        "        int writePos = 0;",
                        "        for (long position : positions) {",
                        "            keyArray[writePos++] = getAsInstant(position);",
                        "        }",
                        "",
                        "        return new ObjectVectorDirect<>(keyArray);",
                        "    }",
                        "",
                        "    public Instant[] toInstantArray() {",
                        "        return keyArrayAsInstants();",
                        "    }",
                        "",
                        "    public Chunk<Values> toInstantChunk() {",
                        "        return ObjectChunk.chunkWrap(toInstantArray());",
                        "    }",
                        "",
                        "    public void fillInstantChunk(WritableChunk destChunk) {",
                        "        if(isEmpty()) {",
                        "            return ;",
                        "        }",
                        "",
                        "        //noinspection unchecked",
                        "        WritableObjectChunk<Instant, Values> writable = destChunk.asWritableObjectChunk();",
                        "        if (leafCount == 1) {",
                        "            for(int ii = 0; ii < size(); ii++) {",
                        "                writable.set(ii, DateTimeUtils.epochNanosToInstant(directoryValues == null ? singletonValue : directoryValues[ii]));",
                        "            }",
                        "        } else if (leafCount > 0) {",
                        "            int offset = 0;",
                        "            for (int li = 0; li < leafCount; ++li) {",
                        "                for(int jj = 0; jj < leafSizes[li]; jj++) {",
                        "                    writable.set(jj + offset, DateTimeUtils.epochNanosToInstant(leafValues[li][jj]));",
                        "                }",
                        "                offset += leafSizes[li];",
                        "            }",
                        "        }",
                        "    }",
                        "",
                        "",
                        "    public ObjectVector<Instant> getDirectAsInstants() {",
                        "        return new ObjectVectorDirect<>(keyArrayAsInstants());",
                        "    }",
                        "",
                        "    private Instant[] keyArrayAsInstants() {",
                        "        return keyArrayAsInstants(0, size()-1);",
                        "    }",
                        "",
                        "    /**",
                        "     * Create an array of the current keys beginning with the first (inclusive) and ending with the last (inclusive)",
                        "     * @param first",
                        "     * @param last",
                        "     * @return",
                        "     */",
                        "    private Instant[] keyArrayAsInstants(long first, long last) {",
                        "        if(isEmpty()) {",
                        "            return DateTimeUtils.ZERO_LENGTH_INSTANT_ARRAY;",
                        "        }",
                        "",
                        "        final int totalSize = (int)(last - first + 1);",
                        "        final Instant[] keyArray = new Instant[intSize()];",
                        "        if (leafCount == 1) {",
                        "            for(int ii = 0; ii < totalSize; ii++) {",
                        "                keyArray[ii] = DateTimeUtils.epochNanosToInstant(directoryValues == null ? singletonValue : directoryValues[ii + (int)first]);",
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
                        "                            keyArray[jj] = DateTimeUtils.epochNanosToInstant(leafValues[li][jj + toSkip]);",
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
                        "                        keyArray[jj + offset] = DateTimeUtils.epochNanosToInstant(leafValues[li][jj]);",
                        "                    }",
                        "                    offset += leafSizes[li];",
                        "                    copied += nToCopy;",
                        "                }",
                        "            }",
                        "        }",
                        "        return keyArray;",
                        "    }",
                        "",
                        "    public String toInstantString() {",
                        "        final StringBuilder arrAsString = new StringBuilder(\"[\");",
                        "        if (leafCount == 1) {",
                        "            for(int ii = 0; ii < intSize(); ii++) {",
                        "                arrAsString.append(DateTimeUtils.epochNanosToInstant(directoryValues == null ? singletonValue : directoryValues[ii])).append(\", \");",
                        "            }",
                        "            ",
                        "            arrAsString.replace(arrAsString.length() - 2, arrAsString.length(), \"]\");",
                        "            return arrAsString.toString();",
                        "        } else if (leafCount > 0) {",
                        "            for (int li = 0; li < leafCount; ++li) {",
                        "                for(int ai = 0; ai < leafSizes[li]; ai++) {",
                        "                    arrAsString.append(DateTimeUtils.epochNanosToInstant(leafValues[li][ai])).append(\", \");",
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
