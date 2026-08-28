//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.*;

/**
 * Code generation for basic RegionedColumnSource implementations as well as the primary region interfaces for some
 * primitive types.
 */
public class ReplicateRegionsAndRegionedSources {
    private static final String TASK = "replicateRegionsAndRegionedSources";

    private static final String PARQUET_REGION_CHAR_PATH =
            "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/region/ParquetColumnRegionChar.java";

    private static final String GENERIC_REGION_CHAR_PATH =
            "extensions/source-support/src/main/java/io/deephaven/generic/region/AppendOnlyFixedSizePageRegionChar.java";
    private static final String GENERIC_REGION_BINARY_SEARCH_KERNEL_PATH =
            "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/regioned/kernel/CharRegionBinarySearchKernel.java";
    private static final String GENERIC_COLUMN_BINARY_SEARCH_KERNEL_PATH =
            "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/regioned/kernel/CharColumnBinarySearchKernel.java";

    public static void main(String... args) throws IOException {
        // Note that Byte and Object regions are not replicated!
        charToAllButBooleanAndByte(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/regioned/ColumnRegionChar.java");
        charToAllButBooleanAndByte(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/regioned/DeferredColumnRegionChar.java");


        // Note that Object regions are not replicated!
        fixupParquetColumnRegions(charToAllButBooleanAndByte(TASK, PARQUET_REGION_CHAR_PATH));
        fixupChunkColumnRegionByte(charToByte(TASK, PARQUET_REGION_CHAR_PATH));

        charToAllButBoolean(TASK, GENERIC_REGION_BINARY_SEARCH_KERNEL_PATH);
        fixupBinSearchObject(charToObject(TASK, GENERIC_REGION_BINARY_SEARCH_KERNEL_PATH));

        final List<String> columnBinarySearchKernels =
                charToAllButBoolean(TASK, GENERIC_COLUMN_BINARY_SEARCH_KERNEL_PATH);
        for (final String path : columnBinarySearchKernels) {
            if (path.contains("Double")) {
                fixupUnboundedUpperRange(path, "Double");
            } else if (path.contains("Float")) {
                fixupUnboundedUpperRange(path, "Float");
            }
        }
        fixupBinSearchObject(charToObject(TASK, GENERIC_COLUMN_BINARY_SEARCH_KERNEL_PATH));
        charToAllButBooleanAndByte(TASK, GENERIC_REGION_CHAR_PATH);
        fixupChunkColumnRegionByte(charToByte(TASK, GENERIC_REGION_CHAR_PATH));
        fixupChunkColumnRegionObject(charToObject(TASK, GENERIC_REGION_CHAR_PATH));

        final List<String> paths = charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/regioned/RegionedColumnSourceChar.java");
        fixupRegionedColumnSourceLong(paths.stream().filter(p -> p.contains("Long")).findFirst().get());
        fixupRegionedColumnSourceByte(paths.stream().filter(p -> p.contains("Byte")).findFirst().get());
    }

    private static void fixupChunkColumnRegionByte(final String bytePath) throws IOException {
        final File byteFile = new File(bytePath);
        List<String> lines = FileUtils.readLines(byteFile, Charset.defaultCharset());
        lines = addImport(lines,
                "import io.deephaven.chunk.WritableByteChunk;",
                "import io.deephaven.chunk.WritableChunk;",
                "import io.deephaven.engine.rowset.RowSequence;",
                "import io.deephaven.engine.rowset.RowSequenceFactory;");
        lines = replaceRegion(lines, "getBytes", Arrays.asList(
                "    public byte[] getBytes(",
                "            final long firstRowKey,",
                "            @NotNull final byte[] destination,",
                "            final int destinationOffset,",
                "            final int length",
                "    ) {",
                "        final WritableChunk<ATTR> byteChunk = WritableByteChunk.writableChunkWrap(destination, destinationOffset, length);",
                "        try (RowSequence rowSequence = RowSequenceFactory.forRange(firstRowKey, firstRowKey + length - 1)) {",
                "            fillChunk(DEFAULT_FILL_INSTANCE, byteChunk, rowSequence);",
                "        }",
                "        return destination;",
                "    }"));
        FileUtils.writeLines(byteFile, lines);
    }

    private static void fixupChunkColumnRegionObject(final String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "<ATTR extends Any>", "<T, ATTR extends Any>",
                " <ATTR", " <T, ATTR",
                "Object\\[]", "T[]",
                "Object value", "T value",
                "Object getObject\\(", "T getObject(");
        lines = lines.stream().map(x -> x.replaceAll("ObjectChunk<([^,>]+)>", "ObjectChunk<T, $1>"))
                .collect(Collectors.toList());
        lines = lines.stream().map(x -> x.replaceAll("ColumnRegionObject<([^,>]+)>", "ColumnRegionObject<T, $1>"))
                .collect(Collectors.toList());
        lines = lines.stream().map(x -> x.replaceAll("ChunkHolderPageObject<([^,>]+)>", "ChunkHolderPageObject<T, $1>"))
                .collect(Collectors.toList());
        lines = replaceRegion(lines, "allocatePage", Arrays.asList(
                "                    // noinspection unchecked",
                "                    pageHolder = new ChunkHolderPageObject<T, ATTR>(mask(), pageFirstRowInclusive, (T[]) new Object[pageSize]);"));
        FileUtils.writeLines(objectFile, lines);
    }

    private static void fixupRegionedColumnSourceByte(String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = addImport(lines, "import io.deephaven.engine.table.ColumnSource;");
        lines = replaceRegion(lines, "reinterpretation", Arrays.asList(
                "    @Override",
                "    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        return alternateDataType == boolean.class || alternateDataType == Boolean.class || super.allowsReinterpret(alternateDataType);",
                "    }",
                "",
                "    @Override",
                "    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        //noinspection unchecked",
                "        return (ColumnSource<ALTERNATE_DATA_TYPE>) new RegionedColumnSourceBoolean(manager, (RegionedColumnSourceByte<Values>)this);",
                "    }"));

        FileUtils.writeLines(new File(path), lines);
    }

    private static void fixupRegionedColumnSourceLong(String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = addImport(lines,
                "import io.deephaven.engine.table.ColumnSource;",
                "import io.deephaven.engine.table.impl.sources.LongAsLocalDateColumnSource;",
                "import io.deephaven.engine.table.impl.sources.LongAsLocalTimeColumnSource;",
                "import io.deephaven.engine.table.impl.sources.ConvertibleTimeSource;");
        lines = addImport(lines, Instant.class, ZonedDateTime.class, LocalDate.class, LocalTime.class, ZoneId.class);
        lines = globalReplacements(lines, "/\\*\\s+MIXIN_INTERFACES\\s+\\*/", ", ConvertibleTimeSource");
        lines = replaceRegion(lines, "reinterpretation", Arrays.asList(
                "    @Override",
                "    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        if(super.allowsReinterpret(alternateDataType)) {",
                "            return true;",
                "        }",
                "",
                "        return alternateDataType == Instant.class;",
                "    }",
                "",
                "    @SuppressWarnings(\"unchecked\")",
                "    @Override",
                "    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        if(alternateDataType == Instant.class) {",
                "            return (ColumnSource<ALTERNATE_DATA_TYPE>) toInstant();",
                "        }",
                "",
                "        return super.doReinterpret(alternateDataType);",
                "    }",
                "",
                "    @Override",
                "    public boolean supportsTimeConversion() {",
                "        return true;",
                "    }",
                "",
                "    public ColumnSource<Instant> toInstant() {",
                "        //noinspection unchecked",
                "        return new RegionedColumnSourceInstant(manager, (RegionedColumnSourceLong<Values>) this);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<ZonedDateTime> toZonedDateTime(ZoneId zone) {",
                "        //noinspection unchecked",
                "        return new RegionedColumnSourceZonedDateTime(manager, zone, (RegionedColumnSourceLong<Values>) this);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<LocalTime> toLocalTime(ZoneId zone) {",
                "        return new LongAsLocalTimeColumnSource(this, zone);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<LocalDate> toLocalDate(ZoneId zone) {",
                "        return new LongAsLocalDateColumnSource(this, zone);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<Long> toEpochNano() {",
                "        return this;",
                "    }"));

        FileUtils.writeLines(new File(path), lines);
    }

    private static void fixupParquetColumnRegions(List<String> files) throws IOException {
        for (String file : files) {
            if (file.contains("Double")) {
                replaceStatistics(file, "Double");
                fixupUnboundedUpperRange(file, "Double");
            } else if (file.contains("Float")) {
                replaceStatistics(file, "Float");
                fixupUnboundedUpperRange(file, "Float");
            } else if (file.contains("Long")) {
                replaceStatistics(file, "Long");
            }
        }
    }

    /**
     * The range dispatch short-circuits to a lower-bound-only search when the filter's upper bound is the greatest
     * value of the type and is inclusive, since every row at or above the lower bound then matches. That test is
     * written against {@code MAX_<TYPE>}, which is correct for the integral types and char but not for the
     * floating-point ones: {@code MAX_FLOAT} and {@code MAX_DOUBLE} are positive infinity, while Deephaven ordering
     * sorts NaN <i>above</i> positive infinity. Taking the shortcut for an inclusive +Inf upper bound therefore returns
     * the trailing NaN block as part of an exact match, which pushdown never re-filters.
     *
     * <p>
     * The greatest value in Deephaven order for these types is NaN, so test for that instead. This both stops the
     * shortcut being taken for an inclusive +Inf bound (which now falls through to a two-sided search that excludes
     * NaN) and starts taking it for an inclusive NaN bound, where it is genuinely correct.
     */
    private static void fixupUnboundedUpperRange(final String path, final String type) throws IOException {
        final File file = new File(path);
        final String maxConstant = "MAX_" + type.toUpperCase();
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "(\\w+)\\.getUpper\\(\\) == " + maxConstant, type + ".isNaN($1.getUpper())");
        lines = removeImport(lines, "\\s*import\\s+static\\s+io\\.deephaven\\.util\\.QueryConstants\\."
                + maxConstant + "\\s*;");
        lines = explainTwoSidedFloatingPointRange(lines);
        FileUtils.writeLines(file, lines);
    }

    /**
     * Annotates the two-sided fall-through of the range dispatch, which is where the floating-point types land for the
     * greater-than filters that the other types short-circuit past. Only the generated floating-point files get this;
     * for char and the integral types the comment would be meaningless.
     */
    private static List<String> explainTwoSidedFloatingPointRange(final List<String> lines) {
        final List<String> newLines = new ArrayList<>(lines);
        for (int ii = 0; ii < newLines.size() - 1; ++ii) {
            if (!newLines.get(ii).trim().equals("} else {")) {
                continue;
            }
            // Step over any comment the template already carries, so this sits directly above the code.
            int statement = ii + 1;
            while (statement < newLines.size() && newLines.get(statement).trim().startsWith("//")) {
                ++statement;
            }
            // The dispatch's fall-through is the only "} else {" leading directly into a two-sided search.
            if (statement >= newLines.size() || !newLines.get(statement).contains("binarySearchMinMax(")) {
                continue;
            }
            final String body = newLines.get(statement);
            final String indent = body.substring(0, body.length() - body.stripLeading().length());
            newLines.addAll(statement, Arrays.asList(
                    indent + "// gt() and geq() build a NaN upper bound that is exclusive, so the common"
                            + " greater-than filters",
                    indent + "// land here rather than short-circuiting: the trailing NaN block has to be located"
                            + " and excluded,",
                    indent + "// which needs the upper bound searched as well as the lower."));
            return newLines;
        }
        throw new IllegalStateException("Could not find the range dispatch fall-through to annotate");
    }

    private static void replaceStatistics(final String f, final String statsReplacement) throws IOException {
        final File file = new File(f);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = globalReplacements(lines, "IntStatistics", statsReplacement + "Statistics",
                "intValue\\(\\)", statsReplacement.toLowerCase() + "Value()");
        FileUtils.writeLines(new File(f), lines);
    }

    private static void fixupBinSearchObject(String charToObject) throws IOException {
        final File file = new File(charToObject);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = removeImport(lines, "import io\\.deephaven\\.util\\.type\\.ArrayTypeUtils;");
        lines = removeAnyImports(lines,
                "import io\\.deephaven\\.engine\\.table\\.impl\\.select\\.ObjectRangeFilter;",
                "import static io\\.deephaven\\.util\\.QueryConstants\\.NULL_OBJECT;",
                "import static io\\.deephaven\\.util\\.QueryConstants\\.MAX_OBJECT;");
        lines = globalReplacements(lines,
                "ColumnRegionObject<\\?>", "ColumnRegionObject<?, ?>",
                "source\\.getObject\\(", "source.get(",
                "source\\.getPrevObject\\(", "source.getPrev(",
                "final Object\\[\\] unboxed = ArrayTypeUtils.getUnboxedObjectArray\\(searchValues\\);",
                "final Object[] copiedValues = Arrays.copyOf(searchValues, searchValues.length);",
                "unboxed", "copiedValues");
        lines = addImport(lines, "import java.util.Arrays;");
        if (file.getName().contains("Column")) {
            lines = replaceRegion(lines, "binsearchRangeFilter", Arrays.asList(
                    "    /**",
                    "     * Performs a binary search on a sorted {@link ElementSource} using bounds from an"
                            + " {@link AbstractRangeFilter}",
                    "     * (either {@link SingleSidedComparableRangeFilter} or {@link ComparableRangeFilter}),"
                            + " returning the row keys that",
                    "     * satisfy the filter.",
                    "     *",
                    "     * @param source The element source to search.",
                    "     * @param selection The {@link RowSet} defining which rows are populated and the order in which"
                            + " they are searched.",
                    "     * @param sortColumn A {@link SortColumn} representing the sorting order.",
                    "     * @param filter The range filter supplying bounds and their inclusive flags.",
                    "     * @param usePrev If true, uses previous values instead of current values.",
                    "     * @return A {@link RowSet} containing the row keys satisfying the filter.",
                    "     */",
                    "    public static RowSet binsearchRangeFilter(",
                    "            @NotNull final ElementSource<?> source,",
                    "            @NotNull final RowSet selection,",
                    "            @NotNull final SortColumn sortColumn,",
                    "            @NotNull final AbstractRangeFilter filter,",
                    "            final boolean usePrev) {",
                    "        if (filter instanceof SingleSidedComparableRangeFilter) {",
                    "            final SingleSidedComparableRangeFilter rangeFilter = (SingleSidedComparableRangeFilter) filter;",
                    "            if (rangeFilter.isGreaterThan()) {",
                    "                return binarySearchMin(source, selection, sortColumn,",
                    "                        rangeFilter.getPivot(), rangeFilter.isLowerInclusive(), usePrev);",
                    "            } else {",
                    "                return binarySearchMax(source, selection, sortColumn,",
                    "                        rangeFilter.getPivot(), rangeFilter.isUpperInclusive(), usePrev);",
                    "            }",
                    "        }",
                    "        final ComparableRangeFilter rangeFilter = (ComparableRangeFilter) filter;",
                    "        return binarySearchMinMax(source, selection, sortColumn,",
                    "                rangeFilter.getLower(), rangeFilter.getUpper(),",
                    "                rangeFilter.isLowerInclusive(), rangeFilter.isUpperInclusive(), usePrev);",
                    "    }"));
            lines = addImport(lines,
                    "import io.deephaven.engine.table.impl.select.AbstractRangeFilter;",
                    "import io.deephaven.engine.table.impl.select.ComparableRangeFilter;",
                    "import io.deephaven.engine.table.impl.select.SingleSidedComparableRangeFilter;");
        }
        FileUtils.writeLines(new File(charToObject), lines);
    }
}
