//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
                "        return (ColumnSource<ALTERNATE_DATA_TYPE>) new RegionedColumnSourceBoolean((RegionedColumnSourceByte<Values>)this);",
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
                "        return new RegionedColumnSourceInstant((RegionedColumnSourceLong<Values>) this);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<ZonedDateTime> toZonedDateTime(ZoneId zone) {",
                "        //noinspection unchecked",
                "        return new RegionedColumnSourceZonedDateTime(zone, (RegionedColumnSourceLong<Values>) this);",
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
            } else if (file.contains("Float")) {
                replaceStatistics(file, "Float");
            } else if (file.contains("Long")) {
                replaceStatistics(file, "Long");
            }
        }
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
        lines = globalReplacements(lines,
                "<\\?>", "<?, ?>",
                "final Object\\[\\] unboxed = ArrayTypeUtils.getUnboxedObjectArray\\(searchValues\\);", "",
                "unboxed", "searchValues");
        FileUtils.writeLines(new File(charToObject), lines);
    }
}
