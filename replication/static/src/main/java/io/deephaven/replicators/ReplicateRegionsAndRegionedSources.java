/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.base.verify.Require;
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
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.*;

/**
 * Code generation for basic RegionedColumnSource implementations as well as well as the primary region interfaces for
 * some primitive types.
 */
public class ReplicateRegionsAndRegionedSources {

    public static void main(String... args) throws IOException {
        charToAllButBooleanAndByte(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/regioned/ColumnRegionChar.java");
        charToAllButBooleanAndByte(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/regioned/DeferredColumnRegionChar.java");
        charToAllButBooleanAndByte(
                "extensions/parquet/table/src/main/java/io/deephaven/parquet/table/region/ParquetColumnRegionChar.java");
        final List<String> paths = charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/regioned/RegionedColumnSourceChar.java");
        fixupLong(paths.stream().filter(p -> p.contains("Long")).findFirst().get());
        fixupByte(paths.stream().filter(p -> p.contains("Byte")).findFirst().get());
    }

    private static void fixupByte(String path) throws IOException {
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

    private static void fixupLong(String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = addImport(lines,
                "import io.deephaven.engine.table.ColumnSource;",
                "import io.deephaven.engine.table.impl.sources.LocalDateWrapperSource;",
                "import io.deephaven.engine.table.impl.sources.LocalTimeWrapperSource;",
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
                "        return new LocalTimeWrapperSource(toZonedDateTime(zone), zone);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<LocalDate> toLocalDate(ZoneId zone) {",
                "        return new LocalDateWrapperSource(toZonedDateTime(zone), zone);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<Long> toEpochNano() {",
                "        return this;",
                "    }"));

        FileUtils.writeLines(new File(path), lines);
    }
}
