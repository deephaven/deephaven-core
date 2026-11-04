//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.base.verify.Require;
import io.deephaven.replication.ReplicatePrimitiveCode;
import io.deephaven.replication.ReplicationUtils;
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
import java.util.Collections;
import java.util.List;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateSourcesAndChunks {
    public static final String TASK = "replicateSourcesAndChunks";

    public static void main(String... args) throws IOException {
        replicateArraySources();
        replicateSparseArraySources();
        replicateSingleValues();

        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/aggregate/CharAggregateColumnSource.java");
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/aggregate/SlicedCharAggregateColumnSource.java");
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/aggregate/RangeAggregateColumnSourceChar.java");
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/UngroupedCharArrayColumnSource.java");
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/UngroupedCharVectorColumnSource.java");
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/UngroupedBoxedCharObjectVectorColumnSource.java");
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/UngroupedBoxedCharArrayColumnSource.java");

        charToAllButBooleanAndLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/ImmutableCharArraySource.java");
        fixupImmutableLongArraySource(charToLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/ImmutableCharArraySource.java"));
        fixupByteReinterpret(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/ImmutableByteArraySource.java");
        replicateObjectImmutableArraySource();

        charToAllButBooleanAndLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/Immutable2DCharArraySource.java");
        fixupImmutable2DLongArraySource(charToLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/Immutable2DCharArraySource.java"));
        fixupByteReinterpret(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/Immutable2DByteArraySource.java");
        replicateObjectImmutable2DArraySource();

        charToAllButBooleanAndLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/ImmutableConstantCharSource.java");
        fixupImmutableConstantLongSource(charToLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/ImmutableConstantCharSource.java"));
        fixupByteReinterpret(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/ImmutableConstantByteSource.java");
        replicateImmutableConstantObjectSource();

        charToAll(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/sized/SizedCharChunk.java");
        replicateObjectSizedChunk();

        charToAll(TASK, "engine/table/src/main/java/io/deephaven/engine/page/CharChunkPage.java");
        replicateObjectChunkPage("engine/table/src/main/java/io/deephaven/engine/page/CharChunkPage.java");

        charToAll(TASK, "extensions/source-support/src/main/java/io/deephaven/generic/page/ChunkHolderPageChar.java");
        replicateObjectChunkPage(
                "extensions/source-support/src/main/java/io/deephaven/generic/page/ChunkHolderPageChar.java");

        replicateChunks();
        replicateWritableChunks();

        replicateChunkChunks();
        replicateWritableChunkChunks();

        replicateResettableChunks();
        replicateResettableChunkChunks();

        replicateResettableWritableChunks();
        replicateResettableWritableChunkChunks();

        replicateFactories();
        charToAll(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/util/pools/CharChunkPool.java");
        charToAll(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/util/pools/CharChunkSoftPool.java");

        replicateChunkFillers();

        charToAll(TASK, "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfillers/CharChunkFiller.java");

        replicateChunkColumnSource();
    }

    private static void replicateObjectSizedChunk() throws IOException {
        String path = ReplicatePrimitiveCode
                .charToObject(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/sized/SizedCharChunk.java");
        final File classFile = new File(path);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "<T> the chunk's attribute", "<ATTR> the chunk's attribute",
                "SizedObjectChunk<T extends Any>", "SizedObjectChunk<T, ATTR extends Any>",
                "WritableObjectChunk<T>", "WritableObjectChunk<T, ATTR>");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateObjectChunkPage(final String charPath) throws IOException {
        String path = ReplicatePrimitiveCode.charToObject(TASK, charPath);
        final File classFile = new File(path);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "<ATTR extends Any>", "<T, ATTR extends Any>",
                " <ATTR", " <T, ATTR",
                "Object\\[]", "T[]",
                "Object value", "T value",
                "Object get\\(", "T get(");
        lines = lines.stream().map(x -> x.replaceAll("ObjectChunk<([^,>]+)>", "ObjectChunk<T, $1>"))
                .collect(Collectors.toList());
        lines = lines.stream().map(x -> x.replaceAll("ObjectChunkPage<([^,>]+)>", "ObjectChunkPage<T, $1>"))
                .collect(Collectors.toList());
        FileUtils.writeLines(classFile, lines);
    }

    private static void fixupImmutableLongArraySource(String longImmutableSource) throws IOException {
        final File resultClassJavaFile = new File(longImmutableSource);
        List<String> lines = FileUtils.readLines(resultClassJavaFile, Charset.defaultCharset());
        lines = addImport(lines, "import io.deephaven.engine.table.ColumnSource;");
        lines = addImport(lines, LongFunction.class, ToLongFunction.class, Instant.class, ZonedDateTime.class,
                LocalDate.class, LocalTime.class, Require.class, ZoneId.class);
        lines = standardCleanups(lines);
        lines = globalReplacements(lines, "/\\*\\s*MIXIN_IMPLS\\s*\\*/", ", ConvertibleTimeSource");
        lines = replaceRegion(lines, "fillChunkByRanges", l -> addLongToBoxedAdapter(l, "LongFunction<R>",
                "WritableObjectChunk<R, ? super Values>", "asWritableObjectChunk"));
        lines = replaceRegion(lines, "fillChunkByKeys", l -> addLongToBoxedAdapter(l, "LongFunction<R>",
                "WritableObjectChunk<R, ? super Values>", "asWritableObjectChunk"));
        lines = replaceRegion(lines, "fillChunkUnordered", l -> addLongToBoxedAdapter(l, "LongFunction<R>",
                "WritableObjectChunk<R, ? super Values>", "asWritableObjectChunk"));
        lines = replaceRegion(lines, "fillFromChunkByRanges",
                l -> addLongToBoxedAdapter(l, "ToLongFunction<R>", "ObjectChunk<R, ? extends Values>",
                        "asObjectChunk"));
        lines = replaceRegion(lines, "fillFromChunkByKeys",
                l -> addLongToBoxedAdapter(l, "ToLongFunction<R>", "ObjectChunk<R, ? extends Values>",
                        "asObjectChunk"));
        lines = replaceRegion(lines, "fillFromChunkUnordered",
                l -> addLongToBoxedAdapter(l, "ToLongFunction<R>", "ObjectChunk<R, ? extends Values>",
                        "asObjectChunk"));
        lines = replaceRegion(lines, "reinterpretation", Arrays.asList(
                "    @Override",
                "    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        return alternateDataType == long.class || alternateDataType == Instant.class;",
                "    }",
                "",
                "    @SuppressWarnings(\"unchecked\")",
                "    @Override",
                "    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        if (alternateDataType == this.getType()) {",
                "            return (ColumnSource<ALTERNATE_DATA_TYPE>) this;",
                "        } else if (alternateDataType == Instant.class) {",
                "            return (ColumnSource<ALTERNATE_DATA_TYPE>) toInstant();",
                "        }",
                "",
                "        throw new IllegalArgumentException(\"Cannot reinterpret `\" + getType().getName() + \"` to `\" + alternateDataType.getName() + \"`\");",
                "    }",
                "",
                "    @Override",
                "    public boolean supportsTimeConversion() {",
                "        return true;",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {",
                "        return new ImmutableZonedDateTimeArraySource(Require.neqNull(zone, \"zone\"), this);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<LocalDate> toLocalDate(@NotNull final ZoneId zone) {",
                "        return new LongAsLocalDateColumnSource(this, zone);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<LocalTime> toLocalTime(@NotNull final ZoneId zone) {",
                "        return new LongAsLocalTimeColumnSource(this, zone);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<Instant> toInstant() {",
                "        return new ImmutableInstantArraySource(this);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<Long> toEpochNano() {",
                "        return this;",
                "    }"));
        FileUtils.writeLines(resultClassJavaFile, lines);
    }

    private static void fixupImmutableConstantLongSource(String longImmutableSource) throws IOException {
        final File resultClassJavaFile = new File(longImmutableSource);
        List<String> lines = FileUtils.readLines(resultClassJavaFile, Charset.defaultCharset());
        lines = addImport(lines, "import io.deephaven.engine.table.ColumnSource;");
        lines = addImport(lines, Instant.class, ZonedDateTime.class, LocalDate.class, LocalTime.class, Require.class,
                ZoneId.class);
        lines = standardCleanups(lines);
        lines = globalReplacements(lines, "/\\*\\s*MIXIN_IMPLS\\s*\\*/", ", ConvertibleTimeSource");
        lines = replaceRegion(lines, "reinterpretation", Arrays.asList(
                "    @Override",
                "    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        return alternateDataType == long.class || alternateDataType == Instant.class;",
                "    }",
                "",
                "    @SuppressWarnings(\"unchecked\")",
                "    @Override",
                "    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        if (alternateDataType == this.getType()) {",
                "            return (ColumnSource<ALTERNATE_DATA_TYPE>) this;",
                "        } else if (alternateDataType == Instant.class) {",
                "            return (ColumnSource<ALTERNATE_DATA_TYPE>) toInstant();",
                "        }",
                "",
                "        throw new IllegalArgumentException(\"Cannot reinterpret `\" + getType().getName() + \"` to `\" + alternateDataType.getName() + \"`\");",
                "    }",
                "",
                "    @Override",
                "    public boolean supportsTimeConversion() {",
                "        return true;",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {",
                "        return new ImmutableConstantZonedDateTimeSource(Require.neqNull(zone, \"zone\"), this);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<LocalDate> toLocalDate(@NotNull final ZoneId zone) {",
                "        return new LongAsLocalDateColumnSource(this, zone);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<LocalTime> toLocalTime(@NotNull final ZoneId zone) {",
                "        return new LongAsLocalTimeColumnSource(this, zone);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<Instant> toInstant() {",
                "        return new ImmutableConstantInstantSource(this);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<Long> toEpochNano() {",
                "        return this;",
                "    }"));
        FileUtils.writeLines(resultClassJavaFile, lines);
    }

    private static void fixupImmutable2DLongArraySource(String longImmutableSource) throws IOException {
        final File resultClassJavaFile = new File(longImmutableSource);
        List<String> lines = FileUtils.readLines(resultClassJavaFile, Charset.defaultCharset());
        lines = addImport(lines, "import io.deephaven.engine.table.ColumnSource;");
        lines = addImport(lines, LongFunction.class, ToLongFunction.class, Instant.class, ZonedDateTime.class,
                LocalDate.class, LocalTime.class, Require.class, ZoneId.class);
        lines = standardCleanups(lines);
        lines = globalReplacements(lines, "/\\*\\s*MIXIN_IMPLS\\s*\\*/", ", ConvertibleTimeSource");
        lines = replaceRegion(lines, "fillChunkByRanges", l -> addLongToBoxedAdapter(l, "LongFunction<R>",
                "WritableObjectChunk<R, ? super Values>", "asWritableObjectChunk"));
        lines = replaceRegion(lines, "fillChunkByKeys", l -> addLongToBoxedAdapter(l, "LongFunction<R>",
                "WritableObjectChunk<R, ? super Values>", "asWritableObjectChunk"));
        lines = replaceRegion(lines, "fillChunkUnordered", l -> addLongToBoxedAdapter(l, "LongFunction<R>",
                "WritableObjectChunk<R, ? super Values>", "asWritableObjectChunk"));
        lines = replaceRegion(lines, "fillFromChunkByRanges",
                l -> addLongToBoxedAdapter(l, "ToLongFunction<R>", "ObjectChunk<R, ? extends Values>",
                        "asObjectChunk"));
        lines = replaceRegion(lines, "fillFromChunkByKeys",
                l -> addLongToBoxedAdapter(l, "ToLongFunction<R>", "ObjectChunk<R, ? extends Values>",
                        "asObjectChunk"));
        lines = replaceRegion(lines, "fillFromChunkUnordered",
                l -> addLongToBoxedAdapter(l, "ToLongFunction<R>", "ObjectChunk<R, ? extends Values>",
                        "asObjectChunk"));
        lines = replaceRegion(lines, "reinterpretation", Arrays.asList(
                "    @Override",
                "    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        return alternateDataType == long.class || alternateDataType == Instant.class;",
                "    }",
                "",
                "    @SuppressWarnings(\"unchecked\")",
                "    @Override",
                "    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        if (alternateDataType == this.getType()) {",
                "            return (ColumnSource<ALTERNATE_DATA_TYPE>) this;",
                "        } else if (alternateDataType == Instant.class) {",
                "            return (ColumnSource<ALTERNATE_DATA_TYPE>) toInstant();",
                "        }",
                "",
                "        throw new IllegalArgumentException(\"Cannot reinterpret `\" + getType().getName() + \"` to `\" + alternateDataType.getName() + \"`\");",
                "    }",
                "",
                "    @Override",
                "    public boolean supportsTimeConversion() {",
                "        return true;",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {",
                "        return new Immutable2DZonedDateTimeArraySource(Require.neqNull(zone, \"zone\"), this);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<LocalDate> toLocalDate(@NotNull final ZoneId zone) {",
                "        return new LongAsLocalDateColumnSource(this, zone);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<LocalTime> toLocalTime(@NotNull final ZoneId zone) {",
                "        return new LongAsLocalTimeColumnSource(this, zone);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<Instant> toInstant() {",
                "        return new Immutable2DInstantArraySource(this);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<Long> toEpochNano() {",
                "        return this;",
                "    }"));
        FileUtils.writeLines(resultClassJavaFile, lines);
    }

    private static void fixupByteReinterpret(String byteImmutableSource) throws IOException {
        final File resultClassJavaFile = new File(byteImmutableSource);
        List<String> lines = FileUtils.readLines(resultClassJavaFile, Charset.defaultCharset());
        lines = addImport(lines, "import io.deephaven.engine.table.ColumnSource;");
        lines = replaceRegion(lines, "reinterpretation", Arrays.asList(
                "    @Override",
                "    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(",
                "            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        return alternateDataType == Boolean.class;",
                "    }",
                "",
                "    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(",
                "               @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "         //noinspection unchecked",
                "         return (ColumnSource<ALTERNATE_DATA_TYPE>) new ByteAsBooleanColumnSource(this);",
                "    }"));
        FileUtils.writeLines(resultClassJavaFile, lines);
    }

    private static void replicateSingleValues() throws IOException {
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/CharacterSingleValueSource.java");
        replicateObjectSingleValue();
        replicateBooleanSingleValue();
    }

    private static void replicateObjectSingleValue() throws IOException {
        final String resultClassJavaPath = charToObject(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/CharacterSingleValueSource.java");
        final File resultClassJavaFile = new File(resultClassJavaPath);
        List<String> lines = FileUtils.readLines(resultClassJavaFile, Charset.defaultCharset());
        try {
            lines = ReplicationUtils.removeImport(lines, "import static io.deephaven.util.QueryConstants.NULL_OBJECT;");
        } catch (Exception e) {
            // Hey' it's fiiiiine. Don't worrrryy about it!
        }
        lines = globalReplacements(lines,
                "class ObjectSingleValueSource", "class ObjectSingleValueSource<T>",
                "<Object>", "<T>",
                "ForObject", "ForObject<T>",
                "Object getObject", "T get",
                "Object getPrevObject", "T getPrev",
                "Object current", "T current",
                "Object prev", "T prev",
                "ColumnSource<[?] extends Object>", "ColumnSource<? extends T>",
                "getObject", "get",
                "getPrevObject", "getPrev",
                "set\\(Object", "set(T",
                "set\\(long key, Object", "set(long key, T",
                "set\\(NULL_OBJECT", "set(null",
                "final ObjectChunk<[?] extends Values>", "final ObjectChunk<T, ? extends Values>",
                "unbox\\((.*)\\)", "$1",
                "NULL_OBJECT", "null",
                "WritableObjectChunk<[?] super Values>", "WritableObjectChunk<T, ? super Values>",
                "Object value", "T value");
        lines = ReplicationUtils.removeRegion(lines, "UnboxedSetter");
        lines = ReplicationUtils.replaceRegion(lines, "Constructor", Arrays.asList(
                "    public ObjectSingleValueSource(Class<T> type, Class<?> componentType) {",
                "        super(type, componentType);",
                "        current = null;",
                "        prev = null;",
                "    }"));
        FileUtils.writeLines(resultClassJavaFile, lines);
    }

    private static void replicateBooleanSingleValue() throws IOException {
        final String resultClassJavaPath = charToBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/CharacterSingleValueSource.java");
        final File resultClassJavaFile = new File(resultClassJavaPath);
        List<String> lines = FileUtils.readLines(resultClassJavaFile, Charset.defaultCharset());
        lines = ReplicationUtils.addImport(lines,
                "import io.deephaven.chunk.ObjectChunk;",
                "import io.deephaven.chunk.WritableObjectChunk;");
        lines = ReplicationUtils.removeImport(lines,
                "import io.deephaven.chunk.BooleanChunk;",
                "import io.deephaven.chunk.WritableBooleanChunk;",
                "import static io.deephaven.util.type.TypeUtils.unbox;");
        lines = globalReplacements(lines,
                "boolean current", "Boolean current",
                "boolean prev", "Boolean prev",
                "super\\(boolean.class", "super(Boolean.class",
                "set\\(long key, boolean", "set(long key, Boolean",
                "getBoolean", "get",
                "getPrevBoolean", "getPrev",
                "boolean get", "Boolean get",
                "boolean value", "Boolean value",
                "final BooleanChunk<[?] extends Values>", "final ObjectChunk<Boolean, ? extends Values>",
                "final WritableBooleanChunk<[?] super Values>", "final WritableObjectChunk<Boolean, ? super Values>",
                "asBooleanChunk\\(", "asObjectChunk(",
                "asWritableBooleanChunk\\(", "asWritableObjectChunk(",
                "unbox\\((.*)\\)", "$1");
        lines = ReplicationUtils.removeRegion(lines, "UnboxedSetter");
        FileUtils.writeLines(resultClassJavaFile, lines);
    }

    private static void replicateChunkColumnSource() throws IOException {
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/chunkcolumnsource/CharChunkColumnSource.java");
        replicateObjectChunkColumnSource();
    }

    private static void replicateObjectChunkColumnSource() throws IOException {
        final String resultClassJavaPath = charToObject(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/chunkcolumnsource/CharChunkColumnSource.java");
        final File resultClassJavaFile = new File(resultClassJavaPath);
        List<String> lines = FileUtils.readLines(resultClassJavaFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "class ObjectChunkColumnSource", "class ObjectChunkColumnSource<T>");
        lines = genericObjectColumnSourceReplacements(lines);

        lines = ReplicationUtils.replaceRegion(lines, "constructor", Arrays.asList(
                "    protected ObjectChunkColumnSource(Class<T> type, Class<?> componentType) {",
                "        this(type, componentType, new LongArrayList());",
                "    }",
                "",
                "    protected ObjectChunkColumnSource(Class<T> type, Class<?> componentType, final LongArrayList firstOffsetForData) {",
                "        super(type, componentType);",
                "        this.firstOffsetForData = firstOffsetForData;",
                "    }"

        ));

        FileUtils.writeLines(resultClassJavaFile, lines);
    }

    private static void replicateObjectImmutableArraySource() throws IOException {
        replicateObjectImmutableSource(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/ImmutableCharArraySource.java");
    }

    private static void replicateObjectImmutable2DArraySource() throws IOException {
        replicateObjectImmutableSource(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/Immutable2DCharArraySource.java");
    }

    private static void replicateImmutableConstantObjectSource() throws IOException {
        replicateObjectImmutableSource(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/ImmutableConstantCharSource.java");
    }

    private static void replicateObjectImmutableSource(String immutableSourcePath) throws IOException {
        final String resultClassJavaPath = charToObject(TASK, immutableSourcePath);
        final File resultClassJavaFile = new File(resultClassJavaPath);
        List<String> lines = FileUtils.readLines(resultClassJavaFile, Charset.defaultCharset());
        lines = removeRegion(lines, "boxing imports");
        lines = globalReplacements(lines,
                "class ImmutableObjectArraySource", "class ImmutableObjectArraySource<T>",
                "class Immutable2DObjectArraySource", "class Immutable2DObjectArraySource<T>",
                "class ImmutableConstantObjectSource", "class ImmutableConstantObjectSource<T>",
                "Object value", "T value",
                "\\? extends Object", "\\? extends T",
                "copyFromTypedArray\\(data", "copyFromTypedArray\\(\\(T[]\\)data",
                "resetFromTypedArray\\(data", "resetFromTypedArray\\(\\(T[]\\)data",
                "copyToTypedArray\\((.*), data", "copyToTypedArray\\($1, \\(T[]\\)data",
                "ObjectDest.set(ii, data\\[key\\])", " ObjectDest.set(ii, (T)data[key])",
                "Object getUnsafe", "T getUnsafe",
                "return data([^;])", "return (T)data$1");

        lines = genericObjectColumnSourceReplacements(lines);

        if (immutableSourcePath.contains("2D")) {
            lines = simpleFixup(lines, "constructor",
                    "Immutable2DObjectArraySource\\(\\)",
                    "Immutable2DObjectArraySource\\(Class<T> type, Class<?> componentType\\)",
                    "Immutable2DObjectArraySource\\(int",
                    "Immutable2DObjectArraySource\\(Class<T> type, Class<?> componentType, int",
                    "super\\(Object.class\\)", "super\\(type, componentType\\)",
                    "this\\(\\)", "this\\(type, componentType\\)",
                    "this\\(DEFAULT_SEGMENT_SHIFT\\)", "this\\(type, componentType, DEFAULT_SEGMENT_SHIFT\\)");
            lines = simpleFixup(lines, "allocateArray", "return \\(T\\)data;", "return data;");
        } else if (immutableSourcePath.contains("Array")) {
            lines = simpleFixup(lines, "constructor",
                    "ImmutableObjectArraySource\\(",
                    "ImmutableObjectArraySource\\(Class<T> type, Class<?> componentType",
                    "super\\(Object.class\\)", "super\\(type, componentType\\)");
            lines = simpleFixup(lines, "array constructor",
                    "ImmutableObjectArraySource\\(",
                    "ImmutableObjectArraySource\\(Class<T> type, Class<?> componentType, ",
                    "super\\(Object.class\\)", "super\\(type, componentType\\)");
        } else if (immutableSourcePath.contains("Constant")) {
            lines = simpleFixup(lines, "constructor",
                    "ImmutableConstantObjectSource\\(",
                    "ImmutableConstantObjectSource\\(@NotNull final Class<T> type, final Class<?> componentType, ",
                    "super\\(Object.class\\)", "super\\(type, componentType\\)");
        } else {
            throw new IllegalStateException("Unexpected source path " + immutableSourcePath);
        }

        FileUtils.writeLines(resultClassJavaFile, lines);
    }

    private static List<String> genericObjectColumnSourceReplacements(List<String> lines) {
        lines = globalReplacements(lines,
                "<Object>", "<T>",
                "ForObject", "ForObject<T>",
                "Object getObject", "T get",
                "getObject\\(", "get\\(",
                "Object current", "T current",
                "ObjectChunk<\\? extends Values>", "ObjectChunk<T, ? extends Values>",
                "WritableObjectChunk<\\? extends Values>", "WritableObjectChunk<T, ? extends Values>",
                "WritableObjectChunk<\\? super Values>", "WritableObjectChunk<T, ? super Values>",
                "QueryConstants.NULL_OBJECT", "null",
                "NULL_OBJECT", "null");
        return lines;
    }

    private static void replicateArraySources() throws IOException {
        charToAllButBooleanAndLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/CharacterArraySource.java");
        replicateLongArraySource();
    }

    private static void replicateSparseArraySources() throws IOException {
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/sparse/CharCompactLeaf.java");
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/sparse/CharSortedLeaf.java");
        charToAllButBoolean(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/sparse/CharBitmapLeaf.java");
        charToAllButBooleanAndLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/CharacterSparseArraySource.java");
        replicateLongSparseArraySource();
        replicateObjectSparseArraySource();
    }

    private static void replicateObjectSparseArraySource() throws IOException {
        for (final String leaf : new String[] {"CharCompactLeaf", "CharSortedLeaf", "CharBitmapLeaf"}) {
            final File leafFile = new File(charToObject(TASK,
                    "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/sparse/" + leaf + ".java"));
            List<String> lines = FileUtils.readLines(leafFile, Charset.defaultCharset());
            if (lines.contains("import static io.deephaven.util.QueryConstants.NULL_OBJECT;")) {
                lines = removeImport(lines, "import static io.deephaven.util.QueryConstants.NULL_OBJECT;");
            }
            lines = globalReplacements(lines,
                    "ObjectChunk<\\?>", "ObjectChunk<Object, ?>",
                    "NULL_OBJECT", "null",
                    "Object.BYTES", "REFERENCE_SIZE");
            if (lines.stream().anyMatch(line -> line.contains("REFERENCE_SIZE"))) {
                lines = addReferenceSize(lines);
            }
            FileUtils.writeLines(leafFile, lines);
        }

        final File sourceFile = new File(charToObject(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/CharacterSparseArraySource.java"));
        List<String> lines = FileUtils.readLines(sourceFile, Charset.defaultCharset());
        lines = removeRegion(lines, "boxed methods");
        lines = removeRegion(lines, "boxing imports");
        lines = removeImport(lines, "import static io.deephaven.util.QueryConstants.NULL_OBJECT;");
        lines = globalReplacements(lines,
                "public class ObjectSparseArraySource extends SparseArrayColumnSource<Object>",
                "public class ObjectSparseArraySource<T> extends SparseArrayColumnSource<T>",
                "MutableColumnSourceGetDefaults.ForObject ", "MutableColumnSourceGetDefaults.ForObject<T> ",
                "WritableColumnSource<Object>", "WritableColumnSource<T>",
                "WritableObjectChunk<\\? super Values>", "WritableObjectChunk<Object, ? super Values>",
                "ObjectChunk<\\? extends Values>", "ObjectChunk<Object, ? extends Values>",
                "public ObjectChunk<Values>", "public ObjectChunk<T, Values>",
                "copyFromTypedArray", "copyFromArray",
                "copyToTypedArray", "copyToArray",
                "public final void set\\(final long key, final Object value\\)",
                "public final void set(final long key, final T value)",
                "getObject\\(", "get(",
                "NULL_OBJECT", "null",
                "Object.BYTES", "REFERENCE_SIZE");
        lines = replaceRegion(lines, "recycler", Arrays.asList(
                "    /**",
                "     * Blocks for previous value snapshots, which are not null-filled when borrowed. A returned block is",
                "     * cleared, so that the recycler holds no references to previous values.",
                "     */",
                "    private static final SoftRecycler<Object[]> snapshotRecycler =",
                "            new SoftRecycler<>(SparseArrayColumnSourceConfiguration.OBJECT_RECYCLER_CAPACITY,",
                "                    () -> new Object[BLOCK_SIZE], block -> Arrays.fill(block, null));"));
        lines = replaceRegion(lines, "constructor", Arrays.asList(
                "    /**",
                "     * Make a source whose leaves are all full blocks.",
                "     */",
                "    public ObjectSparseArraySource(final Class<T> type) {",
                "        this(type, null);",
                "    }",
                "",
                "    /**",
                "     * Make a source whose leaves are all full blocks.",
                "     */",
                "    public ObjectSparseArraySource(final Class<T> type, final Class<?> componentType) {",
                "        this(type, componentType, DEFAULT_SORTED_THRESHOLD, 0);",
                "    }",
                "",
                "    /**",
                "     * @param type the type of the values",
                "     * @param componentType the component type of the values, if they are arrays or vectors",
                "     * @param sortedThreshold the largest number of values a sorted leaf holds",
                "     * @param compactThreshold the largest number of values a sorted or bitmap leaf holds, from 0, when",
                "     *        every leaf is a full block, to the block size",
                "     */",
                "    public ObjectSparseArraySource(",
                "            final Class<T> type,",
                "            final Class<?> componentType,",
                "            final int sortedThreshold,",
                "            final int compactThreshold) {",
                "        super(type, componentType);",
                "        this.sortedThreshold = checkSortedThreshold(sortedThreshold, compactThreshold);",
                "        this.compactThreshold = compactThreshold;",
                "        this.recordThreshold = compactThreshold > 0 ? compactThreshold : DEFAULT_COMPACT_THRESHOLD;",
                "    }"));
        lines = replaceRegion(lines, "primitive get", Arrays.asList(
                "    @Override",
                "    public final T get(final long rowKey) {",
                "        if (rowKey < 0) {",
                "            return null;",
                "        }",
                "        final Level2 level2 = getLevel2(rowKey);",
                "        if (level2 == null) {",
                "            return null;",
                "        }",
                "        final int block2 = block2(rowKey);",
                "        final int offset = (int) (rowKey & INDEX_MASK);",
                "        final Object[] block = level2.getBlock(block2);",
                "        if (block != null) {",
                "            // noinspection unchecked",
                "            return (T) block[offset];",
                "        }",
                "        final ObjectCompactLeaf compact = level2.getCompact(block2);",
                "        // noinspection unchecked",
                "        return compact == null ? null : (T) compact.get(offset);",
                "    }",
                "",
                "    @Override",
                "    public final T getPrev(final long rowKey) {",
                "        // noinspection unchecked",
                "        return (T) getPrevValue(rowKey);",
                "    }"));
        lines = addReferenceSize(lines);
        FileUtils.writeLines(sourceFile, lines);
    }

    /**
     * Declare {@code REFERENCE_SIZE}, the bytes an object reference occupies, after the opening line of the class.
     */
    private static List<String> addReferenceSize(final List<String> lines) {
        final int classLine = findClassBodyStart(lines);
        final List<String> result = new ArrayList<>(lines.subList(0, classLine + 1));
        result.add("    private static final int REFERENCE_SIZE = 8;");
        result.add("");
        result.addAll(lines.subList(classLine + 1, lines.size()));
        return result;
    }

    /**
     * @return the index of the line that opens the body of the first top level class
     */
    private static int findClassBodyStart(final List<String> lines) {
        for (int ii = 0; ii < lines.size(); ++ii) {
            if (lines.get(ii).startsWith("public ") && lines.get(ii).contains(" class ")) {
                for (int jj = ii; jj < lines.size(); ++jj) {
                    if (lines.get(jj).endsWith("{")) {
                        return jj;
                    }
                }
            }
        }
        throw new IllegalStateException("No class declaration found");
    }

    private static void replicateChunks() throws IOException {
        charToAllButBooleanAndByte(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/CharChunk.java");
        replicateByteChunks();
        replicateBooleanChunks();
        replicateObjectChunks();
    }

    private static void replicateByteChunks() throws IOException {
        final String className = charToByte(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/CharChunk.java");
        final File classFile = new File(className);

        List<String> classLines = FileUtils.readLines(classFile, Charset.defaultCharset());

        classLines = replaceRegion(classLines, "ApplyDecoder", Arrays.asList(
                "    public final <T> T applyDecoder(ObjectDecoder<T> decoder) {",
                "        return decoder.decode(data, offset, size);",
                "    }",
                "",
                "    public final <T> T applyDecoder(ObjectDecoder<T> decoder, int offsetSrc, int length) {",
                "        return decoder.decode(data, offset + offsetSrc, length);",
                "    }"));
        classLines = replaceRegion(classLines, "ApplyDecoderImports", Collections.singletonList(
                "import io.deephaven.util.codec.ObjectDecoder;"));

        FileUtils.writeLines(classFile, classLines);
    }

    private static void replicateBooleanChunks() throws IOException {
        final String className = charToBoolean(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/CharChunk.java");
        final File classFile = new File(className);
        List<String> classLines = FileUtils.readLines(classFile, Charset.defaultCharset());
        classLines = ReplicationUtils.removeRegion(classLines, "BufferImports");
        classLines = ReplicationUtils.removeRegion(classLines, "CopyToBuffer");
        classLines = ReplicationUtils.removeRegion(classLines, "BinarySearchImports");
        classLines = ReplicationUtils.removeRegion(classLines, "BinarySearch");
        classLines = ReplicationUtils.replaceRegion(classLines, "isNull", Arrays.asList(
                "    public final boolean isNull(int index) {",
                "        return false;",
                "    }"));
        FileUtils.writeLines(classFile, classLines);
    }

    private static void replicateObjectChunks() throws IOException {
        final String className = charToObject(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/CharChunk.java");
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "Object\\[\\]", "T[]",
                "ObjectChunk.downcast", "ObjectChunk.downcastTypeAndAttr",
                "Object get", "T get",
                " <ATTR", " <T, ATTR",
                "ObjectChunk<ATTR", "ObjectChunk<T, ATTR",
                "ObjectChunk<[?] ", "ObjectChunk<T, ? ",
                "ObjectChunk<Any> EMPTY", "ObjectChunk<Object, Any> EMPTY",
                "static T\\[\\] makeArray", "static <T> T[] makeArray",
                "QueryConstants.NULL_OBJECT", "null");

        lines = replaceRegion(lines, "makeArray", Arrays.asList(
                "    public static <T> T[] makeArray(int capacity) {",
                "        if (capacity == 0) {",
                "            //noinspection unchecked",
                "            return (T[]) ArrayTypeUtils.EMPTY_OBJECT_ARRAY;",
                "        }",
                "        //noinspection unchecked",
                "        return (T[])new Object[capacity];",
                "    }"));
        lines = ReplicationUtils.removeRegion(lines, "BufferImports");
        lines = ReplicationUtils.removeRegion(lines, "CopyToBuffer");
        lines = expandDowncast(lines, "ObjectChunk");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateChunkChunks() throws IOException {
        charToAll(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/CharChunkChunk.java");
        replicateObjectChunkChunks();
    }

    private static void replicateObjectChunkChunks() throws IOException {
        final String className = charToObject(TASK,
                "engine/chunk/src/main/java/io/deephaven/chunk/CharChunkChunk.java");
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "<ATTR extends Any>", "<T, ATTR extends Any>",
                "ObjectChunkChunk<Any> EMPTY", "ObjectChunkChunk<Object, Any> EMPTY",
                "ObjectChunk<ATTR", "ObjectChunk<T, ATTR",
                "ObjectChunk<[?]", "ObjectChunk<T, ?",
                "ObjectChunkChunk<ATTR", "ObjectChunkChunk<T, ATTR",
                "ObjectChunk<ATTR>\\[]", "ObjectChunk<T, ATTR>[]",
                "Object\\[]\\[]", "T[][]",
                "Object get", "T get",
                "(  +)innerData = ", "$1//noinspection unchecked" + "\n$1innerData = (T[][])");

        lines = expandDowncast(lines, "ObjectChunkChunk");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateWritableChunks() throws IOException {
        final List<String> files =
                charToAllButBoolean(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/WritableCharChunk.java");
        for (String fileName : files) {
            final File classFile = new File(fileName);
            List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
            FileUtils.writeLines(classFile, lines);
        }
        replicateWritableBooleanChunks();
        replicateWritableObjectChunks();
    }

    private static void replicateWritableBooleanChunks() throws IOException {
        final String writableBooleanChunkClassName = charToBoolean(TASK,
                "engine/chunk/src/main/java/io/deephaven/chunk/WritableCharChunk.java");
        final File writableBooleanChunkClassFile = new File(writableBooleanChunkClassName);
        List<String> writableBooleanChunkClassLines =
                FileUtils.readLines(writableBooleanChunkClassFile, Charset.defaultCharset());
        writableBooleanChunkClassLines =
                ReplicationUtils.removeRegion(writableBooleanChunkClassLines, "BufferImports");
        writableBooleanChunkClassLines =
                ReplicationUtils.removeRegion(writableBooleanChunkClassLines, "CopyFromBuffer");
        writableBooleanChunkClassLines =
                ReplicationUtils.removeRegion(writableBooleanChunkClassLines, "FillWithNullValueImports");
        writableBooleanChunkClassLines =
                ReplicationUtils.removeRegion(writableBooleanChunkClassLines, "FillWithNullValueImpl");
        writableBooleanChunkClassLines = ReplicationUtils.removeRegion(writableBooleanChunkClassLines, "sort");
        FileUtils.writeLines(writableBooleanChunkClassFile, writableBooleanChunkClassLines);
    }

    private static void replicateWritableObjectChunks() throws IOException {
        final String className = charToObject(TASK,
                "engine/chunk/src/main/java/io/deephaven/chunk/WritableCharChunk.java");
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "<ATTR extends Any>", "<T, ATTR extends Any>",
                "Object\\[]", "T[]",
                "ObjectChunk<ATTR", "ObjectChunk<T, ATTR",
                "ObjectChunk<[?]", "ObjectChunk<T, ?",
                "WritableObjectChunk<ATTR", "WritableObjectChunk<T, ATTR",
                " <ATTR", " <T, ATTR",
                "Object value", "T value",
                "( +)(final T\\[] typedArray)", "$1//noinspection unchecked" + "\n$1$2",
                "NULL_OBJECT", "null");
        lines = ReplicationUtils.removeRegion(lines, "CopyFromBuffer");
        lines = ReplicationUtils.removeRegion(lines, "FillWithNullValueImports");
        lines = ReplicationUtils.removeRegion(lines, "BufferImports");
        lines = expandDowncast(lines, "WritableObjectChunk");
        lines = ReplicationUtils.replaceRegion(lines, "fillWithBoxedValue", Arrays.asList(
                "    @Override",
                "    public final void fillWithBoxedValue(int offset, int size, Object value) {",
                "        fillWithValue(offset,size, (T)value);",
                "    }"));
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateWritableChunkChunks() throws IOException {
        charToAllButBoolean(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/WritableCharChunkChunk.java");
        replicateWritableBooleanChunkChunks();
        replicateWritableObjectChunkChunks();
    }

    private static void replicateWritableBooleanChunkChunks() throws IOException {
        final String writableBooleanChunkClassName = charToBoolean(TASK,
                "engine/chunk/src/main/java/io/deephaven/chunk/WritableCharChunkChunk.java");
        final File writableBooleanChunkClassFile = new File(writableBooleanChunkClassName);
        List<String> writableBooleanChunkClassLines =
                FileUtils.readLines(writableBooleanChunkClassFile, Charset.defaultCharset());
        writableBooleanChunkClassLines =
                ReplicationUtils.removeRegion(writableBooleanChunkClassLines, "BufferImports");
        writableBooleanChunkClassLines =
                ReplicationUtils.removeRegion(writableBooleanChunkClassLines, "CopyFromBuffer");
        writableBooleanChunkClassLines =
                ReplicationUtils.removeRegion(writableBooleanChunkClassLines, "FillWithNullValueImports");
        writableBooleanChunkClassLines =
                ReplicationUtils.removeRegion(writableBooleanChunkClassLines, "FillWithNullValueImpl");
        writableBooleanChunkClassLines = ReplicationUtils.removeRegion(writableBooleanChunkClassLines, "sort");
        FileUtils.writeLines(writableBooleanChunkClassFile, writableBooleanChunkClassLines);
    }

    private static void replicateWritableObjectChunkChunks() throws IOException {
        final String className = charToObject(TASK,
                "engine/chunk/src/main/java/io/deephaven/chunk/WritableCharChunkChunk.java");
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "<ATTR extends Any>", "<T, ATTR extends Any>",
                "Object\\[]", "T[]",
                "ObjectChunkChunk<ATTR", "ObjectChunkChunk<T, ATTR",
                "WritableObjectChunk<ATTR", "WritableObjectChunk<T, ATTR",
                "WritableObjectChunk<[?]", "WritableObjectChunk<?, ATTR",
                "Object value", "T value",
                "( +)(final T\\[] realType)", "$1//noinspection unchecked" + "\n$1$2",
                "NULL_OBJECT", "null");
        lines = ReplicationUtils.removeRegion(lines, "CopyFromBuffer");
        lines = ReplicationUtils.removeRegion(lines, "FillWithNullValueImports");
        lines = ReplicationUtils.removeRegion(lines, "BufferImports");
        lines = expandDowncast(lines, "WritableObjectChunkChunk");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateResettableChunks() throws IOException {
        charToAll(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/ResettableCharChunk.java");
        replicateResettableObjectChunks();
    }

    private static void replicateResettableObjectChunks() throws IOException {
        final String className = charToObject(TASK,
                "engine/chunk/src/main/java/io/deephaven/chunk/ResettableCharChunk.java");
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                " <ATTR_BASE", " <T, ATTR_BASE",
                "ObjectChunk<ATTR", "ObjectChunk<T, ATTR",
                "ObjectChunk<[?] ", "ObjectChunk<T, ? ",
                "Object\\[]", "T[]",
                "ResettableObjectChunk<ATTR", "ResettableObjectChunk<T, ATTR",
                "( +)this\\(ArrayTypeUtils.EMPTY_OBJECT_ARRAY",
                "$1//noinspection unchecked" + "\n$1this((T[])ArrayTypeUtils.EMPTY_OBJECT_ARRAY",
                "( +)(final T\\[] typedArray)", "$1//noinspection unchecked" + "\n$1$2");
        lines = expandDowncast(lines, "ResettableObjectChunk");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateResettableWritableChunks() throws IOException {
        charToAll(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/ResettableWritableCharChunk.java");
        replicateResettableWritableObjectChunks();
    }

    private static void replicateResettableWritableObjectChunks() throws IOException {
        final String className = charToObject(TASK,
                "engine/chunk/src/main/java/io/deephaven/chunk/ResettableWritableCharChunk.java");
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                " <ATTR_BASE", " <T, ATTR_BASE",
                "ObjectChunk<ATTR", "ObjectChunk<T, ATTR",
                "ObjectChunk<[?] ", "ObjectChunk<T, ? ",
                "Object\\[]", "T[]",
                "ResettableObjectChunk<ATTR", "ResettableObjectChunk<T, ATTR",
                "( +)this\\(ArrayTypeUtils.EMPTY_OBJECT_ARRAY",
                "$1//noinspection unchecked" + "\n$1this((T[])ArrayTypeUtils.EMPTY_OBJECT_ARRAY",
                "( +)(final T\\[] typedArray)", "$1//noinspection unchecked" + "\n$1$2");
        lines = expandDowncast(lines, "ResettableWritableObjectChunk");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateResettableWritableChunkChunks() throws IOException {
        charToAll(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/ResettableWritableCharChunkChunk.java");
        replicateResettableWritableObjectChunkChunks();
    }

    private static void replicateResettableWritableObjectChunkChunks() throws IOException {
        final String className = charToObject(TASK,
                "engine/chunk/src/main/java/io/deephaven/chunk/ResettableWritableCharChunkChunk.java");
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "<ATTR extends Any>", "<T, ATTR extends Any>",
                "WritableObjectChunkChunk<ATTR", "WritableObjectChunkChunk<T, ATTR",
                "WritableObjectChunk<ATTR", "WritableObjectChunk<T, ATTR",
                "WritableObjectChunk<[?]", "WritableObjectChunk<T, ?",
                "Object\\[]", "T[]",
                "( +)this\\(ArrayTypeUtils.EMPTY_OBJECT_ARRAY",
                "$1//noinspection unchecked" + "\n$1this((T[])ArrayTypeUtils.EMPTY_OBJECT_ARRAY",
                "( +)(final T\\[] typedArray)", "$1//noinspection unchecked" + "\n$1$2");

        lines = expandDowncast(lines, "ResettableWritableObjectChunkChunk");

        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateResettableChunkChunks() throws IOException {
        charToAll(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/ResettableCharChunkChunk.java");
        replicateResettableObjectChunkChunks();
    }

    private static void replicateResettableObjectChunkChunks() throws IOException {
        final String className = charToObject(TASK,
                "engine/chunk/src/main/java/io/deephaven/chunk/ResettableCharChunkChunk.java");
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "<ATTR extends Any>", "<T, ATTR extends Any>",
                "ResettableObjectChunkChunk<ATTR", "ResettableObjectChunkChunk<T, ATTR",
                "ObjectChunk<ATTR", "ObjectChunk<T, ATTR",
                "ObjectChunk<[?]", "ObjectChunk<T, ?",
                "ObjectChunkChunk<ATTR", "ObjectChunkChunk<T, ATTR");
        lines = expandDowncast(lines, "ResettableObjectChunkChunk");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateFactories() throws IOException {
        charToAllButBoolean(TASK,
                "engine/chunk/src/main/java/io/deephaven/chunk/util/factories/CharChunkFactory.java");
        final String className = charToBoolean(TASK,
                "engine/chunk/src/main/java/io/deephaven/chunk/util/factories/CharChunkFactory.java");
        final File classFile = new File(className);
        List<String> classLines = FileUtils.readLines(classFile, Charset.defaultCharset());
        FileUtils.writeLines(classFile, classLines);
    }

    private static void replicateChunkFillers() throws IOException {
        charToAll(TASK, "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfillers/CharChunkFiller.java");
        replicateObjectChunkFiller();
    }

    private static void replicateObjectChunkFiller() throws IOException {
        final String className = charToObject(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfillers/CharChunkFiller.java");
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "ObjectChunk<\\?", "ObjectChunk<Object, ?",
                "src.getObject", "src.get",
                "src.getPrevObject", "src.getPrev");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateLongArraySource() throws IOException {
        final String className = charToLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/CharacterArraySource.java");
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = addImport(lines,
                "import io.deephaven.engine.table.impl.util.copy.CopyKernel;");
        lines = addImport(lines, LongFunction.class, ToLongFunction.class, Instant.class, ZonedDateTime.class,
                LocalDate.class, LocalTime.class, Require.class, ZoneId.class);
        lines = standardCleanups(lines);
        lines = globalReplacements(lines, "/\\*\\s*MIXIN_IMPLS\\s*\\*/", ", ConvertibleTimeSource");
        lines = replaceRegion(lines, "getAndAddUnsafe", Arrays.asList(
                "    public final long getAndAddUnsafe(long index, long addend) {",
                "        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);",
                "        final int indexWithinBlock = (int) (index & INDEX_MASK);",
                "        final long oldValue = blocks[blockIndex][indexWithinBlock];",
                "        if (addend != 0) {",
                "            if (shouldRecordPrevious(index, prevBlocks, recycler)) {",
                "                prevBlocks[blockIndex][indexWithinBlock] = oldValue;",
                "            }",
                "            blocks[blockIndex][indexWithinBlock] = oldValue + addend;",
                "        }",
                "        return oldValue;",
                "    }"));
        lines = replaceRegion(lines, "fillChunk", l -> addLongToBoxedAdapter(l, "LongFunction<R>",
                "WritableObjectChunk<R, ? super Values>", "asWritableObjectChunk"));
        lines = replaceRegion(lines, "fillPrevChunk", l -> addLongToBoxedAdapter(l, "LongFunction<R>",
                "WritableObjectChunk<R, ? super Values>", "asWritableObjectChunk"));
        lines = replaceRegion(lines, "fillSparseChunk", l -> addLongToBoxedAdapter(l, "LongFunction<R>",
                "WritableObjectChunk<R, ? super Values>", "asWritableObjectChunk"));
        lines = replaceRegion(lines, "fillSparsePrevChunk", l -> addLongToBoxedAdapter(l, "LongFunction<R>",
                "WritableObjectChunk<R, ? super Values>", "asWritableObjectChunk"));
        lines = replaceRegion(lines, "fillSparseChunkUnordered", l -> addLongToBoxedAdapter(l, "LongFunction<R>",
                "WritableObjectChunk<R, ? super Values>", "asWritableObjectChunk"));
        lines = replaceRegion(lines, "fillSparsePrevChunkUnordered", l -> addLongToBoxedAdapter(l, "LongFunction<R>",
                "WritableObjectChunk<R, ? super Values>", "asWritableObjectChunk"));
        lines = replaceRegion(lines, "copyFromChunk",
                l -> addLongToBoxedAdapter(l, "ToLongFunction<R>", "ObjectChunk<R, ? extends Values>",
                        "asObjectChunk"));
        lines = replaceRegion(lines, "fillFromChunkByRanges",
                l -> addLongToBoxedAdapter(l, "ToLongFunction<R>", "ObjectChunk<R, ? extends Values>",
                        "asObjectChunk"));
        lines = replaceRegion(lines, "fillFromChunkByKeys",
                l -> addLongToBoxedAdapter(l, "ToLongFunction<R>", "ObjectChunk<R, ? extends Values>",
                        "asObjectChunk"));
        lines = replaceRegion(lines, "fillFromChunkUnordered",
                l -> addLongToBoxedAdapter(l, "ToLongFunction<R>", "ObjectChunk<R, ? extends Values>",
                        "asObjectChunk"));
        lines = replaceRegion(lines, "reinterpretation", Arrays.asList(
                "    @Override",
                "    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        return alternateDataType == long.class || alternateDataType == Instant.class;",
                "    }",
                "",
                "    @SuppressWarnings(\"unchecked\")",
                "    @Override",
                "    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        if (alternateDataType == this.getType()) {",
                "            return (ColumnSource<ALTERNATE_DATA_TYPE>) this;",
                "        } else if (alternateDataType == Instant.class) {",
                "            return (ColumnSource<ALTERNATE_DATA_TYPE>) toInstant();",
                "        }",
                "",
                "        throw new IllegalArgumentException(\"Cannot reinterpret `\" + getType().getName() + \"` to `\" + alternateDataType.getName() + \"`\");",
                "    }",
                "",
                "    @Override",
                "    public boolean supportsTimeConversion() {",
                "        return true;",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {",
                "        return new ZonedDateTimeArraySource(Require.neqNull(zone, \"zone\"), this);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<LocalDate> toLocalDate(@NotNull final ZoneId zone) {",
                "        return new LongAsLocalDateColumnSource(this, zone);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<LocalTime> toLocalTime(@NotNull final ZoneId zone) {",
                "        return new LongAsLocalTimeColumnSource(this, zone);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<Instant> toInstant() {",
                "        return new InstantArraySource(this);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<Long> toEpochNano() {",
                "        return this;",
                "    }"));
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateLongSparseArraySource() throws IOException {
        final String className = charToLong(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/CharacterSparseArraySource.java");
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = addImport(lines, Instant.class, ZonedDateTime.class, LocalDate.class, LocalTime.class, Require.class,
                ZoneId.class);
        lines = addImport(lines, "import io.deephaven.engine.table.ColumnSource;");
        lines = globalReplacements(lines, "/\\*\\s*MIXIN_IMPLS\\s*\\*/", ", ConvertibleTimeSource");
        lines = replaceRegion(lines, "reinterpretation", Arrays.asList(
                "    @Override",
                "    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        return alternateDataType == long.class || alternateDataType == Instant.class;",
                "    }",
                "",
                "    @SuppressWarnings(\"unchecked\")",
                "    @Override",
                "    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        if (alternateDataType == this.getType()) {",
                "            return (ColumnSource<ALTERNATE_DATA_TYPE>) this;",
                "        } else if (alternateDataType == Instant.class) {",
                "            return (ColumnSource<ALTERNATE_DATA_TYPE>) toInstant();",
                "        }",
                "",
                "        throw new IllegalArgumentException(\"Cannot reinterpret `\" + getType().getName() + \"` to `\" + alternateDataType.getName() + \"`\");",
                "    }",
                "",
                "    @Override",
                "    public boolean supportsTimeConversion() {",
                "        return true;",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {",
                "        return new ZonedDateTimeSparseArraySource(Require.neqNull(zone, \"zone\"), this);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<LocalDate> toLocalDate(@NotNull final ZoneId zone) {",
                "        return new LongAsLocalDateColumnSource(this, zone);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<LocalTime> toLocalTime(@NotNull final ZoneId zone) {",
                "        return new LongAsLocalTimeColumnSource(this, zone);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<Instant> toInstant() {",
                "        return new InstantSparseArraySource(this);",
                "    }",
                "",
                "    @Override",
                "    public ColumnSource<Long> toEpochNano() {",
                "        return this;",
                "    }"));
        FileUtils.writeLines(classFile, lines);
    }

    private static List<String> addLongToBoxedAdapter(
            final List<String> inputLines, String convertType, String chunkClass, String chunkMethod) {
        List<String> permuted = globalReplacements(inputLines,
                "/\\*\\s*TYPE_MIXIN\\s*\\*/", "<R>",
                "/\\*\\s*CONVERTER\\s*\\*/", ", " + convertType + " converter",
                "/\\*\\s*CONVERTER_ARG\\s*\\*/", ", converter",
                "@Override", "",
                "NULL_LONG", "null");

        permuted = applyFixup(permuted, "chunkDecl", "^(\\s+)[^=]+=\\s+([^.]+)(.*)", (m) -> Collections.singletonList(
                m.group(1) + "final " + chunkClass + " chunk = " + m.group(2) + "." + chunkMethod + "();"));
        permuted = replaceRegion(permuted, "copyFromTypedArray", Arrays.asList(
                "                    for (int ii = 0; ii < length; ii++) {",
                "                        chunk.set(offset + ii, converter.apply(block[sIndexWithinBlock + ii]));",
                "                    }"));
        permuted = replaceRegion(permuted, "copyToTypedArray", Arrays.asList(
                "                for (int jj = 0; jj < length; jj++) {",
                "                    block[jj + sIndexWithinBlock] = converter.applyAsLong(chunk.get(offset + jj));",
                "                }"));
        permuted = replaceRegion(permuted, "copyFromTypedArrayImmutable", Arrays.asList(
                "           final int offset = destPosition.getAndAdd(length);",
                "           for (int ii = 0; ii < length; ii++) {",
                "               chunk.set(offset + ii, converter.apply(data[(int)start + ii]));",
                "           }"));
        permuted = replaceRegion(permuted, "copyToTypedArrayImmutable", Arrays.asList(
                "            final int offset = srcPos.getAndAdd(length);",
                "            for (int jj = 0; jj < length; jj++) {",
                "                data[(int)start + jj] = converter.applyAsLong(chunk.get(offset + jj));",
                "            }"));
        permuted = replaceRegion(permuted, "copyFromTypedArrayImmutable2D", Arrays.asList(
                "               final int destOffset = destPosition.getAndAdd(length);",
                "               for (int ii = 0; ii < length; ii++) {",
                "                   chunk.set(destOffset + ii, converter.apply(data[segment][offset + ii]));",
                "               }"));
        permuted = replaceRegion(permuted, "copyToTypedArrayImmutable2D", Arrays.asList(
                "               final int offset = srcPos.getAndAdd(length);",
                "               for (int jj = 0; jj < length; jj++) {",
                "                   data[segment][destOffset + jj] = converter.applyAsLong(chunk.get(offset + jj));",
                "               }"));
        permuted = replaceRegion(permuted, "conditionalCopy", Arrays.asList(
                "                final int chunkOffset = destOffset.get();",
                "                long[] baseInput = (long[]) getBlock(blockNo);",
                "                long[] overInput = (long[]) getPrevBlock(blockNo);",
                "",
                "                final int srcEndOffset = srcOffset + length;",
                "                int nextBit = CopyKernel.Utils.nextSetBit(inUse, srcOffset, srcEndOffset, false);",
                "",
                "                for (int ii = 0; ii < length; ++ii) {",
                "                    if (ii != nextBit - srcOffset) {",
                "                        chunk.set(ii + chunkOffset, converter.apply(baseInput[ii + srcOffset]));",
                "                    } else {",
                "                        nextBit = CopyKernel.Utils.nextSetBit(inUse, nextBit + 1, srcEndOffset, false);",
                "                        chunk.set(ii + chunkOffset, converter.apply(overInput[ii + srcOffset]));",
                "                    }",
                "                }"));
        permuted = applyFixup(permuted, "copyFromArray",
                "^(\\s+).+\\.copyFromArray\\(([^,]+), ([^,]+), ([^,]+), ([^)]+)\\).*", (m) -> Arrays.asList(
                        m.group(1) + "{",
                        m.group(1) + "    long[] block = (long[])" + m.group(2) + ";",
                        m.group(1) + "    for (int ii = 0; ii < " + m.group(5) + "; ii++) {",
                        m.group(1) + "        chunk.set(ii + " + m.group(4) + ", converter.apply(block[ii + "
                                + m.group(3) + "]));",
                        m.group(1) + "    }",
                        m.group(1) + "}"));
        permuted = applyFixup(permuted, "conversion", "^(\\s+chunk\\.set\\([^,]+,)([^;]+);",
                (m) -> Collections.singletonList(m.group(1) + "converter.apply(" + m.group(2) + ");"));
        permuted = applyFixup(permuted, "conversion", "^([^=]+=\\s+)([^;]+);",
                (m) -> Collections.singletonList(m.group(1) + "converter.applyAsLong(" + m.group(2) + ");"));
        permuted = applyFixup(permuted, "conversion", "^(\\s+set\\([^,]+,)([^;]+);",
                (m) -> Collections.singletonList(m.group(1) + "converter.applyAsLong(" + m.group(2) + ");"));

        // Stick them together.
        return Stream.concat(inputLines.stream(), permuted.stream()).collect(Collectors.toList());
    }

    private static List<String> expandDowncast(final List<String> lines, final String classname) {
        final String[] template = {
                "    public <T_DERIV extends T> $TYPE$<T_DERIV, ATTR> asTyped$TYPE$() {",
                "        //noinspection unchecked",
                "        return ($TYPE$<T_DERIV, ATTR>) this;",
                "    }",
                ""
        };
        for (int i = 0; i < template.length; ++i) {
            template[i] = template[i].replace("$TYPE$", classname);
        }
        return insertRegion(lines, "downcast", Arrays.asList(template));
    }
}
