/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateSourcesAndChunks {

    public static void main(String... args) throws IOException {
        replicateSparseArraySources();

        replicateSingleValues();
        charToAllButBooleanAndLong(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/CharacterArraySource.java");
        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/aggregate/CharAggregateColumnSource.java");
        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/UngroupedCharArrayColumnSource.java");
        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/UngroupedCharVectorColumnSource.java");
        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/UngroupedBoxedCharObjectVectorColumnSource.java");
        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/UngroupedBoxedCharArrayColumnSource.java");

        charToAllButBooleanAndLong(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/ImmutableCharArraySource.java");
        fixupLongReinterpret(charToLong(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/ImmutableCharArraySource.java"));
        fixupByteReinterpret(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/ImmutableByteArraySource.java");
        replicateObjectImmutableArraySource();

        charToAllButBooleanAndLong(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/Immutable2DCharArraySource.java");
        fixupLongReinterpret(charToLong(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/Immutable2DCharArraySource.java"));
        fixupByteReinterpret(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/Immutable2DByteArraySource.java");
        replicateObjectImmutable2DArraySource();

        charToAllButBooleanAndLong(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/ImmutableConstantCharSource.java");
        fixupLongReinterpret(charToLong(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/ImmutableConstantCharSource.java"));
        fixupByteReinterpret(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/immutable/ImmutableConstantByteSource.java");
        replicateImmutableConstantObjectSource();

        charToAll("engine/chunk/src/main/java/io/deephaven/chunk/sized/SizedCharChunk.java");
        replicateObjectSizedChunk();

        replicateChunks();
        replicateWritableChunks();

        replicateChunkChunks();
        replicateWritableChunkChunks();

        replicateResettableChunks();
        replicateResettableChunkChunks();

        replicateResettableWritableChunks();
        replicateResettableWritableChunkChunks();

        replicateFactories();
        charToAll("engine/chunk/src/main/java/io/deephaven/chunk/util/pools/CharChunkPool.java");

        replicateChunkFillers();

        charToAll("engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfillers/CharChunkFiller.java");

        replicateChunkColumnSource();
    }

    private static void replicateObjectSizedChunk() throws IOException {
        String path = ReplicatePrimitiveCode
                .charToObject("engine/chunk/src/main/java/io/deephaven/chunk/sized/SizedCharChunk.java");
        final File classFile = new File(path);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "<T> the chunk's attribute", "<ATTR> the chunk's attribute",
                "SizedObjectChunk<T extends Any>", "SizedObjectChunk<T, ATTR extends Any>",
                "WritableObjectChunk<T>", "WritableObjectChunk<T, ATTR>");
        FileUtils.writeLines(classFile, lines);
    }

    private static void fixupLongReinterpret(String longImmutableSource) throws IOException {
        final File resultClassJavaFile = new File(longImmutableSource);
        List<String> lines = FileUtils.readLines(resultClassJavaFile, Charset.defaultCharset());
        lines = addImport(lines, "import io.deephaven.time.DateTime;");
        lines = addImport(lines, "import io.deephaven.engine.table.ColumnSource;");
        lines = replaceRegion(lines, "reinterpret", Arrays.asList("    @Override",
                "    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(",
                "            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        return alternateDataType == DateTime.class;",
                "    }",
                "",
                "    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(",
                "               @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "         //noinspection unchecked",
                "         return (ColumnSource<ALTERNATE_DATA_TYPE>) new LongAsDateTimeColumnSource(this);",
                "    }"));
        FileUtils.writeLines(resultClassJavaFile, lines);
    }

    private static void fixupByteReinterpret(String byteImmutableSource) throws IOException {
        final File resultClassJavaFile = new File(byteImmutableSource);
        List<String> lines = FileUtils.readLines(resultClassJavaFile, Charset.defaultCharset());
        lines = addImport(lines, "import io.deephaven.engine.table.ColumnSource;");
        lines = replaceRegion(lines, "reinterpret", Arrays.asList("    @Override",
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
        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/CharacterSingleValueSource.java");
        replicateObjectSingleValue();
    }

    private static void replicateObjectSingleValue() throws IOException {
        final String resultClassJavaPath = charToObject(
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
                "set\\(Object", "set(T",
                "set\\(long key, Object", "set(long key, T",
                "set\\(NULL_OBJECT", "set(null",
                "final ObjectChunk<[?] extends Values>", "final ObjectChunk<T, ? extends Values>",
                "unbox\\((.*)\\)", "$1");
        lines = ReplicationUtils.removeRegion(lines, "UnboxedSetter");
        lines = ReplicationUtils.replaceRegion(lines, "Constructor", Arrays.asList(
                "    public ObjectSingleValueSource(Class<T> type) {",
                "        super(type);",
                "        current = null;",
                "        prev = null;",
                "    }"));
        FileUtils.writeLines(resultClassJavaFile, lines);
    }

    private static void replicateChunkColumnSource() throws IOException {
        charToAllButBoolean(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/chunkcolumnsource/CharChunkColumnSource.java");
        replicateObjectChunkColumnSource();
    }

    private static void replicateObjectChunkColumnSource() throws IOException {
        final String resultClassJavaPath = charToObject(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/chunkcolumnsource/CharChunkColumnSource.java");
        final File resultClassJavaFile = new File(resultClassJavaPath);
        List<String> lines = FileUtils.readLines(resultClassJavaFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "class ObjectChunkColumnSource", "class ObjectChunkColumnSource<T>");
        lines = genericObjectColumnSourceReplacements(lines);

        lines = ReplicationUtils.replaceRegion(lines, "constructor", Arrays.asList(
                "    protected ObjectChunkColumnSource(Class<T> type, Class<?> componentType) {",
                "        this(type, componentType, new TLongArrayList());",
                "    }",
                "",
                "    protected ObjectChunkColumnSource(Class<T> type, Class<?> componentType, final TLongArrayList firstOffsetForData) {",
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
        final String resultClassJavaPath = charToObject(
                immutableSourcePath);
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

    private static void replicateSparseArraySources() throws IOException {
        replicateOneOrN();

        charToAllButBooleanAndLong(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/CharacterSparseArraySource.java");
        replicateSparseLongSource();

        replicateSparseBooleanSource();
        replicateSparseObjectSource();
    }

    private static void replicateChunks() throws IOException {
        charToAllButBooleanAndByte("engine/chunk/src/main/java/io/deephaven/chunk/CharChunk.java");
        replicateByteChunks();
        replicateBooleanChunks();
        replicateObjectChunks();
    }

    private static void replicateByteChunks() throws IOException {
        final String className = charToByte("engine/chunk/src/main/java/io/deephaven/chunk/CharChunk.java");
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
        final String className = charToBoolean("engine/chunk/src/main/java/io/deephaven/chunk/CharChunk.java");
        final File classFile = new File(className);
        List<String> classLines = FileUtils.readLines(classFile, Charset.defaultCharset());
        classLines = ReplicationUtils.removeRegion(classLines, "BufferImports");
        classLines = ReplicationUtils.removeRegion(classLines, "CopyToBuffer");
        FileUtils.writeLines(classFile, classLines);
    }

    private static void replicateObjectChunks() throws IOException {
        final String className = charToObject("engine/chunk/src/main/java/io/deephaven/chunk/CharChunk.java");
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
                "static T\\[\\] makeArray", "static <T> T[] makeArray");

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
        charToAll("engine/chunk/src/main/java/io/deephaven/chunk/CharChunkChunk.java");
        replicateObjectChunkChunks();
    }

    private static void replicateObjectChunkChunks() throws IOException {
        final String className = charToObject(
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
                charToAllButBoolean("engine/chunk/src/main/java/io/deephaven/chunk/WritableCharChunk.java");
        for (String fileName : files) {
            final File classFile = new File(fileName);
            List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
            lines = ReplicationUtils.removeRegion(lines, "SortFixup");
            FileUtils.writeLines(classFile, lines);
        }
        replicateWritableBooleanChunks();
        replicateWritableObjectChunks();
    }

    private static void replicateWritableBooleanChunks() throws IOException {
        final String writableBooleanChunkClassName = charToBoolean(
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
        final String className = charToObject(
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
                "    @Override\n" +
                        "    public final void fillWithBoxedValue(int offset, int size, Object value) {\n" +
                        "        fillWithValue(offset,size, (T)value);\n" +
                        "    }"));
        lines = ReplicationUtils.addImport(lines,
                "import io.deephaven.util.compare.ObjectComparisons;",
                "import java.util.Comparator;");
        lines = ReplicationUtils.replaceRegion(lines, "sort", Arrays.asList(
                "    private static final Comparator<Comparable<Object>> COMPARATOR = Comparator.nullsFirst(Comparator.naturalOrder());",
                "",
                "    @Override",
                "    public final void sort(int start, int length) {",
                "        //noinspection unchecked",
                "        Arrays.sort(data, offset + start, offset + start + length, (Comparator<? super T>) COMPARATOR);",
                "    }"));
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateWritableChunkChunks() throws IOException {
        charToAllButBoolean("engine/chunk/src/main/java/io/deephaven/chunk/WritableCharChunkChunk.java");
        replicateWritableBooleanChunkChunks();
        replicateWritableObjectChunkChunks();
    }

    private static void replicateWritableBooleanChunkChunks() throws IOException {
        final String writableBooleanChunkClassName = charToBoolean(
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
        final String className = charToObject(
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
        charToAll("engine/chunk/src/main/java/io/deephaven/chunk/ResettableCharChunk.java");
        replicateResettableObjectChunks();
    }

    private static void replicateResettableObjectChunks() throws IOException {
        final String className = charToObject(
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
        charToAll("engine/chunk/src/main/java/io/deephaven/chunk/ResettableWritableCharChunk.java");
        replicateResettableWritableObjectChunks();
    }

    private static void replicateResettableWritableObjectChunks() throws IOException {
        final String className = charToObject(
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
        charToAll("engine/chunk/src/main/java/io/deephaven/chunk/ResettableWritableCharChunkChunk.java");
        replicateResettableWritableObjectChunkChunks();
    }

    private static void replicateResettableWritableObjectChunkChunks() throws IOException {
        final String className = charToObject(
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
        charToAll("engine/chunk/src/main/java/io/deephaven/chunk/ResettableCharChunkChunk.java");
        replicateResettableObjectChunkChunks();
    }

    private static void replicateResettableObjectChunkChunks() throws IOException {
        final String className = charToObject(
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
        charToAllButBoolean(
                "engine/chunk/src/main/java/io/deephaven/chunk/util/factories/CharChunkFactory.java");
        final String className = charToBoolean(
                "engine/chunk/src/main/java/io/deephaven/chunk/util/factories/CharChunkFactory.java");
        final File classFile = new File(className);
        List<String> classLines = FileUtils.readLines(classFile, Charset.defaultCharset());
        FileUtils.writeLines(classFile, classLines);
    }

    private static void replicateChunkFillers() throws IOException {
        charToAll("engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfillers/CharChunkFiller.java");
        replicateObjectChunkFiller();
    }

    private static void replicateObjectChunkFiller() throws IOException {
        final String className = charToObject(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/chunkfillers/CharChunkFiller.java");
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "ObjectChunk<\\?", "ObjectChunk<Object, ?",
                "src.getObject", "src.get",
                "src.getPrevObject", "src.getPrev");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateSparseLongSource() throws IOException {
        final File longSparseArraySourceFile = new File(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/LongSparseArraySource.java");
        final File abstractLongSparseArraySourceFile = new File(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/AbstractSparseLongArraySource.java");


        final String longSparseCode = FileUtils.readFileToString(longSparseArraySourceFile, Charset.defaultCharset());
        charToLong("engine/table/src/main/java/io/deephaven/engine/table/impl/sources/CharacterSparseArraySource.java",
                Collections.emptyMap());

        Files.delete(abstractLongSparseArraySourceFile.toPath());
        Files.move(longSparseArraySourceFile.toPath(), abstractLongSparseArraySourceFile.toPath());

        FileUtils.writeStringToFile(longSparseArraySourceFile, longSparseCode, Charset.defaultCharset());

        List<String> abstractLines = FileUtils.readLines(abstractLongSparseArraySourceFile, Charset.defaultCharset());
        abstractLines = globalReplacements(abstractLines, "LongSparseArraySource", "AbstractSparseLongArraySource",
                "public class AbstractSparseLongArraySource extends SparseArrayColumnSource<Long> implements MutableColumnSourceGetDefaults.ForLong",
                "abstract public class AbstractSparseLongArraySource<T> extends SparseArrayColumnSource<T> implements MutableColumnSourceGetDefaults.LongBacked<T>",
                "ColumnSource<Long>", "ColumnSource<T>");
        abstractLines = replaceRegion(abstractLines, "constructor", Arrays.asList(
                "    AbstractSparseLongArraySource(Class<T> type) {",
                "        super(type);",
                "        blocks = new LongOneOrN.Block0();", "    }"));
        abstractLines = replaceRegion(abstractLines, "boxed methods", Collections.emptyList());
        abstractLines = replaceRegion(abstractLines, "copy method", Collections.emptyList());
        abstractLines = simpleFixup(abstractLines, "getChunk", "LongChunk<Values> getChunk", "Chunk<Values> getChunk");
        abstractLines = simpleFixup(abstractLines, "getPrevChunk", "LongChunk<Values> getPrevChunk",
                "Chunk<Values> getPrevChunk");
        abstractLines = standardCleanups(abstractLines);

        FileUtils.writeLines(abstractLongSparseArraySourceFile, abstractLines);
    }

    private static void replicateSparseBooleanSource() throws IOException {
        final String booleanPath = charToBooleanAsByte(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/CharacterSparseArraySource.java",
                Collections.emptyMap());
        final File booleanFile = new File(booleanPath);
        List<String> lines = FileUtils.readLines(booleanFile, Charset.defaultCharset());

        lines = addImport(lines,
                "import io.deephaven.engine.table.impl.AbstractColumnSource;",
                "import io.deephaven.engine.table.WritableColumnSource;",
                "import io.deephaven.util.BooleanUtils;",
                "import static io.deephaven.util.BooleanUtils.NULL_BOOLEAN_AS_BYTE;");
        lines = globalReplacements(lines, "BooleanOneOrN", "ByteOneOrN");
        lines = globalReplacements(lines, "WritableBooleanChunk", "WritableObjectChunk",
                "asBooleanChunk", "asObjectChunk",
                "BooleanChunk<Values>", "ObjectChunk<Boolean, Values>",
                "ObjectChunk<Values>", "ObjectChunk<Boolean, Values>",
                "BooleanChunk<[?] extends Values>", "ObjectChunk<Boolean, ? extends Values>",
                "ObjectChunk<[?] extends Values>", "ObjectChunk<Boolean, ? extends Values>",
                "BooleanChunk<[?] super Values>", "ObjectChunk<Boolean, ? super Values>",
                "ObjectChunk<[?] super Values>", "ObjectChunk<Boolean, ? super Values>");
        lines = simpleFixup(lines, "primitive get", "NULL_BOOLEAN", "NULL_BOOLEAN_AS_BYTE", "getBoolean", "getByte",
                "getPrevBoolean", "getPrevByte");
        lines = simpleFixup(lines, "setNull", "NULL_BOOLEAN", "NULL_BOOLEAN_AS_BYTE");

        lines = replaceRegion(lines, "copyFromTypedArray", Arrays.asList(
                "                    for (int jj = 0; jj < length; ++jj) {",
                "                         chunk.set(jj + ctx.offset, BooleanUtils.byteAsBoolean(ctx.block[sIndexWithinBlock + jj]));",
                "                    }"));
        lines = replaceRegion(lines, "copyToTypedArray", Arrays.asList(
                "                for (int jj = 0; jj < length; ++jj) {",
                "                    block[sIndexWithinBlock + jj] = BooleanUtils.booleanAsByte(chunk.get(offset + jj));",
                "                }"));

        lines = applyFixup(lines, "fillByKeys", "(.*chunk.set\\(.*, )(ctx\\.block.*)(\\);.*)", m -> Collections
                .singletonList(m.group(1) + "BooleanUtils.byteAsBoolean(" + m.group(2) + ")" + m.group(3)));
        lines = applyFixup(lines, "fillByUnRowSequence", "(.*byteChunk.set\\(.*, )(block.*)(\\);.*)", m -> Collections
                .singletonList(m.group(1) + "BooleanUtils.byteAsBoolean(" + m.group(2) + ")" + m.group(3)));
        lines = applyFixup(lines, "fillFromChunkByKeys", "(.*)(chunk.get\\(.*\\));",
                m -> Collections.singletonList(m.group(1) + "BooleanUtils.booleanAsByte(" + m.group(2) + ");"));
        lines = applyFixup(lines, "fillFromChunkUnordered", "(.*)(chunk.get\\(.*\\));",
                m -> Collections.singletonList(m.group(1) + "BooleanUtils.booleanAsByte(" + m.group(2) + ");"));

        lines = simpleFixup(lines, "allocateNullFilledBlock", "NULL_BOOLEAN", "NULL_BOOLEAN_AS_BYTE");
        lines = simpleFixup(lines, "boxed methods", "box\\(getBoolean\\(", "BooleanUtils.byteAsBoolean(getByte(",
                "box\\(getPrevBoolean\\(", "BooleanUtils.byteAsBoolean(getPrevByte(");
        lines = simpleFixup(lines, "boxed methods", "unbox", "BooleanUtils.booleanAsByte");
        lines = applyFixup(lines, "constructor", "(.*super\\().*(\\);.*)",
                m -> Collections.singletonList(m.group(1) + "Boolean.class" + m.group(2)));

        lines = removeRegion(removeRegion(lines, "getChunk"), "getPrevChunk");
        lines = replaceRegion(lines, "getChunk", Arrays.asList(
                "    @Override",
                "    public ObjectChunk<Boolean, Values> getChunk(@NotNull GetContext context, @NotNull RowSequence RowSequence) {",
                "        return getChunkByFilling(context, RowSequence).asObjectChunk();",
                "    }"));
        lines = replaceRegion(lines, "getPrevChunk", Arrays.asList(
                "    @Override",
                "    public ObjectChunk<Boolean, Values> getPrevChunk(@NotNull GetContext context, @NotNull RowSequence RowSequence) {",
                "        return getPrevChunkByFilling(context, RowSequence).asObjectChunk();",
                "    }"));

        lines = simpleFixup(lines, "fillByUnRowSequence", "WritableObjectChunk byteChunk",
                "WritableObjectChunk<Boolean, ? super Values> byteChunk");
        lines = simpleFixup(lines, "fillByUnRowSequence", "byteChunk", "booleanObjectChunk");
        lines = simpleFixup(lines, "fillByUnRowSequence",
                "BooleanUtils\\.byteAsBoolean\\(blockToUse == null \\? NULL_BOOLEAN : blockToUse\\[indexWithinBlock\\]\\)",
                "blockToUse == null ? NULL_BOOLEAN : BooleanUtils.byteAsBoolean(blockToUse[indexWithinBlock])");

        // AND SO IT BEGINS
        lines = replaceRegion(lines, "reinterpretation", Arrays.asList(
                "    @Override",
                "    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        return alternateDataType.equals(byte.class);",
                "    }",
                "",
                "    @Override",
                "    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "        //noinspection unchecked",
                "        return (ColumnSource<ALTERNATE_DATA_TYPE>) new BooleanSparseArraySource.ReinterpretedAsByte(this);",
                "    }",
                "",
                "    public static class ReinterpretedAsByte extends AbstractColumnSource<Byte> implements MutableColumnSourceGetDefaults.ForByte, FillUnordered, WritableColumnSource<Byte> {",
                "        private final BooleanSparseArraySource wrapped;",
                "",
                "        private ReinterpretedAsByte(BooleanSparseArraySource wrapped) {",
                "            super(byte.class);",
                "            this.wrapped = wrapped;",
                "        }",
                "",
                "        public void shift(final RowSet keysToShift, final long shiftDelta) {",
                "            this.wrapped.shift(keysToShift, shiftDelta);",
                "        }",
                "",
                "        @Override",
                "        public void startTrackingPrevValues() {",
                "            wrapped.startTrackingPrevValues();",
                "        }",
                "",
                "        @Override",
                "        public byte getByte(long rowKey) {",
                "            return wrapped.getByte(rowKey);",
                "        }",
                "",
                "        @Override",
                "        public byte getPrevByte(long rowKey) {",
                "            return wrapped.getPrevByte(rowKey);",
                "        }",
                "",
                "        @Override",
                "        public void setNull(long key) {",
                "            wrapped.setNull(key);",
                "        }",
                "",
                "        @Override",
                "        public void set(long key, Byte value) {",
                "            wrapped.set(key, value);",
                "        }",
                "",
                "        @Override",
                "        public void set(long key, byte value) {",
                "            wrapped.set(key, value);",
                "        }",
                "",
                "        @Override",
                "        public void ensureCapacity(long capacity, boolean nullFilled) {",
                "            wrapped.ensureCapacity(capacity, nullFilled);",
                "        }",
                "",
                "        @Override",
                "        public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "            return alternateDataType == Boolean.class;",
                "        }",
                "",
                "        @Override",
                "        protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {",
                "            // noinspection unchecked",
                "            return (ColumnSource<ALTERNATE_DATA_TYPE>)wrapped;",
                "        }",
                "",
                "        @Override",
                "        public void fillChunk(@NotNull final ColumnSource.FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence RowSequence) {",
                "            fillSparseChunk(destination, RowSequence);",
                "        }",
                "",
                "        @Override",
                "        public void fillPrevChunk(@NotNull final ColumnSource.FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence RowSequence) {",
                "            fillSparsePrevChunk(destination, RowSequence);",
                "        }",
                "",
                "        @Override",
                "        public void fillChunkUnordered(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final LongChunk<? extends RowKeys> keyIndices) {",
                "            fillSparseChunkUnordered(destination, keyIndices);",
                "        }",
                "",
                "        @Override",
                "        public void fillPrevChunkUnordered(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final LongChunk<? extends RowKeys> keyIndices) {",
                "            fillSparsePrevChunkUnordered(destination, keyIndices);",
                "        }",
                "",
                "        private void fillSparseChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final RowSequence indices) {",
                "            if (indices.size() == 0) {",
                "                destGeneric.setSize(0);",
                "                return;",
                "            }",
                "            // This implementation is in \"key\" style (rather than range style).",
                "            final WritableByteChunk<? super Values> chunk = destGeneric.asWritableByteChunk();",
                "            final FillByContext<byte[]> ctx = new FillByContext<>();",
                "            indices.forEachRowKey((final long v) -> {",
                "                if (v > ctx.maxKeyInCurrentBlock) {",
                "                    ctx.block = wrapped.blocks.getInnermostBlockByKeyOrNull(v);",
                "                    ctx.maxKeyInCurrentBlock = v | INDEX_MASK;",
                "                }",
                "                if (ctx.block == null) {",
                "                    chunk.fillWithNullValue(ctx.offset, 1);",
                "                } else {",
                "                    chunk.set(ctx.offset, ctx.block[(int) (v & INDEX_MASK)]);",
                "                }",
                "                ++ctx.offset;",
                "                return true;",
                "            });",
                "            destGeneric.setSize(ctx.offset);",
                "        }",
                "",
                "        private void fillSparsePrevChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final RowSequence indices) {",
                "            final long sz = indices.size();",
                "            if (sz == 0) {",
                "                destGeneric.setSize(0);",
                "                return;",
                "            }",
                "",
                "            if (wrapped.prevFlusher == null) {",
                "                fillSparseChunk(destGeneric, indices);",
                "                return;",
                "            }",
                "            fillSparsePrevChunkUnordered(destGeneric, indices.asRowKeyChunk());",
                "        }",
                "",
                "        private void fillSparseChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices) {",
                "            final WritableByteChunk<? super Values> chunk = destGeneric.asWritableByteChunk();",
                "            // This implementation is in \"key\" style (rather than range style).",
                "            for (int ii = 0; ii < indices.size(); ) {",
                "                final long firstRowKey = indices.get(ii);",
                "                if (firstRowKey == RowSet.NULL_ROW_KEY) {",
                "                    chunk.set(ii++, NULL_BOOLEAN_AS_BYTE);",
                "                    continue;",
                "                }",
                "                final long masked = firstRowKey & ~INDEX_MASK;",
                "                int lastII = ii;",
                "                while (lastII + 1 < indices.size()) {",
                "                    final int nextII = lastII + 1;",
                "                    final long nextKey = indices.get(nextII);",
                "                    final long nextMasked = nextKey & ~INDEX_MASK;",
                "                    if (nextMasked != masked) {",
                "                        break;",
                "                    }",
                "                    lastII = nextII;",
                "                }",
                "                final byte [] block = wrapped.blocks.getInnermostBlockByKeyOrNull(firstRowKey);",
                "                if (block == null) {",
                "                    chunk.fillWithNullValue(ii, lastII - ii + 1);",
                "                    ii = lastII + 1;",
                "                    continue;",
                "                }",
                "                while (ii <= lastII) {",
                "                    final int indexWithinBlock = (int) (indices.get(ii) & INDEX_MASK);",
                "                    chunk.set(ii++, block[indexWithinBlock]);",
                "                }",
                "            }",
                "            destGeneric.setSize(indices.size());",
                "        }",
                "",
                "        private void fillSparsePrevChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices) {",
                "            final WritableByteChunk<? super Values> booleanObjectChunk = destGeneric.asWritableByteChunk();",
                "            for (int ii = 0; ii < indices.size(); ) {",
                "                final long firstRowKey = indices.get(ii);",
                "                if (firstRowKey == RowSet.NULL_ROW_KEY) {",
                "                    booleanObjectChunk.set(ii++, NULL_BOOLEAN_AS_BYTE);",
                "                    continue;",
                "                }",
                "                final long masked = firstRowKey & ~INDEX_MASK;",
                "                int lastII = ii;",
                "                while (lastII + 1 < indices.size()) {",
                "                    final int nextII = lastII + 1;",
                "                    final long nextKey = indices.get(nextII);",
                "                    final long nextMasked = nextKey & ~INDEX_MASK;",
                "                    if (nextMasked != masked) {",
                "                        break;",
                "                    }",
                "                    lastII = nextII;",
                "                }",
                "",
                "                final byte [] block = wrapped.blocks.getInnermostBlockByKeyOrNull(firstRowKey);",
                "                if (block == null) {",
                "                    booleanObjectChunk.fillWithNullValue(ii, lastII - ii + 1);",
                "                    ii = lastII + 1;",
                "                    continue;",
                "                }",
                "",
                "                final long [] prevInUse = (wrapped.prevFlusher == null || wrapped.prevInUse == null) ? null :",
                "                        wrapped.prevInUse.getInnermostBlockByKeyOrNull(firstRowKey);",
                "                final byte [] prevBlock = prevInUse == null ? null : wrapped.prevBlocks.getInnermostBlockByKeyOrNull(firstRowKey);",
                "                while (ii <= lastII) {",
                "                    final int indexWithinBlock = (int) (indices.get(ii) & INDEX_MASK);",
                "                    final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;",
                "                    final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);",
                "",
                "                    final byte[] blockToUse = (prevInUse != null && (prevInUse[indexWithinInUse] & maskWithinInUse) != 0) ? prevBlock : block;",
                "                    booleanObjectChunk.set(ii++, blockToUse == null ? NULL_BOOLEAN_AS_BYTE : blockToUse[indexWithinBlock]);",
                "                }",
                "            }",
                "            destGeneric.setSize(indices.size());",
                "        }",
                "",
                "        @Override",
                "        public boolean providesFillUnordered() {",
                "            return true;",
                "        }",
                "",
                "        @Override",
                "        public void fillFromChunk(@NotNull FillFromContext context_unused, @NotNull Chunk<? extends Values> src, @NotNull RowSequence RowSequence) {",
                "            // This implementation is in \"key\" style (rather than range style).",
                "            if (RowSequence.size() == 0) {",
                "                return;",
                "            }",
                "            final ByteChunk<? extends Values> chunk = src.asByteChunk();",
                "            final LongChunk<OrderedRowKeys> keys = RowSequence.asRowKeyChunk();",
                "",
                "            final boolean hasPrev = wrapped.prevFlusher != null;",
                "",
                "            if (hasPrev) {",
                "                wrapped.prevFlusher.maybeActivate();",
                "            }",
                "",
                "            for (int ii = 0; ii < keys.size(); ) {",
                "                final long firstRowKey = keys.get(ii);",
                "                final long maxKeyInCurrentBlock = firstRowKey | INDEX_MASK;",
                "                int lastII = ii;",
                "                while (lastII + 1 < keys.size() && keys.get(lastII + 1) <= maxKeyInCurrentBlock) {",
                "                    ++lastII;",
                "                }",
                "",
                "                final int block0 = (int) (firstRowKey >> BLOCK0_SHIFT) & BLOCK0_MASK;",
                "                final int block1 = (int) (firstRowKey >> BLOCK1_SHIFT) & BLOCK1_MASK;",
                "                final int block2 = (int) (firstRowKey >> BLOCK2_SHIFT) & BLOCK2_MASK;",
                "                final byte [] block = wrapped.ensureBlock(block0, block1, block2);",
                "",
                "                if (chunk.isAlias(block)) {",
                "                    throw new UnsupportedOperationException(\"Source chunk is an alias for target data\");",
                "                }",
                "",
                "                // This conditional with its constant condition should be very friendly to the branch predictor.",
                "                final byte[] prevBlock = hasPrev ? wrapped.ensurePrevBlock(firstRowKey, block0, block1, block2) : null;",
                "                final long[] inUse = hasPrev ? wrapped.prevInUse.get(block0).get(block1).get(block2) : null;",
                "",
                "                while (ii <= lastII) {",
                "                    final int indexWithinBlock = (int) (keys.get(ii) & INDEX_MASK);",
                "                    // This 'if' with its constant condition should be very friendly to the branch predictor.",
                "                    if (hasPrev) {",
                "                        assert inUse != null;",
                "                        assert prevBlock != null;",
                "",
                "                        final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;",
                "                        final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);",
                "",
                "                        if ((inUse[indexWithinInUse] & maskWithinInUse) == 0) {",
                "                            prevBlock[indexWithinBlock] = block[indexWithinBlock];",
                "                            inUse[indexWithinInUse] |= maskWithinInUse;",
                "                        }",
                "                    }",
                "                    block[indexWithinBlock] = chunk.get(ii);",
                "                    ++ii;",
                "                }",
                "            }",
                "        }",
                "    }"));
        FileUtils.writeLines(booleanFile, lines);
    }

    private static void replicateSparseObjectSource() throws IOException {
        final String objectPath = charToObject(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/CharacterSparseArraySource.java");
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());

        lines = removeRegion(lines, "boxed methods");
        lines = removeRegion(lines, "boxing imports");

        lines = globalReplacements(lines, "ObjectOneOrN.Block([0-2])", "ObjectOneOrN.Block$1<T>");

        lines = globalReplacements(lines,
                "public class ObjectSparseArraySource extends SparseArrayColumnSource<Object> implements MutableColumnSourceGetDefaults.ForObject",
                "public class ObjectSparseArraySource<T> extends SparseArrayColumnSource<T> implements MutableColumnSourceGetDefaults.ForObject<T>",
                "Object[ ]?\\[\\]", "T []",
                "NULL_OBJECT", "null",
                "getObject", "get",
                "getPrevObject", "getPrev",
                "ColumnSource<Object>", "ColumnSource<T>",
                "ObjectChunk<Values>", "ObjectChunk<T, Values>",
                "ObjectChunk<[?] super Values>", "ObjectChunk<T, ? super Values>",
                "ObjectChunk<[?] extends Values>", "ObjectChunk<T, ? extends Values>",
                "Object get", "T get",
                "recycler.borrowItem\\(\\)", "(T[])recycler.borrowItem()",
                "recycler2.borrowItem\\(\\)", "(T[][])recycler2.borrowItem()",
                "recycler1.borrowItem\\(\\)", "(T[][][])recycler1.borrowItem()",
                "recycler0.borrowItem\\(\\)", "(T[][][][])recycler0.borrowItem()",
                "public final void set\\(long key, Object value\\) \\{", "public final void set(long key, T value) {");

        lines = replaceRegion(lines, "recyclers", Arrays.asList(
                "    private static final SoftRecycler recycler = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,",
                "            () -> new Object[BLOCK_SIZE], block -> Arrays.fill(block, null)); // we'll hold onto previous values, fix that",
                "    private static final SoftRecycler recycler2 = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,",
                "            () -> new Object[BLOCK2_SIZE][], null);",
                "    private static final SoftRecycler recycler1 = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,",
                "            () -> new ObjectOneOrN.Block2[BLOCK1_SIZE], null);",
                "    private static final SoftRecycler recycler0 = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,",
                "            () -> new ObjectOneOrN.Block1[BLOCK0_SIZE], null);"));

        lines = replaceRegion(lines, "constructor", Arrays.asList(
                "",
                "    public ObjectSparseArraySource(Class<T> type) {",
                "        super(type);",
                "        blocks = new ObjectOneOrN.Block0<>();",
                "    }",
                "",
                "    ObjectSparseArraySource(Class<T> type, Class componentType) {",
                "        super(type, componentType);",
                "        blocks = new ObjectOneOrN.Block0<>();",
                "    }"));

        lines = replaceRegion(lines, "move method", Arrays.asList(
                "    @Override",
                "    public void move(long sourceKey, long destKey) {",
                "        final T value = get(sourceKey);",
                "        set(destKey, value);",
                "        if (value != null) {",
                "            set(sourceKey, null);",
                "        }",
                "    }"));

        lines = replaceRegion(lines, "allocateNullFilledBlock", Arrays.asList(
                "    final T[] allocateNullFilledBlock(int size){",
                "        //noinspection unchecked",
                "        return (T[]) new Object[size];",
                "    }"));

        FileUtils.writeLines(objectFile, lines);
    }

    private static void replicateOneOrN() throws IOException {
        charToAll("engine/table/src/main/java/io/deephaven/engine/table/impl/sources/sparse/CharOneOrN.java");
        final String objectOneOrNPath = charToObject(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/sources/sparse/CharOneOrN.java");
        final File oneOrNFile = new File(objectOneOrNPath);
        List<String> lines = FileUtils.readLines(oneOrNFile, Charset.defaultCharset());

        lines = globalReplacements(lines,
                "class Block([0-2])", "class Block$1<T>",
                "Object \\[\\]", "T []", "SoftRecycler<Object", "SoftRecycler<T",
                "QueryConstants.NULL_OBJECT", "null",
                "new Object\\[BLOCK2_SIZE\\]\\[\\];",
                "(T[][])new Object[BLOCK2_SIZE][];");

        lines = simpleFixup(lines, "Block0", "Block1", "Block1<T>", "Block2", "Block2<T>");
        lines = simpleFixup(lines, "Block1", "Block2", "Block2<T>");

        lines = globalReplacements(lines,
                "new Block2<T>\\[BLOCK1_SIZE\\]", "new Block2[BLOCK1_SIZE]",
                "new Block1<T>\\[BLOCK0_SIZE\\]", "new Block1[BLOCK0_SIZE]");

        FileUtils.writeLines(oneOrNFile, lines);
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
