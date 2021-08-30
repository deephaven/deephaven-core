/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.compilertools.ReplicateUtilities;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.util.BooleanUtils;
import io.deephaven.db.util.DhObjectComparisons;
import io.deephaven.db.v2.sources.aggregate.CharAggregateColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.page.CharChunkPage;
import io.deephaven.db.v2.sources.chunk.sized.SizedCharChunk;
import io.deephaven.db.v2.sources.chunk.util.factories.CharChunkFactory;
import io.deephaven.db.v2.sources.chunk.util.chunkfillers.CharChunkFiller;
import io.deephaven.db.v2.sources.chunk.util.pools.CharChunkPool;
import io.deephaven.db.v2.sources.chunkcolumnsource.CharChunkColumnSource;
import io.deephaven.db.v2.sources.immutable.ImmutableCharArraySource;
import io.deephaven.db.v2.sources.sparse.CharOneOrN;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.deephaven.compilertools.ReplicateUtilities.*;

public class ReplicateSourcesAndChunks {

    public static void main(String... args) throws IOException {
        replicateSparseArraySources();

        replicateSingleValues();
        ReplicatePrimitiveCode.charToAllButBooleanAndLong(CharacterArraySource.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(CharAggregateColumnSource.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(UngroupedCharArrayColumnSource.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(UngroupedCharDbArrayColumnSource.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(UngroupedBoxedCharDbArrayColumnSource.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(UngroupedBoxedCharArrayColumnSource.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAllButBoolean(ImmutableCharArraySource.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAll(SizedCharChunk.class, ReplicatePrimitiveCode.MAIN_SRC);

        replicateChunks();
        replicateWritableChunks();

        replicateChunkChunks();
        replicateWritableChunkChunks();

        replicateResettableChunks();
        replicateResettableChunkChunks();

        replicateResettableWritableChunks();
        replicateResettableWritableChunkChunks();

        replicateFactories();
        ReplicatePrimitiveCode.charToAll(CharChunkPool.class, ReplicatePrimitiveCode.MAIN_SRC);

        replicateChunkFillers();

        ReplicatePrimitiveCode.charToAll(CharChunkPage.class, ReplicatePrimitiveCode.MAIN_SRC);

        replicateChunkColumnSource();
    }

    private static void replicateSingleValues() throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(CharacterSingleValueSource.class, ReplicatePrimitiveCode.MAIN_SRC);
        replicateObjectSingleValue();
    }

    private static void replicateObjectSingleValue() throws IOException {
        final String className = ReplicatePrimitiveCode.charToObject(CharacterSingleValueSource.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "class ObjectSingleValueSource", "class ObjectSingleValueSource<T>",
                "<Object>", "<T>",
                "ForObject", "ForObject<T>",
                "Object getObject", "T get",
                "Object getPrevObject", "T getPrev",
                "Object current", "T current",
                "Object prev", "T prev",
                "set\\(Object", "set(T",
                "set\\(long key, Object", "set(long key, T",
                "final ObjectChunk", "final ObjectChunk<T, ? extends Attributes.Values>",
                "unbox\\((.*)\\)", "$1"
        );
        lines = ReplicateUtilities.removeRegion(lines, "UnboxedSetter");
        lines = ReplicateUtilities.replaceRegion(lines, "Constructor", Arrays.asList(
                "    public ObjectSingleValueSource(Class<T> type) {",
                "        super(type);",
                "        current = null;",
                "        prev = null;",
                "    }"));
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateChunkColumnSource() throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(CharChunkColumnSource.class, ReplicatePrimitiveCode.MAIN_SRC);
        replicateObjectChunkColumnSource();
    }

    private static void replicateObjectChunkColumnSource() throws IOException {
        final String className = ReplicatePrimitiveCode.charToObject(CharChunkColumnSource.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "class ObjectChunkColumnSource", "class ObjectChunkColumnSource<T>",
                "<Object>", "<T>",
                "ForObject", "ForObject<T>",
                "Object getObject", "T get",
                "Object current", "T current",
                "ObjectChunk<\\? extends Attributes.Values>", "ObjectChunk<T, ? extends Attributes.Values>",
                "QueryConstants.NULL_OBJECT", "null"
        );

        lines = ReplicateUtilities.replaceRegion(lines, "constructor", Arrays.asList(
        "    protected ObjectChunkColumnSource(Class<T> type, Class<?> componentType) {",
                "        this(type, componentType, new TLongArrayList());",
                "    }",
                "",
        "    protected ObjectChunkColumnSource(Class<T> type, Class<?> componentType, final TLongArrayList firstOffsetForData) {",
                "        super(type, componentType);",
                "        this.firstOffsetForData = firstOffsetForData;",
                "    }"

        ));

        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateSparseArraySources() throws IOException {
        replicateOneOrN();

        ReplicatePrimitiveCode.charToAllButBooleanAndLong(CharacterSparseArraySource.class, ReplicatePrimitiveCode.MAIN_SRC);
        replicateSparseLongSource();

        replicateSparseBooleanSource();
        replicateSparseObjectSource();
    }

    private static void replicateChunks() throws IOException {
        ReplicatePrimitiveCode.charToAllButBooleanAndByte(CharChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        replicateByteChunks();
        replicateBooleanChunks();
        replicateObjectChunks();
    }

    private static void replicateByteChunks() throws IOException {
        final String className = ReplicatePrimitiveCode.charToByte(CharChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
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
        final String className = ReplicatePrimitiveCode.charToBoolean(CharChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File classFile = new File(className);
        List<String> classLines = FileUtils.readLines(classFile, Charset.defaultCharset());
        classLines = ReplicateUtilities.removeRegion(classLines, "BufferImports");
        classLines = ReplicateUtilities.removeRegion(classLines, "CopyToBuffer");
        FileUtils.writeLines(classFile, classLines);
    }

    private static void replicateObjectChunks() throws IOException {
        final String className = ReplicatePrimitiveCode.charToObject(CharChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
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
                "static T\\[\\] makeArray", "static <T> T[] makeArray"
        );

        lines = replaceRegion(lines, "makeArray", Arrays.asList(
        "    public static <T> T[] makeArray(int capacity) {",
        "        if (capacity == 0) {",
        "            //noinspection unchecked",
        "            return (T[]) ArrayUtils.EMPTY_OBJECT_ARRAY;",
        "        }",
        "        //noinspection unchecked",
        "        return (T[])new Object[capacity];",
        "    }"));
        lines = ReplicateUtilities.removeRegion(lines, "BufferImports");
        lines = ReplicateUtilities.removeRegion(lines, "CopyToBuffer");
        lines = expandDowncast(lines, "ObjectChunk");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateChunkChunks() throws IOException {
        ReplicatePrimitiveCode.charToAll(CharChunkChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        replicateObjectChunkChunks();
    }

    private static void replicateObjectChunkChunks() throws IOException {
        final String className = ReplicatePrimitiveCode.charToObject(CharChunkChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
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
                "(  +)innerData = ", "$1//noinspection unchecked" + "\n$1innerData = (T[][])"
        );

        lines = expandDowncast(lines, "ObjectChunkChunk");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateWritableChunks() throws IOException {
        final List<String> files = ReplicatePrimitiveCode.charToAllButBoolean(WritableCharChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        for(String fileName : files) {
            final File classFile = new File(fileName);
            List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
            lines = ReplicateUtilities.removeRegion(lines, "SortFixup");
            FileUtils.writeLines(classFile, lines);
        }
        replicateWritableBooleanChunks();
        replicateWritableObjectChunks();
    }

    private static void replicateWritableBooleanChunks() throws IOException {
        final String writableBooleanChunkClassName = ReplicatePrimitiveCode.charToBoolean(WritableCharChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File writableBooleanChunkClassFile = new File(writableBooleanChunkClassName);
        List<String> writableBooleanChunkClassLines = FileUtils.readLines(writableBooleanChunkClassFile, Charset.defaultCharset());
        writableBooleanChunkClassLines = ReplicateUtilities.removeRegion(writableBooleanChunkClassLines, "BufferImports");
        writableBooleanChunkClassLines = ReplicateUtilities.removeRegion(writableBooleanChunkClassLines, "CopyFromBuffer");
        writableBooleanChunkClassLines = ReplicateUtilities.removeRegion(writableBooleanChunkClassLines, "FillWithNullValueImports");
        writableBooleanChunkClassLines = ReplicateUtilities.removeRegion(writableBooleanChunkClassLines, "FillWithNullValueImpl");
        writableBooleanChunkClassLines = ReplicateUtilities.removeRegion(writableBooleanChunkClassLines, "sort");
        FileUtils.writeLines(writableBooleanChunkClassFile, writableBooleanChunkClassLines);
    }

    private static void replicateWritableObjectChunks() throws IOException {
        final String className = ReplicatePrimitiveCode.charToObject(WritableCharChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
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
                "NULL_OBJECT", "null"
        );
        lines = ReplicateUtilities.removeRegion(lines, "CopyFromBuffer");
        lines = ReplicateUtilities.removeRegion(lines, "FillWithNullValueImports");
        lines = ReplicateUtilities.removeRegion(lines, "BufferImports");
        lines = expandDowncast(lines, "WritableObjectChunk");
        lines = ReplicateUtilities.replaceRegion(lines,"fillWithBoxedValue",Arrays.asList(
                "    @Override\n" +
                "    public final void fillWithBoxedValue(int offset, int size, Object value) {\n" +
                "        fillWithValue(offset,size, (T)value);\n" +
                "    }"));
        lines = ReplicateUtilities.addImport(lines, DhObjectComparisons.class);
        lines = ReplicateUtilities.replaceRegion(lines, "sort", Arrays.asList(
                "    @Override",
                "    public final void sort(int start, int length) {",
                "        Arrays.sort(data, offset + start, offset + start + length, DhObjectComparisons::compare);",
                "    }"));
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateWritableChunkChunks() throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(WritableCharChunkChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        replicateWritableBooleanChunkChunks();
        replicateWritableObjectChunkChunks();
    }

    private static void replicateWritableBooleanChunkChunks() throws IOException {
        final String writableBooleanChunkClassName = ReplicatePrimitiveCode.charToBoolean(WritableCharChunkChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File writableBooleanChunkClassFile = new File(writableBooleanChunkClassName);
        List<String> writableBooleanChunkClassLines = FileUtils.readLines(writableBooleanChunkClassFile, Charset.defaultCharset());
        writableBooleanChunkClassLines = ReplicateUtilities.removeRegion(writableBooleanChunkClassLines, "BufferImports");
        writableBooleanChunkClassLines = ReplicateUtilities.removeRegion(writableBooleanChunkClassLines, "CopyFromBuffer");
        writableBooleanChunkClassLines = ReplicateUtilities.removeRegion(writableBooleanChunkClassLines, "FillWithNullValueImports");
        writableBooleanChunkClassLines = ReplicateUtilities.removeRegion(writableBooleanChunkClassLines, "FillWithNullValueImpl");
        writableBooleanChunkClassLines = ReplicateUtilities.removeRegion(writableBooleanChunkClassLines, "sort");
        FileUtils.writeLines(writableBooleanChunkClassFile, writableBooleanChunkClassLines);
    }

    private static void replicateWritableObjectChunkChunks() throws IOException {
        final String className = ReplicatePrimitiveCode.charToObject(WritableCharChunkChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
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
                "NULL_OBJECT", "null"
        );
        lines = ReplicateUtilities.removeRegion(lines, "CopyFromBuffer");
        lines = ReplicateUtilities.removeRegion(lines, "FillWithNullValueImports");
        lines = ReplicateUtilities.removeRegion(lines, "BufferImports");
        lines = expandDowncast(lines, "WritableObjectChunkChunk");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateResettableChunks() throws IOException {
        ReplicatePrimitiveCode.charToAll(ResettableCharChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        replicateResettableObjectChunks();
    }

    private static void replicateResettableObjectChunks() throws IOException {
        final String className = ReplicatePrimitiveCode.charToObject(ResettableCharChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                " <ATTR_BASE", " <T, ATTR_BASE",
                "ObjectChunk<ATTR", "ObjectChunk<T, ATTR",
                "ObjectChunk<[?] ", "ObjectChunk<T, ? ",
                "Object\\[]", "T[]",
                "ResettableObjectChunk<ATTR", "ResettableObjectChunk<T, ATTR",
                "( +)this\\(ArrayUtils.EMPTY_OBJECT_ARRAY", "$1//noinspection unchecked" + "\n$1this((T[])ArrayUtils.EMPTY_OBJECT_ARRAY",
                "( +)(final T\\[] typedArray)", "$1//noinspection unchecked" + "\n$1$2"
        );
        lines = expandDowncast(lines, "ResettableObjectChunk");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateResettableWritableChunks() throws IOException {
        ReplicatePrimitiveCode.charToAll(ResettableWritableCharChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        replicateResettableWritableObjectChunks();
    }

    private static void replicateResettableWritableObjectChunks() throws IOException {
        final String className = ReplicatePrimitiveCode.charToObject(ResettableWritableCharChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                " <ATTR_BASE", " <T, ATTR_BASE",
                "ObjectChunk<ATTR", "ObjectChunk<T, ATTR",
                "ObjectChunk<[?] ", "ObjectChunk<T, ? ",
                "Object\\[]", "T[]",
                "ResettableObjectChunk<ATTR", "ResettableObjectChunk<T, ATTR",
                "( +)this\\(ArrayUtils.EMPTY_OBJECT_ARRAY", "$1//noinspection unchecked" + "\n$1this((T[])ArrayUtils.EMPTY_OBJECT_ARRAY",
                "( +)(final T\\[] typedArray)", "$1//noinspection unchecked" + "\n$1$2"
        );
        lines = expandDowncast(lines, "ResettableWritableObjectChunk");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateResettableWritableChunkChunks() throws IOException {
        ReplicatePrimitiveCode.charToAll(ResettableWritableCharChunkChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        replicateResettableWritableObjectChunkChunks();
    }

    private static void replicateResettableWritableObjectChunkChunks() throws IOException {
        final String className = ReplicatePrimitiveCode.charToObject(ResettableWritableCharChunkChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "<ATTR extends Any>", "<T, ATTR extends Any>",
                "WritableObjectChunkChunk<ATTR", "WritableObjectChunkChunk<T, ATTR",
                "WritableObjectChunk<ATTR", "WritableObjectChunk<T, ATTR",
                "WritableObjectChunk<[?]", "WritableObjectChunk<T, ?",
                "Object\\[]", "T[]",
                "( +)this\\(ArrayUtils.EMPTY_OBJECT_ARRAY", "$1//noinspection unchecked" + "\n$1this((T[])ArrayUtils.EMPTY_OBJECT_ARRAY",
                "( +)(final T\\[] typedArray)", "$1//noinspection unchecked" + "\n$1$2"
        );

        lines = expandDowncast(lines, "ResettableWritableObjectChunkChunk");

        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateResettableChunkChunks() throws IOException {
        ReplicatePrimitiveCode.charToAll(ResettableCharChunkChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        replicateResettableObjectChunkChunks();
    }

    private static void replicateResettableObjectChunkChunks() throws IOException {
        final String className = ReplicatePrimitiveCode.charToObject(ResettableCharChunkChunk.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "<ATTR extends Any>", "<T, ATTR extends Any>",
                "ResettableObjectChunkChunk<ATTR", "ResettableObjectChunkChunk<T, ATTR",
                "ObjectChunk<ATTR", "ObjectChunk<T, ATTR",
                "ObjectChunk<[?]", "ObjectChunk<T, ?",
                "ObjectChunkChunk<ATTR", "ObjectChunkChunk<T, ATTR"
        );
        lines = expandDowncast(lines, "ResettableObjectChunkChunk");
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateFactories() throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(CharChunkFactory.class, ReplicatePrimitiveCode.MAIN_SRC);
        final String className = ReplicatePrimitiveCode.charToBoolean(CharChunkFactory.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File classFile = new File(className);
        List<String> classLines = FileUtils.readLines(classFile, Charset.defaultCharset());
        classLines = ReplicateUtilities.replaceRegion(classLines, "dbArrayWrap", Arrays.asList(
                "    @NotNull",
                "    @Override",
                "    public final DbBooleanArrayDirect dbArrayWrap(Object array) {",
                "        throw new UnsupportedOperationException(\"No boolean primitive DbArray.\");",
                "    }",
                "",
                "    @NotNull",
                "    @Override",
                "    public DbBooleanArraySlice dbArrayWrap(Object array, int offset, int capacity) {",
                "        throw new UnsupportedOperationException(\"No boolean primitive DbArray.\");",
                "    }"));
        FileUtils.writeLines(classFile, classLines);
    }

    private static void replicateChunkFillers() throws IOException {
        ReplicatePrimitiveCode.charToAll(CharChunkFiller.class, ReplicatePrimitiveCode.MAIN_SRC);
        replicateObjectChunkFiller();
    }

    private static void replicateObjectChunkFiller() throws IOException {
        final String className = ReplicatePrimitiveCode.charToObject(CharChunkFiller.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File classFile = new File(className);
        List<String> lines = FileUtils.readLines(classFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "ObjectChunk<\\?", "ObjectChunk<Object, ?",
                "src.getObject", "src.get",
                "src.getPrevObject", "src.getPrev"
        );
        FileUtils.writeLines(classFile, lines);
    }

    private static void replicateSparseLongSource() throws IOException {
        // this is super hokey, but we need to preserve the Long thing that we replicate to
        final String longBasePath = ReplicatePrimitiveCode.basePathForClass(LongSparseArraySource.class, ReplicatePrimitiveCode.MAIN_SRC);

        final File longSparseArraySourceFile = new File(longBasePath, LongSparseArraySource.class.getSimpleName() + ".java");
        final File abstractLongSparseArraySourceFile = new File(longBasePath, AbstractSparseLongArraySource.class.getSimpleName() + ".java");


        final String longSparseCode = FileUtils.readFileToString(longSparseArraySourceFile, Charset.defaultCharset());
        ReplicatePrimitiveCode.charToLong(CharacterSparseArraySource.class, ReplicatePrimitiveCode.MAIN_SRC, Collections.emptyMap());

        Files.delete(abstractLongSparseArraySourceFile.toPath());
        Files.move(longSparseArraySourceFile.toPath(), abstractLongSparseArraySourceFile.toPath());

        FileUtils.writeStringToFile(longSparseArraySourceFile, longSparseCode, Charset.defaultCharset());

        List<String> abstractLines = FileUtils.readLines(abstractLongSparseArraySourceFile, Charset.defaultCharset());
        abstractLines = globalReplacements(abstractLines, "LongSparseArraySource", "AbstractSparseLongArraySource",
                "public class AbstractSparseLongArraySource extends SparseArrayColumnSource<Long> implements MutableColumnSourceGetDefaults.ForLong", "abstract public class AbstractSparseLongArraySource<T> extends SparseArrayColumnSource<T> implements MutableColumnSourceGetDefaults.LongBacked<T>",
                "ColumnSource<Long>", "ColumnSource<T>");
        abstractLines = replaceRegion(abstractLines, "constructor", Arrays.asList(
                "    AbstractSparseLongArraySource(Class<T> type) {",
                "        super(type);",
                "        blocks = new LongOneOrN.Block0();", "    }"));
        abstractLines = replaceRegion(abstractLines, "boxed methods", Collections.emptyList());
        abstractLines = replaceRegion(abstractLines, "copy method", Collections.emptyList());
        abstractLines = simpleFixup(abstractLines, "getChunk", "LongChunk<Values> getChunk", "Chunk<Values> getChunk");
        abstractLines = simpleFixup(abstractLines, "getPrevChunk", "LongChunk<Values> getPrevChunk", "Chunk<Values> getPrevChunk");
        abstractLines = standardCleanups(abstractLines);

        FileUtils.writeLines(abstractLongSparseArraySourceFile, abstractLines);
    }

    private static void replicateSparseBooleanSource() throws IOException {
        final String booleanPath = ReplicatePrimitiveCode.charToBooleanAsByte(CharacterSparseArraySource.class, ReplicatePrimitiveCode.MAIN_SRC, Collections.emptyMap());
        final File booleanFile = new File(booleanPath);
        List<String> lines = FileUtils.readLines(booleanFile, Charset.defaultCharset());

        lines = addImport(lines, "import static " + BooleanUtils.class.getCanonicalName() + ".NULL_BOOLEAN_AS_BYTE;\n");
        lines = addImport(lines, BooleanUtils.class);
        lines = globalReplacements(lines, "BooleanOneOrN", "ByteOneOrN");
        lines = globalReplacements(lines, "WritableBooleanChunk", "WritableObjectChunk",
                "asBooleanChunk", "asObjectChunk",
                "BooleanChunk<Values>", "ObjectChunk<Boolean, Values>",
                "ObjectChunk<Values>", "ObjectChunk<Boolean, Values>",
                "BooleanChunk<[?] extends Values>", "ObjectChunk<Boolean, ? extends Values>",
                "ObjectChunk<[?] extends Values>", "ObjectChunk<Boolean, ? extends Values>",
                "BooleanChunk<[?] super Values>", "ObjectChunk<Boolean, ? super Values>",
                "ObjectChunk<[?] super Values>", "ObjectChunk<Boolean, ? super Values>");
        lines = simpleFixup(lines, "primitive get", "NULL_BOOLEAN", "NULL_BOOLEAN_AS_BYTE", "getBoolean", "getByte", "getPrevBoolean", "getPrevByte");

        lines = replaceRegion(lines, "copyFromTypedArray", Arrays.asList("                    for (int jj = 0; jj < length; ++jj) {", "                         chunk.set(jj + ctx.offset, BooleanUtils.byteAsBoolean(ctx.block[sIndexWithinBlock + jj]));", "                    }"));
        lines = replaceRegion(lines, "copyToTypedArray", Arrays.asList("                for (int jj = 0; jj < length; ++jj) {", "                    block[sIndexWithinBlock + jj] = BooleanUtils.booleanAsByte(chunk.get(offset + jj));", "                }"));

        lines = applyFixup(lines, "fillByKeys", "(.*chunk.set\\(.*, )(ctx\\.block.*)(\\);.*)", m -> Collections.singletonList(m.group(1) + "BooleanUtils.byteAsBoolean(" + m.group(2) + ")"  + m.group(3)));
        lines = applyFixup(lines, "fillByUnorderedKeys", "(.*byteChunk.set\\(.*, )(block.*)(\\);.*)", m -> Collections.singletonList(m.group(1) + "BooleanUtils.byteAsBoolean(" + m.group(2) + ")"  + m.group(3)));
        lines = applyFixup(lines, "fillFromChunkByKeys", "(.*)(chunk.get\\(.*\\));", m -> Collections.singletonList(m.group(1) + "BooleanUtils.booleanAsByte(" + m.group(2) + ");"));
        lines = applyFixup(lines, "fillFromChunkUnordered", "(.*)(chunk.get\\(.*\\));", m -> Collections.singletonList(m.group(1) + "BooleanUtils.booleanAsByte(" + m.group(2) + ");"));

        lines = simpleFixup(lines, "allocateNullFilledBlock", "NULL_BOOLEAN", "NULL_BOOLEAN_AS_BYTE");
        lines = simpleFixup(lines, "boxed methods", "box\\(getBoolean\\(", "BooleanUtils.byteAsBoolean(getByte(", "box\\(getPrevBoolean\\(", "BooleanUtils.byteAsBoolean(getPrevByte(");
        lines = simpleFixup(lines, "boxed methods", "unbox", "BooleanUtils.booleanAsByte");
        lines = applyFixup(lines, "constructor", "(.*super\\().*(\\);.*)", m -> Collections.singletonList(m.group(1) + "Boolean.class" + m.group(2)));

        lines = removeRegion(removeRegion(lines, "getChunk"), "getPrevChunk");
        lines = replaceRegion(lines, "getChunk", Arrays.asList(
                "    @Override",
                "    public ObjectChunk<Boolean, Values> getChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {",
                "        return getChunkByFilling(context, orderedKeys).asObjectChunk();",
                "    }"));
        lines = replaceRegion(lines, "getPrevChunk", Arrays.asList(
                "    @Override",
                "    public ObjectChunk<Boolean, Values> getPrevChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {",
                "        return getPrevChunkByFilling(context, orderedKeys).asObjectChunk();",
                "    }"));

        lines = simpleFixup(lines, "fillByUnorderedKeys", "WritableObjectChunk byteChunk", "WritableObjectChunk<Boolean, ? super Values> byteChunk");
        lines = simpleFixup(lines, "fillByUnorderedKeys", "byteChunk", "booleanObjectChunk");
        lines = simpleFixup(lines, "fillByUnorderedKeys", "BooleanUtils\\.byteAsBoolean\\(blockToUse == null \\? NULL_BOOLEAN : blockToUse\\[indexWithinBlock\\]\\)", "blockToUse == null ? NULL_BOOLEAN : BooleanUtils.byteAsBoolean(blockToUse[indexWithinBlock])");

        lines = simpleFixup(lines, "serialization",
                "NULL_BOOLEAN", "NULL_BOOLEAN_AS_BYTE",
                "ObjectChunk", "ByteChunk",
                "BooleanChunk", "ByteChunk",
                "<Boolean>", "<Byte>",
                "<Boolean, Values>", "<Values>"
                );

        lines = insertRegion(lines, "serialization", Arrays.asList(
                "    WritableSource reinterpretForSerialization() {",
                "        return (WritableSource)reinterpret(byte.class);",
                "    }",
                ""));

        lines = simpleFixup(lines, "reinterpretForSerialization",
                "return this;", "return (WritableSource)reinterpret(byte.class);");

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
                "    private static class ReinterpretedAsByte extends AbstractColumnSource<Byte> implements MutableColumnSourceGetDefaults.ForByte, FillUnordered, WritableSource<Byte> {",
                "        private final BooleanSparseArraySource wrapped;",
                "",
                "        private ReinterpretedAsByte(BooleanSparseArraySource wrapped) {",
                "            super(byte.class);",
                "            this.wrapped = wrapped;",
                "        }",
                "",
                "        @Override",
                "        public byte getByte(long index) {",
                "            return wrapped.getByte(index);",
                "        }",
                "",
                "        @Override",
                "        public byte getPrevByte(long index) {",
                "            return wrapped.getPrevByte(index);",
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
                "        public void copy(ColumnSource<Byte> sourceColumn, long sourceKey, long destKey) {",
                "            set(destKey, sourceColumn.getByte(sourceKey));",
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
                "        public void fillChunk(@NotNull final ColumnSource.FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {",
                "            fillSparseChunk(destination, orderedKeys);",
                "        }",
                "",
                "        @Override",
                "        public void fillPrevChunk(@NotNull final ColumnSource.FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {",
                "            fillSparsePrevChunk(destination, orderedKeys);",
                "        }",
                "",
                "        @Override",
                "        public void fillChunkUnordered(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final LongChunk<? extends KeyIndices> keyIndices) {",
                "            fillSparseChunkUnordered(destination, keyIndices);",
                "        }",
                "",
                "        @Override",
                "        public void fillPrevChunkUnordered(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final LongChunk<? extends KeyIndices> keyIndices) {",
                "            fillSparsePrevChunkUnordered(destination, keyIndices);",
                "        }",
                "",
                "        private void fillSparseChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final OrderedKeys indices) {",
                "            if (indices.size() == 0) {",
                "                destGeneric.setSize(0);",
                "                return;",
                "            }",
                "            // This implementation is in \"key\" style (rather than range style).",
                "            final WritableByteChunk<? super Values> chunk = destGeneric.asWritableByteChunk();",
                "            final FillByContext<byte[]> ctx = new FillByContext<>();",
                "            indices.forEachLong((final long v) -> {",
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
                "        private void fillSparsePrevChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final OrderedKeys indices) {",
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
                "            fillSparsePrevChunkUnordered(destGeneric, indices.asKeyIndicesChunk());",
                "        }",
                "",
                "        private void fillSparseChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends KeyIndices> indices) {",
                "            final WritableByteChunk<? super Values> chunk = destGeneric.asWritableByteChunk();",
                "            // This implementation is in \"key\" style (rather than range style).",
                "            for (int ii = 0; ii < indices.size(); ) {",
                "                final long firstKey = indices.get(ii);",
                "                if (firstKey == Index.NULL_KEY) {",
                "                    chunk.set(ii++, NULL_BOOLEAN_AS_BYTE);",
                "                    continue;",
                "                }",
                "                final long masked = firstKey & ~INDEX_MASK;",
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
                "                final byte [] block = wrapped.blocks.getInnermostBlockByKeyOrNull(firstKey);",
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
                "        private void fillSparsePrevChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends KeyIndices> indices) {",
                "            final WritableByteChunk<? super Values> booleanObjectChunk = destGeneric.asWritableByteChunk();",
                "            for (int ii = 0; ii < indices.size(); ) {",
                "                final long firstKey = indices.get(ii);",
                "                if (firstKey == Index.NULL_KEY) {",
                "                    booleanObjectChunk.set(ii++, NULL_BOOLEAN_AS_BYTE);",
                "                    continue;",
                "                }",
                "                final long masked = firstKey & ~INDEX_MASK;",
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
                "                final byte [] block = wrapped.blocks.getInnermostBlockByKeyOrNull(firstKey);",
                "                if (block == null) {",
                "                    booleanObjectChunk.fillWithNullValue(ii, lastII - ii + 1);",
                "                    ii = lastII + 1;",
                "                    continue;",
                "                }",
                "",
                "                final long [] prevInUse = (wrapped.prevFlusher == null || wrapped.prevInUse == null) ? null :",
                "                        wrapped.prevInUse.getInnermostBlockByKeyOrNull(firstKey);",
                "                final byte [] prevBlock = prevInUse == null ? null : wrapped.prevBlocks.getInnermostBlockByKeyOrNull(firstKey);",
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
                "        public void fillFromChunk(@NotNull FillFromContext context_unused, @NotNull Chunk<? extends Values> src, @NotNull OrderedKeys orderedKeys) {",
                "            // This implementation is in \"key\" style (rather than range style).",
                "            if (orderedKeys.size() == 0) {",
                "                return;",
                "            }",
                "            final ByteChunk<? extends Values> chunk = src.asByteChunk();",
                "            final LongChunk<OrderedKeyIndices> keys = orderedKeys.asKeyIndicesChunk();",
                "",
                "            final boolean hasPrev = wrapped.prevFlusher != null;",
                "",
                "            if (hasPrev) {",
                "                wrapped.prevFlusher.maybeActivate();",
                "            }",
                "",
                "            for (int ii = 0; ii < keys.size(); ) {",
                "                final long firstKey = keys.get(ii);",
                "                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;",
                "                int lastII = ii;",
                "                while (lastII + 1 < keys.size() && keys.get(lastII + 1) <= maxKeyInCurrentBlock) {",
                "                    ++lastII;",
                "                }",
                "",
                "                final int block0 = (int) (firstKey >> BLOCK0_SHIFT) & BLOCK0_MASK;",
                "                final int block1 = (int) (firstKey >> BLOCK1_SHIFT) & BLOCK1_MASK;",
                "                final int block2 = (int) (firstKey >> BLOCK2_SHIFT) & BLOCK2_MASK;",
                "                final byte [] block = wrapped.ensureBlock(block0, block1, block2);",
                "",
                "                if (chunk.isAlias(block)) {",
                "                    throw new UnsupportedOperationException(\"Source chunk is an alias for target data\");",
                "                }",
                "",
                "                // This conditional with its constant condition should be very friendly to the branch predictor.",
                "                final byte[] prevBlock = hasPrev ? wrapped.ensurePrevBlock(firstKey, block0, block1, block2) : null;",
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
                "    }"
                ));
        FileUtils.writeLines(booleanFile, lines);
    }

    private static void replicateSparseObjectSource() throws IOException {
        final String objectPath = ReplicatePrimitiveCode.charToObject(CharacterSparseArraySource.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());

        lines = removeRegion(lines, "boxed methods");
        lines = removeRegion(lines, "boxing imports");

        lines = globalReplacements(lines, "ObjectOneOrN.Block([0-2])", "ObjectOneOrN.Block$1<T>");

        lines = globalReplacements(lines,
                "public class ObjectSparseArraySource extends SparseArrayColumnSource<Object> implements MutableColumnSourceGetDefaults.ForObject", "public class ObjectSparseArraySource<T> extends SparseArrayColumnSource<T> implements MutableColumnSourceGetDefaults.ForObject<T>",
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
                "    private final boolean isArrayType;",
                "",
                "    ObjectSparseArraySource(Class<T> type) {",
                "        super(type);",
                "        blocks = new ObjectOneOrN.Block0<>();",
                "        isArrayType = DbArrayBase.class.isAssignableFrom(type);",
                "    }",
                "",
                "    ObjectSparseArraySource(Class<T> type, Class componentType) {",
                "        super(type, componentType);",
                "        blocks = new ObjectOneOrN.Block0<>();",
                "        isArrayType = DbArrayBase.class.isAssignableFrom(type);",
                "    }"
        ));

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

        lines = replaceRegion(lines, "copy method", Arrays.asList(
                "    @Override",
                "    public void copy(ColumnSource<T> sourceColumn, long sourceKey, long destKey) {",
                "        final T value = sourceColumn.get(sourceKey);",
                "",
                "        if (isArrayType && value instanceof DbArrayBase) {",
                "            final DbArrayBase dbArray = (DbArrayBase) value;",
                "            set(destKey, (T) dbArray.getDirect());",
                "        } else {",
                "            set(destKey, value);",
                "        }",
                "    }"));

        lines = addImport(lines, DbArrayBase.class);

        FileUtils.writeLines(objectFile, lines);
    }

    private static void replicateOneOrN() throws IOException {
        ReplicatePrimitiveCode.charToAll(CharOneOrN.class, ReplicatePrimitiveCode.MAIN_SRC);
        final String objectOneOrNPath = ReplicatePrimitiveCode.charToObject(CharOneOrN.class, ReplicatePrimitiveCode.MAIN_SRC);
        final File oneOrNFile = new File(objectOneOrNPath);
        List<String> lines = FileUtils.readLines(oneOrNFile, Charset.defaultCharset());

        lines = globalReplacements(lines,
                "class Block([0-2])", "class Block$1<T>",
                "Object \\[\\]", "T []", "SoftRecycler<Object", "SoftRecycler<T",
                "QueryConstants.NULL_OBJECT", "null",
                "new Object\\[BLOCK2_SIZE\\]\\[\\];",
                "(T[][])new Object[BLOCK2_SIZE][];"
                );

        lines = simpleFixup(lines, "Block0", "Block1", "Block1<T>", "Block2", "Block2<T>");
        lines = simpleFixup(lines, "Block1", "Block2", "Block2<T>");

        lines = globalReplacements(lines,
                "new Block2<T>\\[BLOCK1_SIZE\\]", "new Block2[BLOCK1_SIZE]",
                "new Block1<T>\\[BLOCK0_SIZE\\]", "new Block1[BLOCK0_SIZE]"
                );

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
