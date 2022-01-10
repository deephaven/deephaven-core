/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.replicators;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateSourceAndChunkTests {
    public static void main(String[] args) throws IOException {
        ReplicateSourcesAndChunks.main(args);

        charToAllButBoolean(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterArraySource.java");
        charToAllButBoolean(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterArraySource.java");

        fixupBooleanColumnSourceTest(charToBooleanAsByte(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterArraySource.java",
                Collections.emptyMap()));
        fixupObjectColumnSourceTest(charToObject(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterArraySource.java"));

        charToAllButBoolean(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/AbstractCharacterColumnSourceTest.java");
        fixupBooleanColumnSourceTest(charToBooleanAsByte(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/AbstractCharacterColumnSourceTest.java",
                Collections.emptyMap()));
        fixupObjectColumnSourceTest(charToObject(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/AbstractCharacterColumnSourceTest.java"));
        charToAllButBoolean(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterSparseArraySource.java");
        fixupBooleanColumnSourceTest(charToBooleanAsByte(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterSparseArraySource.java",
                Collections.emptyMap()));
        charToAllButBoolean(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterImmutableArraySource.java");
        charToAllButBoolean(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterImmutable2DArraySource.java");

        charToAll("engine/chunk/src/test/java/io/deephaven/chunk/TestCharChunk.java");
        fixupChunkTest(charToObject("engine/chunk/src/test/java/io/deephaven/chunk/TestCharChunk.java"));

        charToAllButBoolean(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/deltaaware/TestCharacterDeltaAwareColumnSource.java");
        fixupBooleanDeltaAwareColumnSourceTest(charToBooleanAsByte(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/deltaaware/TestCharacterDeltaAwareColumnSource.java",
                Collections.emptyMap()));
        fixupObjectDeltaAwareColumnSourceTest(charToObject(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/deltaaware/TestCharacterDeltaAwareColumnSource.java"));
    }

    private static void fixupObjectColumnSourceTest(String objectPath) throws IOException {
        List<String> lines;
        final File objectFile = new File(objectPath);
        lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = removeImport(lines, "\\s*import static.*QueryConstants.*;");
        lines = globalReplacements(lines, "NULL_OBJECT", "null",
                "ObjectChunk<? extends Values>", "ObjectChunk<?, ? extends Values>",
                "source.getObject", "source.get",
                "dest.getObject", "dest.get",
                "source.getPrevObject", "source.getPrev",
                "ObjectChunk<Values>", "ObjectChunk<?, ? extends Values>",
                "ObjectChunk<[?] extends Values>", "ObjectChunk<?, ? extends Values>",
                "Map<String, ObjectArraySource>", "Map<String, ObjectArraySource<?>>",
                "new ObjectArraySource\\(\\)", "new ObjectArraySource<>\\(String.class\\)",
                "new ObjectSparseArraySource\\(\\)", "new ObjectSparseArraySource<>\\(String.class\\)");
        lines = removeRegion(lines, "boxing imports");
        FileUtils.writeLines(objectFile, lines);
    }

    private static void fixupBooleanColumnSourceTest(String booleanPath) throws IOException {
        final File booleanFile = new File(booleanPath);
        List<String> lines = FileUtils.readLines(booleanFile, Charset.defaultCharset());

        lines = globalReplacements(lines,
                "\\(BooleanChunk\\)", "\\(ObjectChunk\\)",
                "\\(BooleanChunk<[?] extends Values>\\)", "\\(ObjectChunk<Boolean, ? extends Values>\\)",
                "BooleanChunk<Values>(\\s+)", "ObjectChunk<Boolean, Values>$1",
                "BooleanChunk<[?] extends Values>(\\s+)", "ObjectChunk<Boolean, ? extends Values>$1",
                "asBooleanChunk", "asObjectChunk",
                "BooleanChunk.chunkWrap", "ObjectChunk.chunkWrap",
                "BooleanChunkEquals", "ObjectChunkEquals",
                "WritableBooleanChunk.makeWritableChunk", "WritableObjectChunk.makeWritableChunk");

        if (booleanPath.contains("Abstract")) {
            lines = globalReplacements(lines, "BooleanChunk(\\s+)", "ObjectChunk<Boolean, Values>$1");
        } else {
            lines = globalReplacements(lines, "BooleanChunk(\\s+)", "ObjectChunk<Boolean, ? extends Values>$1");
        }

        lines = simpleFixup(lines, "arrayFill", "NULL_BOOLEAN", "BooleanUtils.NULL_BOOLEAN_AS_BYTE");
        lines = simpleFixup(lines, "testsourcesink", "ChunkType.Boolean", "ChunkType.Object");
        lines = simpleFixup(lines, "null unordered check", "NULL_BOOLEAN", "BooleanUtils.NULL_BOOLEAN_AS_BYTE");

        lines = applyFixup(lines, "fromsource", "(.*)checkFromSource\\((.*)byte fromSource(.*)\\) \\{",
                m -> Collections.singletonList(
                        m.group(1) + "checkFromSource(" + m.group(2) + "Boolean fromSource" + m.group(3) + ") {"));
        lines = applyFixup(lines, "fromsource", "(.*)checkFromSource\\((.*)byte fromChunk(.*)\\) \\{",
                m -> Collections.singletonList(
                        m.group(1) + "checkFromSource(" + m.group(2) + "Boolean fromChunk" + m.group(3) + ") {"));
        lines = applyFixup(lines, "fromvalues", "(.*)checkFromValues\\((.*)byte fromChunk(.*)\\) \\{",
                m -> Collections.singletonList(
                        m.group(1) + "checkFromValues(" + m.group(2) + "Boolean fromChunk" + m.group(3) + ") {"));
        lines = applyFixup(lines, "fromvalues", "(.*)fromValues, fromChunk\\);", m -> Collections.singletonList(m
                .group(1)
                + "fromValues == BooleanUtils.NULL_BOOLEAN_AS_BYTE ? null : fromValues == BooleanUtils.TRUE_BOOLEAN_AS_BYTE, fromChunk);"));
        lines = removeRegion(lines, "samecheck");
        lines = addImport(lines, "import io.deephaven.util.BooleanUtils;");
        lines = addImport(lines, "import io.deephaven.chunk.WritableObjectChunk;");
        lines = addImport(lines, "import io.deephaven.chunk.ObjectChunk;");
        if (!booleanPath.contains("Abstract") && !booleanPath.contains("Sparse")) {
            lines = removeImport(lines, "import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;");
        }
        lines = simpleFixup(lines, "elementGet",
                "getBoolean", "getByte");
        FileUtils.writeLines(booleanFile, lines);
    }

    private static void fixupChunkTest(String objectPath) throws IOException {
        List<String> lines;
        final File objectFile = new File(objectPath);
        lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "ObjectChunk<ATTR>", "ObjectChunk<Object, ATTR>",
                "ObjectChunk<Values>", "ObjectChunk<Object, Values>",
                "ObjectChunkChunk<Values>", "ObjectChunkChunk<Object, Values>",
                "String\\[\\]", "byte[]");
        lines = removeRegion(lines, "boxing imports");
        FileUtils.writeLines(objectFile, lines);
    }

    private static void fixupObjectDeltaAwareColumnSourceTest(String objectPath) throws IOException {
        fixupObjectColumnSourceTest(objectPath);
    }

    private static void fixupBooleanDeltaAwareColumnSourceTest(String booleanPath) throws IOException {
        final File booleanFile = new File(booleanPath);
        List<String> lines = FileUtils.readLines(booleanFile, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "Map<Long, Boolean>", "Map<Long, Byte>", // covers Map and HashMap
                "source.getBoolean", "source.getByte",
                "source.getPrevBoolean", "source.getPrevByte",
                "NULL_BOOLEAN", "BooleanUtils.NULL_BOOLEAN_AS_BYTE",
                "byte.class", "boolean.class",
                "BooleanChunk<[?] extends Values>", "ObjectChunk<Boolean, ? extends Values>",
                "asBooleanChunk", "asObjectChunk",
                "values.get\\((.*)\\)", "io.deephaven.util.BooleanUtils.booleanAsByte(values.get($1))");
        lines = addImport(lines, "import io.deephaven.util.BooleanUtils;");
        lines = addImport(lines, "import io.deephaven.chunk.ObjectChunk;");
        FileUtils.writeLines(booleanFile, lines);
    }
}
