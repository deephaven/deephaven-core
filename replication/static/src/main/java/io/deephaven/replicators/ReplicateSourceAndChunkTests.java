//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateSourceAndChunkTests {
    private static final String TASK = "replicateSourceAndChunkTests";

    public static void main(String[] args) throws IOException {
        ReplicateSourcesAndChunks.main(args);

        charToAllButBoolean(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterArraySource.java");
        charToAllButBoolean(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterArraySource.java");

        fixupBooleanColumnSourceTest(charToBooleanAsByte(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterArraySource.java",
                Collections.emptyMap()));
        fixupObjectColumnSourceTest(charToObject(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterArraySource.java"));

        charToAllButBoolean(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/AbstractCharacterColumnSourceTest.java");
        fixupBooleanColumnSourceTest(charToBooleanAsByte(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/AbstractCharacterColumnSourceTest.java",
                Collections.emptyMap()));
        fixupObjectColumnSourceTest(charToObject(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/AbstractCharacterColumnSourceTest.java"));
        charToAllButBoolean(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterSparseArraySource.java");
        fixupBooleanColumnSourceTest(charToBooleanAsByte(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterSparseArraySource.java",
                Collections.emptyMap()));
        charToAllButBoolean(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterImmutableArraySource.java");
        charToAllButBoolean(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/TestCharacterImmutable2DArraySource.java");

        charToAllButBoolean(TASK, "engine/chunk/src/test/java/io/deephaven/chunk/TestCharChunk.java");
        fixupChunkTest(charToObject(TASK, "engine/chunk/src/test/java/io/deephaven/chunk/TestCharChunk.java"));
        fixupBooleanChunkTest(charToBoolean(TASK, "engine/chunk/src/test/java/io/deephaven/chunk/TestCharChunk.java"));

        charToAllButBoolean(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/deltaaware/TestCharacterDeltaAwareColumnSource.java");
        fixupBooleanDeltaAwareColumnSourceTest(charToBooleanAsByte(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/deltaaware/TestCharacterDeltaAwareColumnSource.java",
                Collections.emptyMap()));
        fixupObjectDeltaAwareColumnSourceTest(charToObject(TASK,
                "engine/table/src/test/java/io/deephaven/engine/table/impl/sources/deltaaware/TestCharacterDeltaAwareColumnSource.java"));

        charToAllButBoolean(TASK,
                "engine/test-utils/src/main/java/io/deephaven/engine/testutil/sources/CharTestSource.java");
        fixupObjectTestSource(charToObject(TASK,
                "engine/test-utils/src/main/java/io/deephaven/engine/testutil/sources/CharTestSource.java"));
        charToAllButBoolean(TASK,
                "engine/test-utils/src/main/java/io/deephaven/engine/testutil/sources/ImmutableCharTestSource.java");
        fixupObjectTestSource(charToObject(TASK,
                "engine/test-utils/src/main/java/io/deephaven/engine/testutil/sources/ImmutableCharTestSource.java"));

        charToAllButBooleanAndFloats(TASK,
                "engine/test-utils/src/main/java/io/deephaven/engine/testutil/generator/CharGenerator.java");
        floatToAllFloatingPoints(TASK,
                "engine/test-utils/src/main/java/io/deephaven/engine/testutil/generator/FloatGenerator.java");
        charToAllButBooleanAndFloats(TASK,
                "engine/test-utils/src/main/java/io/deephaven/engine/testutil/generator/UniqueCharGenerator.java");
        charToLong(TASK,
                "engine/test-utils/src/main/java/io/deephaven/engine/testutil/generator/SortedCharGenerator.java");
        charToInteger(TASK,
                "engine/test-utils/src/main/java/io/deephaven/engine/testutil/generator/SortedCharGenerator.java",
                Collections.emptyMap());
        fixupSortedDoubleGenerator(charToDouble(TASK,
                "engine/test-utils/src/main/java/io/deephaven/engine/testutil/generator/SortedCharGenerator.java",
                Collections.emptyMap()));
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
        lines = simpleFixup(lines, "validate with fill", "NULL_BOOLEAN", "BooleanUtils.NULL_BOOLEAN_AS_BYTE");

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

    private static void fixupBooleanChunkTest(String booleanPath) throws IOException {
        List<String> lines;
        final File booleanFile = new File(booleanPath);
        lines = FileUtils.readLines(booleanFile, Charset.defaultCharset());
        lines = removeRegion(lines, "testArray");
        FileUtils.writeLines(booleanFile, lines);
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

    private static void fixupObjectTestSource(String objectPath) throws IOException {
        List<String> lines;
        final File objectFile = new File(objectPath);
        lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = removeImport(lines, "\\s*import .*QueryConstants.*;");
        lines = globalReplacements(lines, "QueryConstants.NULL_OBJECT", "null",
                "Object getObject", "T get",
                "getObject", "get",
                "Object retVal", "T retVal",
                "Object getPrevObject", "T getPrev",
                "ForObject", "ForObject<T>",
                "ObjectTestSource extends", "ObjectTestSource<T> extends",
                "<Object>", "<T>",
                "add(.*)Object \\[\\]", "add$1T \\[\\]",
                "([^.])Long2ObjectOpenHashMap", "$1Long2ObjectOpenHashMap<T>");
        lines = removeRegion(lines, "boxing imports");
        lines = removeRegion(lines, "boxed get");
        lines = removeRegion(lines, "boxed add");
        lines = removeRegion(lines, "boxed getPrev");
        final boolean isImmutable = objectPath.contains("Immutable");
        if (isImmutable) {
            lines = replaceRegion(lines, "empty constructor",
                    Collections.singletonList("    public ImmutableObjectTestSource(Class<T> type) {\n" +
                            "        this(type, RowSetFactory.empty(), ObjectChunk.getEmptyChunk());\n" +
                            "    }\n"));
        } else {
            lines = replaceRegion(lines, "empty constructor",
                    Collections.singletonList("    public ObjectTestSource(Class<T> type) {\n" +
                            "        this(type, RowSetFactory.empty(), ObjectChunk.getEmptyChunk());\n" +
                            "    }\n"));
        }
        if (isImmutable) {
            lines = replaceRegion(lines, "chunk constructor", Collections.singletonList(
                    "    public ImmutableObjectTestSource(Class<T> type, RowSet rowSet, Chunk<Values> data) {\n" +
                            "        super(type);\n" +
                            "        add(rowSet, data);\n" +
                            "        setDefaultReturnValue(this.data);\n" +
                            "    }\n"));
        } else {
            lines = replaceRegion(lines, "chunk constructor",
                    Collections.singletonList(
                            "    public ObjectTestSource(Class<T> type, RowSet rowSet, Chunk<Values> data) {\n" +
                                    "        super(type);\n" +
                                    "        add(rowSet, data);\n" +
                                    "        setDefaultReturnValue(this.data);\n" +
                                    "        this.prevData = this.data;\n" +
                                    "    }\n"));
        }
        lines = replaceRegion(lines, "chunk add", Arrays.asList(
                "    public synchronized void add(final RowSet rowSet, Chunk<Values> vs) {\n" +
                        "        if (rowSet.size() != vs.size()) {\n" +
                        "            throw new IllegalArgumentException(\"rowSet=\" + rowSet + \", data size=\" + vs.size());\n"
                        +
                        "        }",
                (isImmutable ? "" : "\n        maybeInitializePrevForStep();\n"),
                "        final ObjectChunk<T, Values> vcs = vs.asObjectChunk();",
                "        rowSet.forAllRowKeys(new LongConsumer() {",
                "            private final MutableInt ii = new MutableInt(0);",
                "",
                "            @Override",
                "            public void accept(final long v) {",
                (isImmutable
                        ? "                // the unit test framework will ask us to add things, we need to conveniently ignore it\n"
                        : "") +
                        (isImmutable ? "                if (!data.containsKey(v)) {\n" : "") +
                        (isImmutable ? "    " : "") + "                data.put(v, vcs.get(ii.get()));",
                (isImmutable ? "                }\n" : "") +
                        "                ii.increment();",
                "            }",
                "        });",
                "    }"));
        FileUtils.writeLines(objectFile, lines);
    }

    private static void fixupSortedDoubleGenerator(String doublePath) throws IOException {
        List<String> lines;
        final File doubleFile = new File(doublePath);
        lines = FileUtils.readLines(doubleFile, Charset.defaultCharset());
        lines = addImport(lines, "import org.apache.commons.lang3.mutable.MutableDouble;");
        lines = replaceRegion(lines, "check sorted mutable", Collections
                .singletonList("        final MutableDouble lastValue = new MutableDouble(-Double.MAX_VALUE);"));
        lines = replaceRegion(lines, "check sorted assertion", Collections
                .singletonList("            Assert.leq(lastValue.doubleValue(), \"lastValue\", value, \"value\");\n"));
        FileUtils.writeLines(doubleFile, lines);
    }
}
