/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public class ReplicateVectorTests {

    private static final String CHAR_BASE_PATH = "engine/vector/src/test/java/io/deephaven/vector/CharVectorTest.java";
    private static final String CHAR_DIRECT_PATH =
            "engine/vector/src/test/java/io/deephaven/vector/CharVectorDirectTest.java";

    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(CHAR_BASE_PATH);
        ReplicatePrimitiveCode.charToAllButBoolean(CHAR_DIRECT_PATH);
        fixupObject(ReplicatePrimitiveCode.charToObject(CHAR_BASE_PATH));
        fixupObject(ReplicatePrimitiveCode.charToObject(CHAR_DIRECT_PATH));
    }

    private static void fixupObject(@NotNull final String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = ReplicationUtils.replaceRegion(lines, "IteratorTypeImport", List.of(
                "import io.deephaven.engine.primitive.iterator.CloseableIterator;"));
        lines = ReplicationUtils.removeRegion(lines, "NullConstantImport");
        lines = ReplicationUtils.replaceRegion(lines, "TestType", List.of(
                "        assertEquals(ObjectVector.type(io.deephaven.qst.type.StringType.of()).clazz(), ObjectVector.class);"));
        lines = ReplicationUtils.globalReplacements(lines,
                "new ObjectVectorDirect", "new ObjectVectorDirect<>",
                "ObjectVector ", "ObjectVector<Object> ",
                "\\(Object\\) ", "",
                "assertEquals\\(NULL_OBJECT, ", "assertNull(",
                "NULL_OBJECT", "null",
                "ObjectVector.type\\(\\)", "ObjectVector.type()",
                "CloseablePrimitiveIteratorOfObject", "CloseableIterator<Object>",
                "nextObject", "next");
        FileUtils.writeLines(file, lines);
    }
}
