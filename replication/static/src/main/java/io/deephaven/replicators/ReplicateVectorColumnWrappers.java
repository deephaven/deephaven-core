//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBoolean;
import static io.deephaven.replication.ReplicatePrimitiveCode.charToObject;

public class ReplicateVectorColumnWrappers {
    private static final String TASK = "replicateVectorColumnWrappers";

    private static final String CHAR_IMPL_PATH =
            "engine/api/src/main/java/io/deephaven/engine/table/vectors/CharVectorColumnWrapper.java";
    private static final String CHAR_TEST_PATH =
            "engine/api/src/test/java/io/deephaven/engine/table/vectors/CharVectorColumnWrapperTest.java";

    public static void main(String[] args) throws IOException {
        charToAllButBoolean(TASK, CHAR_IMPL_PATH);
        charToAllButBoolean(TASK, CHAR_TEST_PATH);
        fixupObject(charToObject(TASK, CHAR_TEST_PATH));
    }

    private static void fixupObject(@NotNull final String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = ReplicationUtils.globalReplacements(lines,
                "ObjectVector ", "ObjectVector<Object> ",
                "new ObjectVectorColumnWrapper", "new ObjectVectorColumnWrapper<>",
                "getMemoryColumnSource\\(data\\)", "getMemoryColumnSource(data, Object.class, null)");
        FileUtils.writeLines(file, lines);
    }
}
