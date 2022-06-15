/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

/**
 * Code generation for primitive ColumnIterator implementations.
 */
public class ReplicateColumnIterators {

    private static final String CHAR_PATH =
            "engine/api/src/main/java/io/deephaven/engine/table/iterators/CharacterColumnIterator.java";
    private static final String INT_PATH =
            "engine/api/src/main/java/io/deephaven/engine/table/iterators/IntegerColumnIterator.java";

    public static void main(String... args) throws IOException {
        charToByte(CHAR_PATH, Collections.emptyMap());
        charToShort(CHAR_PATH, Collections.emptyMap());
        charToFloat(CHAR_PATH, Collections.emptyMap());

        fixupChunkSize(intToLong(INT_PATH, Collections.emptyMap()), "long");
        fixupChunkSize(intToDouble(INT_PATH, Collections.emptyMap()), "double");
    }

    private static void fixupChunkSize(@NotNull final String path, @NotNull final String primitive) throws IOException {
        final File file = new File(path);
        final List<String> inputLines = FileUtils.readLines(file, Charset.defaultCharset());
        final List<String> outputLines =
                ReplicationUtils.simpleFixup(
                        ReplicationUtils.simpleFixup(inputLines,
                                "chunkSize", primitive + " chunkSize", "int chunkSize"),
                        "currentSize", primitive + " currentSize", "int currentSize");
        FileUtils.writeLines(file, outputLines);
    }
}
