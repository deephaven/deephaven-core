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

    private static final String CHAR_IFACE_PATH =
            "engine/api/src/main/java/io/deephaven/engine/table/iterators/CharacterColumnIterator.java";
    private static final String INT_IFACE_PATH =
            "engine/api/src/main/java/io/deephaven/engine/table/iterators/IntegerColumnIterator.java";
    private static final String CHAR_CHUNKED_PATH =
            "engine/api/src/main/java/io/deephaven/engine/table/iterators/ChunkedCharacterColumnIterator.java";
    private static final String CHAR_SERIAL_PATH =
            "engine/api/src/main/java/io/deephaven/engine/table/iterators/SerialCharacterColumnIterator.java";

    public static void main(String... args) throws IOException {
        {
            charToByte(CHAR_IFACE_PATH, Collections.emptyMap());
            charToShort(CHAR_IFACE_PATH, Collections.emptyMap());
            fixupIntToDouble(charToFloat(CHAR_IFACE_PATH, Collections.emptyMap()));
        }
        {
            fixupChunkSize(intToLong(INT_IFACE_PATH, Collections.emptyMap(), "interface"), "long");
            fixupChunkSize(intToDouble(INT_IFACE_PATH, Collections.emptyMap(), "interface"), "double");
        }
        {
            fixupPrimitiveConsumer(charToAllButBoolean(CHAR_CHUNKED_PATH));
        }
        {
            charToAllButBoolean(CHAR_SERIAL_PATH);
        }
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

    private static void fixupPrimitiveConsumer(@NotNull final List<String> paths) throws IOException {
        for (final String path : paths) {
            if (!(path.contains("Integer") || path.contains("Long") || path.contains("Double"))) {
                continue;
            }
            final File file = new File(path);
            final List<String> inputLines = FileUtils.readLines(file, Charset.defaultCharset());
            final List<String> outputLines = ReplicationUtils.simpleFixup(inputLines,
                    "ConsumerInterface", "io.deephaven.engine.primitive.function", "java.util.function");
            FileUtils.writeLines(file, outputLines);
        }
    }

    private static void fixupIntToDouble(@NotNull final String path) throws IOException {
        final File file = new File(path);
        final List<String> lines = ReplicationUtils.globalReplacements(
                FileUtils.readLines(file, Charset.defaultCharset()),
                "IntStream", "DoubleStream",
                "NULL_INT", "NULL_DOUBLE",
                "\\(int\\)", "(double)",
                "\\{@code int\\}", "{@code double}",
                "OfInt", "OfDouble",
                "IntToLongFunction", "IntToDoubleFunction",
                "FloatToIntFunction", "FloatToDoubleFunction",
                "applyAsLong", "applyAsDouble",
                "applyAsInt", "applyAsDouble",
                "primitive int", "primitive double",
                "IntStream", "DoubleStream",
                "intStream", "doubleStream",
                "streamAsInt", "streamAsDouble");
        FileUtils.writeLines(file, lines);
    }
}
