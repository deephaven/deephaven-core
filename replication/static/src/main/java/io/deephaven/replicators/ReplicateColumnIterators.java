//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
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
    public static final String TASK = "replicateColumnIterators";

    public static void main(String... args) throws IOException {
        {
            charToByte(TASK, CHAR_IFACE_PATH, Collections.emptyMap());
            charToShort(TASK, CHAR_IFACE_PATH, Collections.emptyMap());
            fixupCharToInt(charToInteger(TASK, CHAR_IFACE_PATH, Collections.emptyMap()));
            fixupCharToFloat(charToFloat(TASK, CHAR_IFACE_PATH, Collections.emptyMap()));
        }
        {
            fixupChunkSize(intToLong(TASK, INT_IFACE_PATH, Collections.emptyMap(), "interface"), "long");
            fixupChunkSize(intToDouble(TASK, INT_IFACE_PATH, Collections.emptyMap(), "interface"), "double");
        }
        {
            fixupPrimitiveConsumer(charToAllButBoolean(TASK, CHAR_CHUNKED_PATH));
        }
        {
            charToAllButBoolean(TASK, CHAR_SERIAL_PATH);
        }
    }

    private static void fixupChunkSize(@NotNull final String path, @NotNull final String primitive)
            throws IOException {
        final File file = new File(path);
        final List<String> inputLines = FileUtils.readLines(file, Charset.defaultCharset());
        final List<String> outputLines = ReplicationUtils.simpleFixup(
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

    private static void fixupCharToInt(@NotNull final String path) throws IOException {
        final File file = new File(path);
        List<String> lines = ReplicationUtils.removeRegion(
                FileUtils.readLines(file, Charset.defaultCharset()), "streamAsInt");
        lines = ReplicationUtils.removeImport(lines,
                "import io.deephaven.engine.primitive.function.IntToIntFunction;",
                "import java.util.PrimitiveIterator;");
        lines = ReplicationUtils.addImport(lines,
                "import io.deephaven.util.type.TypeUtils;");
        lines = ReplicationUtils.replaceRegion(lines, "stream",
                ReplicationUtils.indent(List.of(
                        "/**",
                        " * Create an unboxed {@link IntStream} over the remaining elements of this IntegerColumnIterator. The result",
                        " * <em>must</em> be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A",
                        " * try-with-resources block is strongly encouraged.",
                        " *",
                        " * @return An unboxed {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close()",
                        " *         closed}.",
                        " */",
                        "@Override",
                        "@FinalDefault",
                        "default IntStream intStream() {",
                        "    return StreamSupport.intStream(",
                        "            Spliterators.spliterator(",
                        "                    this,",
                        "                    remaining(),",
                        "                    Spliterator.IMMUTABLE | Spliterator.ORDERED),",
                        "            false)",
                        "            .onClose(this::close);",
                        "}"), 4));
        FileUtils.writeLines(file, lines);
    }

    private static void fixupCharToFloat(@NotNull final String path) throws IOException {
        final File file = new File(path);
        final List<String> lines = ReplicationUtils.globalReplacements(
                FileUtils.readLines(file, Charset.defaultCharset()),
                "IntStream", "DoubleStream",
                "NULL_INT", "NULL_DOUBLE",
                "\\(int\\)", "(double)",
                "\\{@code int\\}", "{@code double}",
                "OfInt", "OfDouble",
                "FloatToIntFunction", "FloatToDoubleFunction",
                "intStream", "doubleStream",
                "streamAsInt", "streamAsDouble");
        FileUtils.writeLines(file, lines);
    }
}
