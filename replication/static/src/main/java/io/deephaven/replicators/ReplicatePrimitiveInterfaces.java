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
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicatePrimitiveInterfaces {
    private static final String TASK = "replicatePrimitiveInterfaces";

    private static final String CHAR_CONSUMER_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/function/CharConsumer.java";

    private static final String CHAR_TO_INT_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/function/CharToIntFunction.java";

    private static final String CHAR_VALUE_ITERATOR_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/value/iterator/ValueIteratorOfChar.java";

    private static final String CHAR_ITERATOR_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/iterator/CloseablePrimitiveIteratorOfChar.java";

    private static final String INT_ITERATOR_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/iterator/CloseablePrimitiveIteratorOfInt.java";

    private static final String TO_CHAR_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/function/ToCharFunction.java";

    public static void main(String[] args) throws IOException {
        {
            charToShortAndByte(TASK, CHAR_CONSUMER_PATH);
            charToFloat(TASK, CHAR_CONSUMER_PATH, null);
        }
        {
            charToShortAndByte(TASK, TO_CHAR_PATH);
            charToFloat(TASK, TO_CHAR_PATH, null);
        }
        {
            charToShortAndByte(TASK, CHAR_TO_INT_PATH);
            final String floatToIntPath = charToFloat(TASK, CHAR_TO_INT_PATH, null);
            intToDouble(TASK, floatToIntPath, null,
                    "interface",
                    "FunctionalInterface",
                    CHAR_TO_INT_PATH.substring(
                            CHAR_TO_INT_PATH.lastIndexOf('/') + 1,
                            CHAR_TO_INT_PATH.lastIndexOf(".java")));
            if (!new File(floatToIntPath).delete()) {
                throw new IOException("Failed to delete extraneous " + floatToIntPath);
            }
        }
        {
            charToShortAndByte(TASK, CHAR_ITERATOR_PATH);
            final String floatPath = charToFloat(TASK, CHAR_ITERATOR_PATH, null);
            intToDouble(TASK, floatPath, null,
                    "interface",
                    "FunctionalInterface",
                    "int valueIndex",
                    "int subIteratorIndex",
                    CHAR_ITERATOR_PATH.substring(
                            CHAR_ITERATOR_PATH.lastIndexOf('/') + 1,
                            CHAR_ITERATOR_PATH.lastIndexOf(".java")));
        }
        {
            intToLong(TASK, INT_ITERATOR_PATH, null,
                    "interface",
                    "FunctionalInterface",
                    "int valueIndex",
                    "int subIteratorIndex");
            intToDouble(TASK, INT_ITERATOR_PATH, null,
                    "interface",
                    "FunctionalInterface",
                    "int valueIndex",
                    "int subIteratorIndex");
        }
        {
            charToShortAndByte(TASK, CHAR_VALUE_ITERATOR_PATH);
            fixupCharToInt(charToInteger(TASK, CHAR_VALUE_ITERATOR_PATH, null));
            fixupCharToLong(charToLong(TASK, CHAR_VALUE_ITERATOR_PATH));
            fixupCharToFloat(charToFloat(TASK, CHAR_VALUE_ITERATOR_PATH, null));
            fixupCharToDouble(charToDouble(TASK, CHAR_VALUE_ITERATOR_PATH, null));
        }
    }

    public static void fixupCharToInt(@NotNull final String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = ReplicationUtils.removeImport(lines,
                "import io.deephaven.engine.primitive.function.IntToIntFunction;",
                "import io.deephaven.engine.primitive.function.IntConsumer;");
        lines = ReplicationUtils.addImport(lines,
                "import java.util.function.IntConsumer;");
        lines = ReplicationUtils.replaceRegion(lines, "streamAsInt",
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
                        "}",
                        "",
                        "/**",
                        " * Create a boxed {@link Stream} over the remaining elements of this DeephavenValueIteratorOfInt. ",
                        " * The result <em>must</em>, be {@link java.util.stream.BaseStream#close() closed} in order ",
                        " * to ensure resources are released. A try-with-resources block is strongly encouraged.",
                        " *",
                        " * @return A boxed {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close()",
                        " *         closed}.",
                        " */",
                        "@Override",
                        "@FinalDefault",
                        "default Stream<Integer> stream() {",
                        "   return intStream().mapToObj(TypeUtils::box);",
                        "}"), 4));
        FileUtils.writeLines(file, lines);
    }

    public static void fixupCharToLong(@NotNull final String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = ReplicationUtils.removeImport(lines,
                "import io.deephaven.engine.primitive.function.LongToIntFunction;",
                "import io.deephaven.engine.primitive.function.LongConsumer;");
        lines = ReplicationUtils.addImport(lines,
                "import java.util.stream.LongStream;",
                "import java.util.function.LongConsumer;");
        lines = ReplicationUtils.replaceRegion(lines, "streamAsInt",
                ReplicationUtils.indent(List.of("    /**",
                        " * Create an unboxed {@link LongStream} over the remaining elements of this ValueIteratorOfLong. The result",
                        " * <em>must</em> be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A",
                        " * try-with-resources block is strongly encouraged.",
                        " *",
                        " * @return An unboxed {@link LongStream} over the remaining contents of this iterator. Must be {@link Stream#close()",
                        " *         closed}.",
                        " */",
                        "@Override",
                        "@FinalDefault",
                        "default LongStream longStream() {",
                        "    return StreamSupport.longStream(",
                        "            Spliterators.spliterator(",
                        "                    this,",
                        "                    remaining(),",
                        "                    Spliterator.IMMUTABLE | Spliterator.ORDERED),",
                        "            false)",
                        "            .onClose(this::close);",
                        "}",
                        "",
                        "/**",
                        " * Create a boxed {@link Stream} over the remaining elements of this ValueIteratorOfLong. The result <em>must</em>",
                        " * be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A",
                        " * try-with-resources block is strongly encouraged.",
                        " *",
                        " * @return A boxed {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close()",
                        " *         closed}.",
                        " */",
                        "@Override",
                        "@FinalDefault",
                        "default Stream<Long> stream() {",
                        "    return longStream().mapToObj(TypeUtils::box);",
                        "}"), 4));
        FileUtils.writeLines(file, lines);
    }

    public static void fixupCharToDouble(@NotNull final String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = ReplicationUtils.removeImport(lines,
                "import io.deephaven.engine.primitive.function.DoubleToIntFunction;",
                "import io.deephaven.engine.primitive.function.DoubleConsumer;");
        lines = ReplicationUtils.addImport(lines,
                "import java.util.stream.DoubleStream;",
                "import java.util.function.DoubleConsumer;");
        lines = ReplicationUtils.replaceRegion(lines, "streamAsInt",
                ReplicationUtils.indent(List.of(
                        "/**",
                        " * Create an unboxed {@link DoubleStream} over the remaining elements of this ValueIteratorOfDouble. The result",
                        " * <em>must</em> be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A",
                        " * try-with-resources block is strongly encouraged.",
                        " *",
                        " * @return An unboxed {@link DoubleStream} over the remaining contents of this iterator. Must be {@link Stream#close()",
                        " *         closed}.",
                        " */",
                        "@Override",
                        "@FinalDefault",
                        "default DoubleStream doubleStream() {",
                        "    return StreamSupport.doubleStream(",
                        "            Spliterators.spliterator(",
                        "                    this,",
                        "                    remaining(),",
                        "                    Spliterator.IMMUTABLE | Spliterator.ORDERED),",
                        "            false)",
                        "            .onClose(this::close);",
                        "}",
                        "",
                        "/**",
                        " * Create a boxed {@link Stream} over the remaining elements of this ValueIteratorOfDouble. The result <em>must</em>",
                        " * be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A",
                        " * try-with-resources block is strongly encouraged.",
                        " *",
                        " * @return A boxed {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close()",
                        " *         closed}.",
                        " */",
                        "@Override",
                        "@FinalDefault",
                        "default Stream<Double> stream() {",
                        "    return doubleStream().mapToObj(TypeUtils::box);",
                        "}"), 4));
        FileUtils.writeLines(file, lines);
    }

    public static void fixupCharToFloat(@NotNull final String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = ReplicationUtils.removeImport(lines,
                "import io.deephaven.engine.primitive.function.FloatToIntFunction;",
                "import java.util.stream.IntStream;");
        lines = ReplicationUtils.addImport(lines,
                "import java.util.stream.DoubleStream;",
                "import io.deephaven.engine.primitive.function.FloatToDoubleFunction;");

        lines = ReplicationUtils.simpleFixup(lines, "streamAsInt",
                "streamAsInt\\(", "streamAsDouble(",
                "IntStream", "DoubleStream",
                "NULL_INT", "NULL_DOUBLE",
                "\\(int\\)", "(double)",
                "\\{@code int\\}", "{@code double}",
                "OfInt", "OfDouble",
                "FloatToIntFunction", "FloatToDoubleFunction",
                "intStream", "doubleStream");
        FileUtils.writeLines(file, lines);
    }
}
