//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ReplicateVectors {
    private static final String TASK = "replicateVectors";

    @SuppressWarnings("AutoBoxing")
    public static void main(String[] args) throws IOException {
        Map<String, Long> serialVersionUIDs = Collections.emptyMap();

        final String charVectorJavaPath = "engine/vector/src/main/java/io/deephaven/vector/CharVector.java";

        ReplicatePrimitiveCode.charToByte(TASK, charVectorJavaPath, serialVersionUIDs);
        ReplicatePrimitiveCode.charToShort(TASK, charVectorJavaPath, serialVersionUIDs);

        fixupCharToInt(ReplicatePrimitiveCode.charToInteger(TASK, charVectorJavaPath, serialVersionUIDs));
        fixupCharToLong(ReplicatePrimitiveCode.charToLong(TASK, charVectorJavaPath, serialVersionUIDs));
        fixupCharToFloat(ReplicatePrimitiveCode.charToFloat(TASK, charVectorJavaPath, serialVersionUIDs));
        fixupCharToDouble(ReplicatePrimitiveCode.charToDouble(TASK, charVectorJavaPath, serialVersionUIDs));

        ReplicatePrimitiveCode.charToAllButBoolean(TASK,
                "engine/vector/src/main/java/io/deephaven/vector/CharVectorDirect.java",
                serialVersionUIDs);
        ReplicatePrimitiveCode.charToAllButBoolean(TASK,
                "engine/vector/src/main/java/io/deephaven/vector/CharVectorSlice.java",
                serialVersionUIDs);
        ReplicatePrimitiveCode.charToAllButBoolean(TASK,
                "engine/vector/src/main/java/io/deephaven/vector/CharSubVector.java",
                serialVersionUIDs);
    }

    public static void fixupCharToInt(@NotNull final String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = ReplicationUtils.removeImport(lines,
                "import io.deephaven.engine.primitive.function.IntToIntFunction;");
        lines = ReplicationUtils.replaceRegion(lines, "streamAsInt",
                ReplicationUtils.indent(List.of(
                        "",
                        "/**",
                        "* Create a boxed {@link Stream} over the remaining elements of this IntegerColumnIterator. The result <em>must</em>",
                        "* be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A",
                        "* try-with-resources block is strongly encouraged.",
                        "*",
                        "* @return A boxed {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close()",
                        "*         closed}.",
                        "*/",
                        "@Override",
                        "@FinalDefault",
                        "default Stream<Integer> stream() {",
                        "   return intStream().mapToObj(TypeUtils::box);",
                        "}"), 8));
        FileUtils.writeLines(file, lines);
    }

    public static void fixupCharToLong(@NotNull final String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = ReplicationUtils.removeImport(lines,
                "import io.deephaven.engine.primitive.function.LongToIntFunction;");
        lines = ReplicationUtils.removeRegion(lines, "streamAsInt");
        FileUtils.writeLines(file, lines);
    }

    public static void fixupCharToDouble(@NotNull final String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = ReplicationUtils.removeImport(lines,
                "import io.deephaven.engine.primitive.function.DoubleToIntFunction;");
        lines = ReplicationUtils.simpleFixup(lines, "ElementEquals",
                "aIterator\\.nextDouble\\(\\) != bIterator\\.nextDouble\\(\\)",
                "Double.doubleToLongBits(aIterator.nextDouble()) != Double.doubleToLongBits(bIterator.nextDouble())");
        lines = ReplicationUtils.removeRegion(lines, "streamAsInt");
        FileUtils.writeLines(file, lines);
    }

    public static void fixupCharToFloat(@NotNull final String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = ReplicationUtils.addImport(lines,
                "import java.util.stream.DoubleStream;");
        lines = ReplicationUtils.simpleFixup(lines, "ElementEquals",
                "aIterator\\.nextFloat\\(\\) != bIterator\\.nextFloat\\(\\)",
                "Float.floatToIntBits(aIterator.nextFloat()) != Float.floatToIntBits(bIterator.nextFloat())");
        lines = ReplicationUtils.removeImport(lines,
                "import io.deephaven.engine.primitive.function.FloatToIntFunction;");
        lines = ReplicationUtils.simpleFixup(lines, "streamAsInt",
                "streamAsInt\\(", "streamAsDouble(",
                "IntStream", "DoubleStream",
                "NULL_INT", "NULL_DOUBLE",
                "\\(int\\)", "(double)");
        FileUtils.writeLines(file, lines);
    }
}
