/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicatePrimitiveInterfaces {

    private static final String CHAR_CONSUMER_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/function/CharConsumer.java";

    private static final String CHAR_ITERATOR_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/iterator/PrimitiveIteratorOfChar.java";

    private static final String CHAR_TO_INT_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/function/CharToIntFunction.java";

    public static void main(String[] args) throws IOException {
        for (final String charPath : List.of(CHAR_CONSUMER_PATH, CHAR_ITERATOR_PATH)) {
            charToShortAndByte(charPath);
            fixupIntToDouble(charToFloat(charPath, null));
        }
        for (final String charPath : List.of(CHAR_TO_INT_PATH)) {
            charToShortAndByte(charPath);
            renameIntToDouble(fixupIntToDouble(charToFloat(charPath, null)));
        }
    }

    private static String fixupIntToDouble(@NotNull final String path) throws IOException {
        final File file = new File(path);
        final List<String> lines = ReplicationUtils.globalReplacements(
                FileUtils.readLines(file, Charset.defaultCharset()),
                "IntConsumer", "DoubleConsumer",
                "accept\\(int\\)", "accept(double)",
                "\\{@code int\\}", "{@code double}",
                "OfInt", "OfDouble",
                "int ", "double ",
                "nextInt", "nextDouble",
                "IntToLongFunction", "IntToDoubleFunction",
                "FloatToIntFunction", "FloatToDoubleFunction",
                "applyAsLong", "applyAsDouble",
                "applyAsInt", "applyAsDouble",
                "primitive int", "primitive double",
                "IntStream", "DoubleStream",
                "intStream", "doubleStream",
                "streamAsInt", "streamAsDouble",
                "\\{@code int\\}", "{@code double}");
        FileUtils.writeLines(file, lines);
        return path;
    }

    private static void renameIntToDouble(@NotNull final String path) throws IOException {
        final File oldFile = new File(path);
        final File newFile = new File(path.replace("Int", "Double"));
        if (!oldFile.renameTo(newFile)) {
            throw new IOException("Rename " + oldFile + " to " + newFile + " failed");
        }
    }
}
