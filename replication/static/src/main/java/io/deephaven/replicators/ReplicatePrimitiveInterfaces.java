/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.IntStream;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicatePrimitiveInterfaces {

    private static final String CHAR_CONSUMER_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/function/CharConsumer.java";

    private static final String CHAR_TO_INT_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/function/CharToIntFunction.java";

    private static final String CHAR_ITERATOR_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/iterator/CloseablePrimitiveIteratorOfChar.java";

    private static final String INT_ITERATOR_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/iterator/CloseablePrimitiveIteratorOfInt.java";

    public static void main(String[] args) throws IOException {
        {
            charToShortAndByte(CHAR_CONSUMER_PATH);
            charToFloat(CHAR_CONSUMER_PATH, null);
        }
        {
            charToShortAndByte(CHAR_TO_INT_PATH);
            final String floatToIntPath = charToFloat(CHAR_TO_INT_PATH, null);
            removeExtraCopyrightHeader(intToDouble(floatToIntPath, null,
                    "interface",
                    "FunctionalInterface",
                    CHAR_TO_INT_PATH.substring(
                            CHAR_TO_INT_PATH.lastIndexOf('/') + 1,
                            CHAR_TO_INT_PATH.lastIndexOf(".java"))));
            if (!new File(floatToIntPath).delete()) {
                throw new IOException("Failed to delete extraneous " + floatToIntPath);
            }
        }
        {
            charToShortAndByte(CHAR_ITERATOR_PATH);
            final String floatPath = charToFloat(CHAR_ITERATOR_PATH, null);
            removeExtraCopyrightHeader(intToDouble(floatPath, null,
                    "interface",
                    "FunctionalInterface",
                    "int valueIndex",
                    "int subIteratorIndex",
                    CHAR_ITERATOR_PATH.substring(
                            CHAR_ITERATOR_PATH.lastIndexOf('/') + 1,
                            CHAR_ITERATOR_PATH.lastIndexOf(".java"))));
        }
        {
            intToLong(INT_ITERATOR_PATH, null,
                    "interface",
                    "FunctionalInterface",
                    "int valueIndex",
                    "int subIteratorIndex");
            intToDouble(INT_ITERATOR_PATH, null,
                    "interface",
                    "FunctionalInterface",
                    "int valueIndex",
                    "int subIteratorIndex");
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    private static String removeExtraCopyrightHeader(@NotNull final String path) throws IOException {
        final File file = new File(path);
        final List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        IntStream.of(5, 5, 5, 5, 5).forEach(lines::remove);
        FileUtils.writeLines(file, lines);
        return path;
    }
}
