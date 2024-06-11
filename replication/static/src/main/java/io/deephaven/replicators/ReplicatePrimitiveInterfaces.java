//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.File;
import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicatePrimitiveInterfaces {
    private static final String TASK = "replicatePrimitiveInterfaces";

    private static final String CHAR_CONSUMER_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/function/CharConsumer.java";

    private static final String CHAR_TO_INT_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/function/CharToIntFunction.java";

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
    }
}
