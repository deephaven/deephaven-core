/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateRangeSearchKernels {

    private static final String CHAR_PATH =
            "engine/table/src/main/java/io/deephaven/engine/table/impl/rangejoin/RangeSearchKernelChar.java";

    public static void main(@NotNull final String... args) throws IOException {
        charToAllButBooleanAndFloats(CHAR_PATH);

        fixupDouble(charToDouble(CHAR_PATH, Collections.emptyMap()));
        fixupFloat(charToFloat(CHAR_PATH, Collections.emptyMap()));
        fixupObject(charToObject(CHAR_PATH));
    }

    private static void fixupDouble(@NotNull final String doublePath) throws IOException {
        final File doubleFile = new File(doublePath);
        List<String> doubleLines = FileUtils.readLines(doubleFile, Charset.defaultCharset());
        doubleLines = simpleFixup(doubleLines, "isNaN",
                "\\@SuppressWarnings\\(\"unused\"\\) ", "", "false", "Double.isNaN(v)");
        FileUtils.writeLines(doubleFile, doubleLines);
    }

    private static void fixupFloat(@NotNull final String floatPath) throws IOException {
        final File floatFile = new File(floatPath);
        List<String> floatLines = FileUtils.readLines(floatFile, Charset.defaultCharset());
        floatLines = simpleFixup(floatLines, "isNaN",
                "\\@SuppressWarnings\\(\"unused\"\\) ", "", "false", "Float.isNaN(v)");
        FileUtils.writeLines(floatFile, floatLines);
    }

    private static void fixupObject(@NotNull final String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> objectLines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        objectLines = removeImport(objectLines, "import static io.deephaven.util.QueryConstants.NULL_OBJECT;");
        objectLines = fixupChunkAttributes(objectLines);
        objectLines = simpleFixup(objectLines, "isNull", "v == NULL_OBJECT", "v == null");
        objectLines = replaceRegion(objectLines, "lt", List.of(
                "    private static boolean lt(final Object a, final Object b) {",
                "        // We need not deal with null or NaN values here",
                "        //noinspection unchecked,rawtypes",
                "        return ((Comparable)a).compareTo(b) < 0;",
                "    }"));
        objectLines = replaceRegion(objectLines, "leq", List.of(
                "    private static boolean leq(final Object a, final Object b) {",
                "        // We need not deal with null or NaN values here",
                "        //noinspection unchecked,rawtypes",
                "        return ((Comparable)a).compareTo(b) <= 0;",
                "    }"));
        FileUtils.writeLines(objectFile, objectLines);
    }
}
