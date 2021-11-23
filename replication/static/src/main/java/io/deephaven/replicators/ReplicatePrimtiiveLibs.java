/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.replicators;

import io.deephaven.replication.ReplicateUtilities;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

/**
 * Autogenerates primitives from template java files (e.g. CharacterPrimitives, ShortNumericPrimitives,
 * FloatNumericPrimitives, and FlotFpPrimitives).
 */
public class ReplicatePrimtiiveLibs {
    public static void main(String[] args) throws IOException {
        List<String> files =
                charToAllButBoolean("engine/table/src/main/java/io/deephaven/libs/primitives/CharacterPrimitives.java");
        fixup(files);

        shortToAllIntegralTypes("engine/table/src/main/java/io/deephaven/libs/primitives/ShortNumericPrimitives.java");
        floatToAllFloatingPoints("engine/table/src/main/java/io/deephaven/libs/primitives/FloatNumericPrimitives.java");
        floatToAllFloatingPoints("engine/table/src/main/java/io/deephaven/libs/primitives/FloatFpPrimitives.java");
    }

    private static void fixup(List<String> files) throws IOException {
        for (String file : files) {
            final File fileyfile = new File(file);
            List<String> lines = FileUtils.readLines(fileyfile, Charset.defaultCharset());
            lines = ReplicateUtilities.removeRegion(lines, "SortFixup");
            FileUtils.writeLines(fileyfile, lines);
        }
    }
}
