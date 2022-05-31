/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
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
public class ReplicatePrimitiveLibs {
    public static void main(String[] args) throws IOException {
        List<String> files = charToAllButBoolean(
                "engine/function/src/main/java/io/deephaven/function/CharacterPrimitives.java");
        fixup(files);

        shortToAllIntegralTypes(
                "engine/function/src/main/java/io/deephaven/function/ShortNumericPrimitives.java");
        floatToAllFloatingPoints(
                "engine/function/src/main/java/io/deephaven/function/FloatNumericPrimitives.java");
        floatToAllFloatingPoints("engine/function/src/main/java/io/deephaven/function/FloatFpPrimitives.java");
    }

    private static void fixup(List<String> files) throws IOException {
        for (String file : files) {
            final File fileyfile = new File(file);
            List<String> lines = FileUtils.readLines(fileyfile, Charset.defaultCharset());
            lines = ReplicationUtils.removeRegion(lines, "SortFixup");
            FileUtils.writeLines(fileyfile, lines);
        }
    }
}
