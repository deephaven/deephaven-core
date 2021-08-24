/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.libs.primitives;

import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.compilertools.ReplicateUtilities;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Autogenerates primitives from template java files (e.g. CharacterPrimitives,
 * ShortNumericPrimitives, FloatNumericPrimitives, and FlotFpPrimitives).
 */
public class Replicate {
    public static void main(String[] args) throws IOException {
        List<String> files = ReplicatePrimitiveCode.charToAllButBoolean(CharacterPrimitives.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        fixup(files);

        ReplicatePrimitiveCode.shortToAllIntegralTypes(ShortNumericPrimitives.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(FloatNumericPrimitives.class,
            ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.floatToAllFloatingPoints(FloatFpPrimitives.class,
            ReplicatePrimitiveCode.MAIN_SRC);
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
