//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateBarrageUtils {
    private static final String CHUNK_PACKAGE =
            "extensions/barrage/src/main/java/io/deephaven/extensions/barrage/chunk";

    public static void main(final String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean("replicateBarrageUtils",
                CHUNK_PACKAGE + "/CharChunkWriter.java");

        ReplicatePrimitiveCode.charToAllButBoolean("replicateBarrageUtils",
                CHUNK_PACKAGE + "/CharChunkReader.java");
        // ReplicatePrimitiveCode.floatToAllFloatingPoints("replicateBarrageUtils",
        // CHUNK_PACKAGE + "/FloatChunkReader.java", "Float16");
        fixupDoubleChunkReader(CHUNK_PACKAGE + "/DoubleChunkReader.java");

        ReplicatePrimitiveCode.charToAllButBoolean("replicateBarrageUtils",
                CHUNK_PACKAGE + "/array/CharArrayExpansionKernel.java");

        ReplicatePrimitiveCode.charToAllButBoolean("replicateBarrageUtils",
                CHUNK_PACKAGE + "/vector/CharVectorExpansionKernel.java");

        ReplicatePrimitiveCode.charToAllButBoolean("replicateBarrageUtils",
                CHUNK_PACKAGE + "/CharBarrageCopyKernel.java");

        ReplicatePrimitiveCode.charToAllButBoolean("replicateBarrageUtils",
                CHUNK_PACKAGE + "/CharBarrageRunKernel.java");

        // The Object variant is hand-written because null handling and generics differ; the Float and Double
        // variants need NaN canonicalization in getOrAdd, handled by the region fixup below.
        ReplicatePrimitiveCode.charToAllButBoolean("replicateBarrageUtils",
                CHUNK_PACKAGE + "/writermap/CharDictionaryWriterValueMap.java");
        fixupDictionaryWriterValueMap(CHUNK_PACKAGE + "/writermap/FloatDictionaryWriterValueMap.java", "float",
                "Float");
        fixupDictionaryWriterValueMap(CHUNK_PACKAGE + "/writermap/DoubleDictionaryWriterValueMap.java", "double",
                "Double");
    }

    private static void fixupDictionaryWriterValueMap(
            final @NotNull String path,
            final @NotNull String primitiveType,
            final @NotNull String boxedType) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = replaceRegion(lines, "canonicalization", List.of(
                "        // Canonicalize NaN so all bit patterns map to the same entry (fastutil hashes the raw bits).",
                "        final " + primitiveType + " key = " + boxedType + ".isNaN(value) ? " + boxedType
                        + ".NaN : value;"));
        FileUtils.writeLines(file, lines);
    }

    private static void fixupDoubleChunkReader(final @NotNull String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "Float16.toDouble", "Float16.toFloat",
                "doubleing point precision", "floating point precision",
                "half-precision doubles", "half-precision floats");
        lines = replaceRegion(lines, "PrecisionSingleDhNulls", List.of(
                "                    final float v = is.readFloat();",
                "                    chunk.set(offset + ii, doubleCast(v));"));
        lines = replaceRegion(lines, "PrecisionDoubleDhNulls", List.of(
                "                    chunk.set(offset + ii, is.readDouble());"));
        lines = replaceRegion(lines, "PrecisionSingleValidityBuffer", List.of(
                "                elementSize = Float.BYTES;",
                "                supplier = () -> doubleCast(is.readFloat());"));
        lines = replaceRegion(lines, "PrecisionDoubleValidityBuffer", List.of(
                "                supplier = is::readDouble;"));
        lines = replaceRegion(lines, "FPCastHelper", List.of(
                "    private static double doubleCast(float a) {",
                "        return a == QueryConstants.NULL_FLOAT ? QueryConstants.NULL_DOUBLE : (double) a;",
                "    }"));
        FileUtils.writeLines(file, lines);
    }
}
