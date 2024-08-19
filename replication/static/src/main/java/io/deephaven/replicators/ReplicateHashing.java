//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.base.verify.Assert;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateHashing {
    private static final String TASK = "replicateHashing";

    public static void main(String[] args) throws IOException {
        charToAll(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/util/hashing/CharChunkHasher.java");
        final String objectHasher =
                charToObject(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/util/hashing/CharChunkHasher.java");
        fixupObjectChunkHasher(objectHasher);

        charToIntegers(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/util/hashing/CharToIntegerCast.java");
        charToIntegers(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/util/hashing/CharToLongCast.java");
        charToIntegers(TASK,
                "engine/chunk/src/main/java/io/deephaven/chunk/util/hashing/CharToIntegerCastWithOffset.java");
        charToIntegers(TASK,
                "engine/chunk/src/main/java/io/deephaven/chunk/util/hashing/CharToLongCastWithOffset.java");

        charToAll(TASK,
                "engine/chunk/src/main/java/io/deephaven/chunk/util/hashing/CharChunkEquals.java");

        final String objectIdentityEquals =
                charToObject(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/util/hashing/CharChunkEquals.java");
        fixupObjectChunkIdentityEquals(objectIdentityEquals);

        final String objectDeepEquals =
                charToObject(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/util/hashing/CharChunkEquals.java");
        fixupObjectChunkDeepEquals(objectDeepEquals);

        final String objectEquals =
                charToObject(TASK, "engine/chunk/src/main/java/io/deephaven/chunk/util/hashing/CharChunkEquals.java");
        fixupObjectChunkEquals(objectEquals);

        final List<String> compactKernels = charToAll(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/util/compact/CharCompactKernel.java");
        final String objectCompact = charToObject(TASK,
                "engine/table/src/main/java/io/deephaven/engine/table/impl/util/compact/CharCompactKernel.java");
        fixupObjectCompact(objectCompact);
        // noinspection OptionalGetWithoutIsPresent
        fixupBooleanCompact(compactKernels.stream().filter(x -> x.contains("Boolean")).findFirst().get());
        fixupFloatCompact(compactKernels.stream().filter(x -> x.contains("Double")).findFirst().get(), "Double");
        fixupFloatCompact(compactKernels.stream().filter(x -> x.contains("Float")).findFirst().get(), "Float");
    }

    private static void fixupObjectChunkHasher(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = addImport(lines, Objects.class);
        FileUtils.writeLines(objectFile,
                globalReplacements(fixupChunkAttributes(lines), "TypeUtils.unbox\\(\\(Object\\) value\\)", "value"));
    }

    private static void fixupObjectChunkEquals(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = addImport(lines, Objects.class);
        FileUtils.writeLines(objectFile, fixupChunkAttributes(lines));
    }

    private static void fixupBooleanCompact(String booleanPath) throws IOException {
        final File objectFile = new File(booleanPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());

        lines = replaceRegion(lines, "compactAndCount", Arrays.asList(
                "        int trueValues = 0;" +
                        "        int falseValues = 0;" +
                        "        final int end = start + length;" +
                        "        for (int rpos = start; rpos < end; ++rpos) {" +
                        "            final boolean nextValue = valueChunk.get(rpos);" +
                        "            if (nextValue) {" +
                        "                trueValues++;" +
                        "            }" +
                        "            else {" +
                        "                falseValues++;" +
                        "            }" +
                        "        }",
                "        if (trueValues > 0) {",
                "            valueChunk.set(++wpos, true);",
                "            counts.set(wpos, trueValues);",
                "        }",
                "        if (falseValues > 0) {",
                "            valueChunk.set(++wpos, false);",
                "            counts.set(wpos, falseValues);",
                "        }"));

        lines = replaceRegion(lines, "shouldIgnore", Collections.singletonList("        return false;"));

        lines = removeImport(lines, "\\s*import io.deephaven.util.compare.BooleanComparisons;");
        lines = removeImport(lines, "\\s*import static.*QueryConstants.*;");

        FileUtils.writeLines(objectFile, lines);
    }

    private static void fixupObjectCompact(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = fixupChunkAttributes(lines, "T");
        lines = globalReplacements(lines, "public static void", "public static <T> void",
                "private static void", "private static <T> void",
                "public static int", "public static <T> int",
                "final Object nextValue", "final T nextValue");
        lines = globalReplacements(lines, "NULL_OBJECT", "null");
        lines = removeImport(lines, "\\s*import static.*QueryConstants.*;");
        FileUtils.writeLines(objectFile, lines);
    }

    private static void fixupFloatCompact(String doublePath, String typeOfFloat) throws IOException {
        final File objectFile = new File(doublePath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = replaceRegion(lines, "shouldIgnore", Collections.singletonList(
                "        return value == NULL_" + typeOfFloat.toUpperCase() + " || " + typeOfFloat + ".isNaN(value);"));
        FileUtils.writeLines(objectFile, lines);
    }

    private static void fixupObjectChunkIdentityEquals(String objectPath) throws IOException {
        final File objectChunkEqualsFileName = new File(objectPath);
        final File objectChunkIdentifyEqualsFileName =
                new File(objectChunkEqualsFileName.getParent(), "ObjectChunkIdentityEquals.java");
        Assert.eqTrue(objectChunkEqualsFileName.renameTo(objectChunkIdentifyEqualsFileName),
                "objectChunkEqualsFileName.renameTo(objectChunkIdentifyEqualsFileName)");

        List<String> lines = FileUtils.readLines(objectChunkIdentifyEqualsFileName, Charset.defaultCharset());
        lines = fixupChunkAttributes(lines);
        lines = simpleFixup(lines,
                "name", "ObjectChunkEquals", "ObjectChunkIdentityEquals");
        lines = simpleFixup(lines,
                "eq", "ObjectComparisons\\.eq\\(lhs, rhs\\)", "lhs == rhs");
        FileUtils.writeLines(objectChunkIdentifyEqualsFileName, lines);
    }

    private static void fixupObjectChunkDeepEquals(String objectPath) throws IOException {
        final File objectChunkEqualsFileName = new File(objectPath);
        final File objectChunkDeepEqualsFileName =
                new File(objectChunkEqualsFileName.getParent(), "ObjectChunkDeepEquals.java");
        Assert.eqTrue(objectChunkEqualsFileName.renameTo(objectChunkDeepEqualsFileName),
                "objectChunkEqualsFileName.renameTo(objectChunkDeepEqualsFileName)");

        {
            List<String> lines = FileUtils.readLines(objectChunkDeepEqualsFileName, Charset.defaultCharset());
            lines = addImport(lines, Objects.class);
            FileUtils.writeLines(objectChunkDeepEqualsFileName, simpleFixup(fixupChunkAttributes(lines),
                    "name", "ObjectChunkEquals", "ObjectChunkDeepEquals"));
        }
        {
            final List<String> lines = FileUtils.readLines(objectChunkDeepEqualsFileName, Charset.defaultCharset());
            FileUtils.writeLines(objectChunkDeepEqualsFileName, simpleFixup(lines,
                    "eq", "ObjectComparisons\\.eq\\(lhs, rhs\\)", "Objects.deepEquals(lhs, rhs)"));
        }
    }
}
