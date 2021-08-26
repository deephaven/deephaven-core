package io.deephaven.db.v2.hashing;

import io.deephaven.base.verify.Assert;
import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.db.v2.utils.compact.CharCompactKernel;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static io.deephaven.compilertools.ReplicateUtilities.*;

public class ReplicateHashing {
    public static void main(String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAll(CharChunkHasher.class, ReplicatePrimitiveCode.MAIN_SRC);
        final String objectHasher =
                ReplicatePrimitiveCode.charToObject(CharChunkHasher.class, ReplicatePrimitiveCode.MAIN_SRC);
        fixupObjectChunkHasher(objectHasher);

        ReplicatePrimitiveCode.charToIntegers(CharToIntegerCast.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToIntegers(CharToLongCast.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToIntegers(CharToIntegerCastWithOffset.class, ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToIntegers(CharToLongCastWithOffset.class, ReplicatePrimitiveCode.MAIN_SRC);

        final List<String> paths =
                ReplicatePrimitiveCode.charToAll(CharChunkEquals.class, ReplicatePrimitiveCode.MAIN_SRC);
        final String floatPath =
                paths.stream().filter(p -> p.contains("Float")).findFirst().orElseThrow(FileNotFoundException::new);
        final String doublePath =
                paths.stream().filter(p -> p.contains("Double")).findFirst().orElseThrow(FileNotFoundException::new);

        fixupFloatChunkEquals(floatPath);
        fixupDoubleChunkEquals(doublePath);

        final String objectIdentityEquals =
                ReplicatePrimitiveCode.charToObject(CharChunkEquals.class, ReplicatePrimitiveCode.MAIN_SRC);
        fixupObjectChunkIdentityEquals(objectIdentityEquals);

        final String objectEquals =
                ReplicatePrimitiveCode.charToObject(CharChunkEquals.class, ReplicatePrimitiveCode.MAIN_SRC);
        fixupObjectChunkEquals(objectEquals);

        final List<String> compactKernels =
                ReplicatePrimitiveCode.charToAll(CharCompactKernel.class, ReplicatePrimitiveCode.MAIN_SRC);
        final String objectCompact =
                ReplicatePrimitiveCode.charToObject(CharCompactKernel.class, ReplicatePrimitiveCode.MAIN_SRC);
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
        FileUtils.writeLines(objectFile, globalReplacements(fixupChunkAttributes(lines), "Object.hashCode",
                "Objects.hashCode", "TypeUtils.unbox\\(\\(Object\\)value\\)", "value"));
    }

    private static void fixupObjectChunkEquals(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = addImport(lines, Objects.class);
        FileUtils.writeLines(objectFile, simpleFixup(fixupChunkAttributes(lines),
                "eq", "lhs == rhs", "Objects.equals(lhs, rhs)"));
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

        lines = removeImport(lines, "\\s*import io.deephaven.db.util.DhBooleanComparisons;");
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

        final List<String> lines = FileUtils.readLines(objectChunkIdentifyEqualsFileName, Charset.defaultCharset());
        FileUtils.writeLines(objectChunkIdentifyEqualsFileName, simpleFixup(fixupChunkAttributes(lines),
                "name", "ObjectChunkEquals", "ObjectChunkIdentityEquals"));
    }

    private static void fixupDoubleChunkEquals(String doublePath) throws IOException {
        final File objectFile = new File(doublePath);
        final List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        FileUtils.writeLines(objectFile,
                simpleFixup(lines, "eq", "lhs == rhs", "((Double.isNaN(lhs) && Double.isNaN(rhs)) || lhs == rhs)"));
    }

    private static void fixupFloatChunkEquals(String floatPath) throws IOException {
        final File objectFile = new File(floatPath);
        final List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        FileUtils.writeLines(objectFile,
                simpleFixup(lines, "eq", "lhs == rhs", "((Float.isNaN(lhs) && Float.isNaN(rhs)) || lhs == rhs)"));
    }

}
