package io.deephaven.engine.v2.utils.copy;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.compilertools.ReplicatePrimitiveCode.charToAll;
import static io.deephaven.compilertools.ReplicatePrimitiveCode.charToObject;
import static io.deephaven.compilertools.ReplicateUtilities.globalReplacements;

public class ReplicateCopyKernelTest {
    public static void main(String[] args) throws IOException {
        ReplicateCopyKernel.main(args);
        charToAll("DB/src/test/java/io/deephaven/engine/v2/utils/copy/TestCharCopyKernel.java");
        fixupObjectCopyKernelTest(
                charToObject("DB/src/test/java/io/deephaven/engine/v2/utils/copy/TestCharCopyKernel.java"));
    }

    private static void fixupObjectCopyKernelTest(String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "ObjectChunk<Values>", "ObjectChunk<Object, Values>");
        FileUtils.writeLines(file, lines);
    }
}
