package io.deephaven.replicators;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAll;
import static io.deephaven.replication.ReplicatePrimitiveCode.charToObject;
import static io.deephaven.replication.ReplicationUtils.globalReplacements;

public class ReplicateCopyKernelTests {
    public static void main(String[] args) throws IOException {
        ReplicateCopyKernel.main(args);
        charToAll("engine/table/src/test/java/io/deephaven/engine/table/impl/util/copy/TestCharCopyKernel.java");
        fixupObjectCopyKernelTest(charToObject(
                "engine/table/src/test/java/io/deephaven/engine/table/impl/util/copy/TestCharCopyKernel.java"));
    }

    private static void fixupObjectCopyKernelTest(String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = globalReplacements(lines,
                "ObjectChunk<Values>", "ObjectChunk<Object, Values>");
        FileUtils.writeLines(file, lines);
    }
}
