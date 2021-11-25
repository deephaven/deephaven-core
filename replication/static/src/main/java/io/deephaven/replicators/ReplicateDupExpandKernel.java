package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public class ReplicateDupExpandKernel {
    public static void main(String[] args) throws IOException {
        final String charClassPath =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/join/dupexpand/CharDupExpandKernel.java";
        ReplicatePrimitiveCode.charToAll(charClassPath);
        fixupObjectDupCompact(ReplicatePrimitiveCode.charToObject(charClassPath));
    }

    private static void fixupObjectDupCompact(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        final List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        FileUtils.writeLines(objectFile,
                ReplicateSortKernel.fixupObjectComparisons(ReplicationUtils.fixupChunkAttributes(lines)));
    }
}
