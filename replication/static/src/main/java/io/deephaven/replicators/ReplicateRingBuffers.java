/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicateRingBuffers {

    public static void main(String... args) throws IOException {
        // replicate ring buffers to all but Object (since RingBuffer<> already exisits)
        charToAllButBoolean("Base/src/main/java/io/deephaven/base/ringbuffer/CharRingBuffer.java");

        // replicate the tests
        charToAllButBoolean("Base/src/test/java/io/deephaven/base/ringbuffer/CharRingBufferTest.java");
    }

    private static void replaceLines(String fileResult, String... replacements) throws IOException {
        final File objectFile = new File(fileResult);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = ReplicationUtils.globalReplacements(lines, replacements);
        FileUtils.writeLines(objectFile, lines);
    }
}
